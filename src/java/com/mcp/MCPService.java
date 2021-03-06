package com.mcp;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.utils.SystemProperty;
import com.google.apphosting.api.ApiProxy;
import com.quest.access.common.UniqueRandom;
import com.quest.access.common.datastore.Datastore;
import com.quest.access.common.io;
import com.quest.access.control.Server;
import com.quest.access.control.Server.BackgroundTask;
import com.quest.access.useraccess.services.Endpoint;
import com.quest.access.useraccess.services.Serviceable;
import com.quest.access.useraccess.services.WebService;
import com.quest.servlets.ClientWorker;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.Message;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.mozilla.javascript.NativeArray;

/**
 *
 * @author conny
 */
@WebService(name="mcp_service",privileged = "no")
public class MCPService implements Serviceable {
    
    private static volatile ConcurrentHashMap<String, String> reducedData = new ConcurrentHashMap();
    
    //how many data fetches, this is loosely translated as how many
    //data aggregations are done for a given request id
    private static volatile ConcurrentHashMap<String, Integer> fetchCount = new ConcurrentHashMap();
    
    private static volatile ConcurrentHashMap<String, Boolean> killProcess = new ConcurrentHashMap();
    
    private static String SEPARATOR = "*!__sep__!*";
    
    private static String mcpScript;
    
    @Override
    public void service() {}

    @Override
    public void onStart(Server serv) {
        mcpScript = Server.streamToString(Server.class.getResourceAsStream("mcp.js"));
    }

    @Override
    public void onPreExecute(Server serv, ClientWorker worker) {

    }
    
    @Endpoint(name="run_script")
    public void runScript(Server serv, ClientWorker worker) throws Exception{
        //this node has received the start process message and will act
        //as the aggregator
        io.log("received run script request", Level.INFO, null);
        JSONObject request = worker.getRequestData();
        String script = request.optString("script");
        String requestId = new UniqueRandom(20).nextMixedRandom();
        //String selfUrl = "https://" + getAppId() + ".appspot.com/server";
        String selfUrl = "http://localhost:8200/server";
        io.log("self url -> "+selfUrl, Level.INFO, null);
        //inform other nodes that you are the aggregator
        String decodeScript = URLDecoder.decode(script, "utf-8");
        io.log(decodeScript, Level.INFO, MCPService.class);
        HashMap m = new HashMap();
        m.put("value", new Text(decodeScript));
        m.put("request_id", requestId);
        m.put("created", System.currentTimeMillis());
        Datastore.insert("Script", m);
        
        Queue queue = QueueFactory.getDefaultQueue();
        queue.add(TaskOptions.Builder
                .withPayload(new BackgroundTask(selfUrl, requestId, decodeScript, mcpScript))
                .etaMillis(System.currentTimeMillis()));//start executing the task now!
        serv.messageToClient(worker.setResponseData(requestId));
    }
    
    private boolean onProduction(){
        return SystemProperty.environment.value() == SystemProperty.Environment.Value.Production;
    }
    
    //runs on aggregator
    @Endpoint(name="kill_script")
    public void killScript(Server serv, ClientWorker worker){
        killProcess(worker);
        serv.messageToClient(worker.setResponseData("success"));
    }
    
    private String getScript(String requestId){
        Filter[] filters = new Filter[]{
            equalFilter("request_id", requestId)
        };
        Entity en = Datastore.getSingleEntity("Script", filters);
        Text txt =  (Text)en.getProperty("value");
        return txt.getValue();
    }
    
    //runs on aggregator
    private void killProcess(ClientWorker worker){
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        //String script = request.optString("script");
        try {
            //script = URLDecoder.decode(script, "utf-8");
            String script = getScript(reqId);
            //check if we have already killed this process
            Boolean processKilled = killProcess.get(reqId);
            if (processKilled != null && processKilled == true) 
                return;
            //run the onFinish handler
            HashMap params = new HashMap();
            params.put("_all_data_", reducedData.get(reqId));
            params.put("_request_id_", reqId);
            params.put("_service_", this);
            params.put("_fetch_count_", fetchCount.get(reqId));
            String onFinish = "\n" + "if(onFinish) onFinish();";
            //call the reduce function with the response
    
            Object finishResult = Server.execScript(mcpScript + script + onFinish, params);
            killProcess.put(reqId, true);
            reducedData.remove(reqId);
            fetchCount.remove(reqId);
            io.log("on finish returned -> "+finishResult, Level.INFO, null);
            addLog(reqId, "process killed and onfinish called");
            addLog(reqId, "action:app.stopPolling()");
            addLog(reqId, "!!quit");
        } catch (Exception e) {
            e.printStackTrace();
            addLog(reqId, e.getLocalizedMessage());
        }
    }
    
 
    public synchronized void addLog(String reqId, String event) {
        Filter[] filters = new Filter[]{
            equalFilter("request_id", reqId)
        };
        Entity en = Datastore.getSingleEntity("EventLog", filters);
        if(en != null) {
            Text log = (Text)en.getProperty("value");
            String logValue = log.getValue();
            logValue += SEPARATOR + event;
            Datastore.updateSingleEntity("EventLog", new String[]{"value"}, new Object[]{new Text(logValue)}, filters);
        } else {
            en = new Entity("EventLog");
            en.setProperty("value", new Text(event));
            en.setProperty("created", System.currentTimeMillis());
            en.setProperty("request_id", reqId);
            Datastore.insert(en);
        }
    }
    
    @Endpoint(name="fetch_messages")
    public void fetchMessages(Server serv, ClientWorker worker) throws JSONException{
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        Filter[] filters = new Filter[]{
            equalFilter("request_id", reqId)
        };
        Entity en = Datastore.getSingleEntity("EventLog", filters);
        String log = "";
        if(en != null) log = ((Text)en.getProperty("value")).getValue();
        io.log("sending log ->" + log, Level.INFO, null);
        if(log.length() > 0 && log.contains("!!quit")) {
            Datastore.deleteSingleEntity("EventLog", filters);
            Datastore.deleteSingleEntity("Script", filters);
        }
        JSONObject data = new JSONObject();
        data.put("events", log);
        serv.messageToClient(worker.setResponseData(data));
    }
    
    public String createEntity(String entityName, JSONObject values){
        Key k = Datastore.insert(entityName, values);
        return KeyFactory.keyToString(k);
    }
    
    private Object[][] getFilters(NativeArray filters) {
        NativeArray root = (NativeArray) filters;
        int rootLen = (int) root.getLength();
        Object[][] filterz = new Object[rootLen][3];
        for (int x = 0; x < rootLen; x++) {
            NativeArray arr = (NativeArray) root.get(x);
            Object[] filter = new Object[3];
            for (int y = 0; y < 3; y++) {
                filter[y] = arr.get(y);
            }
            filterz[x] = filter;
        }
        return filterz;
    }

    //filters: [["age", ">", 20], ["name", "=", "constant"]]
    public JSONObject dbGet(String entityName, Object filters) {
        NativeArray root = (NativeArray) filters;
        return Datastore.getSingleEntity(entityName, getFilters(root));
    }
    
    public JSONObject dbGetMultiple(String entityName, Object filters){
        NativeArray root = (NativeArray) filters;
        return Datastore.getMultipleEntities(entityName, getFilters(root)); 
    }
    
    private static Filter equalFilter(String propName, Object value) {
        return new FilterPredicate(propName, FilterOperator.EQUAL, value);
    }
    
    private static Filter greaterThanFilter(String propName, Object value) {
        return new FilterPredicate(propName, FilterOperator.GREATER_THAN, value);
    }
    
    private static Filter greaterThanOrEqualFilter(String propName, Object value) {
        return new FilterPredicate(propName, FilterOperator.GREATER_THAN_OR_EQUAL, value);
    }
    
    private static Filter lessThanFilter(String propName, Object value) {
        return new FilterPredicate(propName, FilterOperator.LESS_THAN, value);
    }
    
    
    
    @Endpoint(name = "bg_message")
    public void bgMessage(Server serv, ClientWorker worker) {
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        String msg = request.optString("message");
        io.log("bg message received-> "+msg + " req_id : "+reqId, Level.INFO, null);
        addLog(reqId, msg);
        serv.messageToClient(worker.setResponseData("success"));
    }
    

    
    //runs on aggregator
    @Endpoint(name="reduce")
    public synchronized void reduce(Server serv, ClientWorker worker) {
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        try {
            //the best way to reduce is store the results
            //in java memory and load to javascript when needed
            String resp = request.optString("response");
            //String script = request.optString("script");
            updateFetchCount(reqId);
            //call the reduce function with the response
            //script = URLDecoder.decode(script, "utf-8");
            String script = getScript(reqId);
            io.log("aggregating data for req_id -> "+reqId, Level.INFO, null);
            //call the reduce function with the response
            HashMap params = new HashMap(); 
            params.put("_reduced_data_", reducedData.get(reqId));
            params.put("_new_data_", URLDecoder.decode(resp, "utf-8"));
            params.put("_service_", this);
            params.put("_request_id_", reqId);
            params.put("_fetch_count_", fetchCount.get(reqId));
            String aggrScript = "\n" + "if(reduce) reduce();";
            //io.log(script, Level.INFO, null);
            Object result = Server.execScript(script + aggrScript, params);
            reducedData.put(reqId, (String)result);
            JSONObject respObj = new JSONObject();
            respObj.put("fetch_count", fetchCount.get(reqId));
            respObj.put("kill_process", killProcess.get(reqId));
            worker.setResponseData(respObj);
            serv.messageToClient(worker);
        } catch (Exception ex) {
            addLog(reqId, ex.getLocalizedMessage());
            Logger.getLogger(MCPService.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    //runs on aggregator
    private synchronized void updateFetchCount(String requestId){
        Integer count = fetchCount.get(requestId);
        if(count == null) {
            count = 1;
            fetchCount.put(requestId, count);
        }
        fetchCount.put(requestId, ++count);
    }
    
    private String getAppId(){
        String appId = ApiProxy.getCurrentEnvironment().getAppId();
        int index = appId.indexOf("~");
        if(index > 0)
            return appId.substring(index + 1);
        return appId;
        
    }
    
    
    //runs on any node
    public void sendEmail(String reqId, String to, String subject, String text){
        Properties props = new Properties();
        Session session = Session.getDefaultInstance(props, null);
        try {
            MimeMessage msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress("tooduol@gmail.com", "MCP Alerts"));
            msg.addRecipient(Message.RecipientType.TO,new InternetAddress(to, to));
            msg.setSubject(subject);
            msg.setContent(text, "text/html; charset=utf-8");
            Transport.send(msg);
            io.log("sending email-> " +text, Level.INFO, null);
        } catch (Exception e) {
            addLog(reqId, e.getLocalizedMessage());
        }
    }
    
    public static void main(String [] args) throws ParseException{
      io.out(new Date("2013-01-01"));
              
    }
}
