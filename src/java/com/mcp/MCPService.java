package com.mcp;

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
import java.io.Serializable;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.servlet.ServletConfig;
import javax.mail.Message;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author conny
 */
@WebService(name="mcp_service",privileged = "no")
public class MCPService implements Serviceable, Serializable {
    
    private HashMap<String, String> aggregatedData = new HashMap();
    
    private HashMap<String, ArrayList> eventLog = new HashMap();
    
    //how many data fetches, this is loosely translated as how many
    //data aggregations are done for a given request id
    private HashMap<String, Integer> fetchCount = new HashMap();
    
    private HashMap<String, Boolean> killProcess = new HashMap();
    
    private HashMap<String, String> scripts = new HashMap<String, String>();
    
    private final String KILL_MESSAGE = "!!kill_process";
    
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
        String selfUrl = "https://" + getAppId() + ".appspot.com/server";
        //String selfUrl = "http://localhost:8200/server";
        io.log("self url -> "+selfUrl, Level.INFO, null);
        //inform other nodes that you are the aggregator
        String decodeScript = URLDecoder.decode(script, "utf-8");
        scripts.put(requestId, decodeScript);
        io.log(decodeScript, Level.INFO, MCPService.class);
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
        killProcess(worker.getRequestData().optString("request_id"));
        serv.messageToClient(worker.setResponseData("success"));
    }
    

    //runs on aggregator
    private void killProcess(String reqId){
        try {
            //check if we have already killed this process
            Boolean processKilled = killProcess.get(reqId);
            if (processKilled != null && processKilled == true) {
                return;
            }
            //run the onFinish handler
            HashMap params = new HashMap();
            params.put("_all_data_", aggregatedData.get(reqId));
            params.put("_request_id_", reqId);
            params.put("_service_", this);
            params.put("_fetch_count_", fetchCount.get(reqId));
            String onFinish = "\n" + "if(onFinish) onFinish();";
            //call the aggregate function with the response
            String script = scripts.get(reqId);
            Server.execScript(mcpScript + script + onFinish, params);
            killProcess.put(reqId, true);
            aggregatedData.remove(reqId);
            fetchCount.remove(reqId);
            scripts.remove(reqId);
            addLog(reqId, "process killed");
            addLog(reqId, "on finish called");
        } catch (Exception e) {
            e.printStackTrace();
            addLog(reqId, e.getLocalizedMessage());
        }
    }
    
    //runs on aggregator
    private void interceptMessages(String reqId, String msg) {
        if(msg.equals(KILL_MESSAGE)){
            io.out("kill process called");
            killProcess(reqId);
        }
    }
    
    //runs on aggregator
    public void addLog(String reqId, String event){
        interceptMessages(reqId, event);
        ArrayList log = eventLog.get(reqId);
        if(log == null) log = new ArrayList();
        log.add(event);
        eventLog.put(reqId, log);
    }
    
    //runs on aggregator
    @Endpoint(name="fetch_messages")
    public void fetchMessages(Server serv, ClientWorker worker){
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        ArrayList log = eventLog.get(reqId);
        if(log == null) log = new ArrayList();
        io.log("all logs ->"+eventLog, Level.INFO, null);
        io.log("sending log ->" + log, Level.INFO, null);
        serv.messageToClient(worker.setResponseData(new JSONArray(log)));
        eventLog.put(reqId, new ArrayList());
    }
    
    @Endpoint(name = "bg_message")
    public void bgMessage(Server serv, ClientWorker worker) {
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        String msg = request.optString("message");
        io.log("bg message received-> "+msg + " req_id : "+reqId, Level.INFO, null);
        interceptMessages(reqId, msg);
        addLog(reqId, msg);
    }
    
    //runs on aggregator
    @Endpoint(name="aggregate")
    public synchronized void aggregate(Server serv, ClientWorker worker) {
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        try {
            //the best way to aggregate is store the results
            //in java memory and load to javascript when needed
            String resp = request.optString("response");
            String script = request.optString("script");
            updateFetchCount(reqId);
            //call the aggregate function with the response
            script = URLDecoder.decode(script, "utf-8");
            io.log("aggregating data for req_id -> "+reqId, Level.INFO, null);
            //call the aggregate function with the response
            HashMap params = new HashMap(); 
            params.put("_aggregated_data_", aggregatedData.get(reqId));
            params.put("_new_data_", URLDecoder.decode(resp, "utf-8"));
            params.put("_service_", this);
            params.put("_request_id_", reqId);
            params.put("_fetch_count_", fetchCount.get(reqId));
            String aggrScript = "\n" + "if(aggregate) aggregate();";
            Object result = Server.execScript(script + aggrScript, params);
            aggregatedData.put(reqId, (String)result);
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
    
    public static void main(String [] args){
      io.out(Server.streamToString(Server.class.getResourceAsStream("Server.java")));
    }
}
