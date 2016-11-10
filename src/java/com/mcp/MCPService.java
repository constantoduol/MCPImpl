package com.mcp;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.apphosting.api.ApiProxy;
import com.quest.access.common.UniqueRandom;
import com.quest.access.common.io;
import com.quest.access.control.Server;
import com.quest.access.control.Server.BackgroundTask;
import com.quest.access.useraccess.services.Endpoint;
import com.quest.access.useraccess.services.Serviceable;
import com.quest.access.useraccess.services.WebService;
import com.quest.servlets.ClientWorker;
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
public class MCPService implements Serviceable {
    
    private final HashMap<String,String> allNodes = new HashMap();
    
    private final HashMap<String,String> aggregators = new HashMap();
    
    private HashMap<String, String> aggregatedData = new HashMap();
    
    private HashMap<String, ArrayList> eventLog = new HashMap();
    
    //how many data fetches, this is loosely translated as how many
    //data aggregations are done for a given request id
    private HashMap<String, Integer> fetchCount = new HashMap();
    
    private HashMap<String, Boolean> killProcess = new HashMap();
    
    private final String KILL_MESSAGE = "!!kill_process";
    
    private static String mcpScript;
    
    @Override
    public void service() {}

    @Override
    public void onStart(Server serv) {
        loadMCPNodes(serv);
        mcpScript = Server.streamToString(Server.class.getResourceAsStream("mcp.js"));
    }

    @Override
    public void onPreExecute(Server serv, ClientWorker worker) {

    }
    
    @Endpoint(name="run_script")
    public void runScript(Server serv, ClientWorker worker) throws Exception{
        //this node has received the start process message and will act
        //as the aggregator
        sendHandshake(serv, worker);
    }
    
    @Endpoint(name="handshake")
    public void receiveHandshake(Server serv, ClientWorker worker){
        //this will receive a handshake with aggregator
        JSONObject request = worker.getRequestData();
        String aggregator = request.optString("aggregator");
        String script = request.optString("script");
        String requestId = request.optString("request_id");
        aggregators.put(requestId, aggregator);
        io.log("handshake received from "+aggregator, Level.WARNING, this.getClass());
        sendMessage(requestId, "received handshake from "+aggregator);
        String node = getAppId();
        Queue queue = QueueFactory.getDefaultQueue();
        queue.add(TaskOptions
                .Builder
                .withPayload(new BackgroundTask(aggregator, requestId, script, node, mcpScript))
                .etaMillis(System.currentTimeMillis()));//start executing the task now!
        serv.messageToClient(worker.setResponseData("success"));
    }
    
    //runs on aggregator
    @Endpoint(name="bg_message")
    public void bgMessage(Server serv, ClientWorker worker) {
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        String msg = request.optString("message");
        io.log("background message received from "+getAggregator(reqId)+" msg: "+msg, Level.WARNING, this.getClass());
        interceptMessages(worker);
        addLog(reqId, msg);
    }
    
    //runs on aggregator
    @Endpoint(name="kill_script")
    public void killScript(Server serv, ClientWorker worker){
        killProcess(worker);
        serv.messageToClient(worker.setResponseData("success"));
    }
    
    private String getAggregator(String reqId){
        if(aggregators != null && aggregators.get(reqId) != null)
            return aggregators.get(reqId);
        else
            return "";
    }
    //runs on aggregator
    private void killProcess(ClientWorker worker){
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        io.log("kill script received from "+getAggregator(reqId), Level.WARNING, this.getClass());
        try {
            //check if we have already killed this process
            Boolean processKilled = killProcess.get(reqId);
            if (processKilled != null && processKilled == true) {
                return;
            }
            //run the onFinish handler
            String script = request.optString("script");
            HashMap params = new HashMap();
            params.put("_all_data_", aggregatedData.get(reqId));
            params.put("_request_id_", reqId);
            params.put("_service_", this);
            params.put("_fetch_count_", fetchCount.get(reqId));
            String onFinish = "\n" + "if(onFinish) onFinish();";
            //call the aggregate function with the response
            Server.execScript(mcpScript + script + onFinish, params);
            killProcess.put(reqId, true);
            aggregatedData.remove(reqId);
            fetchCount.remove(reqId);
            addLog(reqId, "process killed");
            addLog(reqId, "on finish called");
        } catch (Exception e) {
            e.printStackTrace();
            addLog(reqId, e.getLocalizedMessage());
        }
    }
    
    //runs on aggregator
    private void interceptMessages(ClientWorker worker) {
        JSONObject request = worker.getRequestData();
        String msg = request.optString("message");
        if(msg.equals(KILL_MESSAGE)){
            io.out("kill process called");
            killProcess(worker);
        }
    }
    
    //runs on aggregator
    private void addLog(String reqId, String event){
        ArrayList log = eventLog.get(reqId);
        if(log == null) log = new ArrayList();
        log.add(event);
        eventLog.put(reqId, log);
    }
    
    //runs on remote node
    public void sendMessage(String requestId, String msg) {
        String aggregator = aggregators.get(requestId);
        if(aggregator != null && aggregator.equals(getAppId())){
            addLog(requestId, msg);
            return;
        }
        io.log("message sent from "+aggregator+" to "+aggregators.get(requestId) +" msg: "+msg, Level.WARNING, this.getClass());
        String params = "svc=mcp_service&msg=bg_message&request_id="
                + requestId + "&message=" + msg;
        String aggregatorUrl = "https://"+aggregators.get(requestId) +".appspot.com/server";
        Server.post(aggregatorUrl, params);
    }
    
    //runs on aggregator
    @Endpoint(name="fetch_messages")
    public void fetchMessages(Server serv, ClientWorker worker){
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        ArrayList log = eventLog.get(reqId);
        if(log == null) log = new ArrayList();
        serv.messageToClient(worker.setResponseData(new JSONArray(log)));
        eventLog.put(reqId, new ArrayList());
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
            HashMap params = new HashMap(); 
            params.put("_aggregated_data_", aggregatedData.get(reqId));
            params.put("_new_data_", URLDecoder.decode(resp, "utf-8"));
            params.put("_service_", this);
            params.put("_request_id_", reqId);
            params.put("_fetch_count_", fetchCount.get(reqId));
            String aggrScript = "\n" + "if(aggregate) aggregate();";
            Object result = Server.execScript(script + aggrScript, params);
            io.log("aggregate on "+getAppId() +" result: "+result, Level.WARNING, this.getClass());
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
    
    //runs on aggregator
    private void sendHandshake(Server serv, ClientWorker worker)  {
        String requestId = new UniqueRandom(20).nextMixedRandom();
        try {
            String aggregator = getAppId();
            io.log("aggregator is : "+aggregator, Level.WARNING, MCPService.class);
            aggregators.put(requestId, aggregator);
            String script = worker.getRequestData().optString("script");
            io.log(script, Level.WARNING, MCPService.class);
            //inform other nodes that you are the aggregator
            String encodedScript = URLEncoder.encode(script, "utf-8");
            String params = "svc=mcp_service&msg=handshake&"
                    + "aggregator="+aggregator+"&script="+encodedScript+"&request_id="+requestId;
            //go through all the nodes and send a handshake signal
            serv.messageToClient(worker.setResponseData(requestId));
            for(String node : allNodes.keySet()){
                io.log("sending handshake to "+allNodes.get(node), Level.WARNING, this.getClass());;
                String result = Server.post(allNodes.get(node), params);
                io.log("handshake result from node "+node+" is "+result, Level.WARNING, this.getClass());
            }
            //Server.post("http://localhost:8200/server", params);
        } catch (Exception ex) {
            addLog(requestId, ex.getLocalizedMessage());
            Logger.getLogger(MCPService.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    //runs on every node
    private void loadMCPNodes(Server serv){
        ServletConfig c = serv.getConfig();
        String nodes = c.getInitParameter("nodes");
        StringTokenizer st = new StringTokenizer(nodes, ",");
        while(st.hasMoreTokens()){
            String node = st.nextToken().trim();
            String nodeAddress = "https://" + node + ".appspot.com/server";
            allNodes.put(node, nodeAddress);
        }
    }
    
    //runs on any node
    public void sendEmail(String reqId, String to, String subject, String text){
        Properties props = new Properties();
        Session session = Session.getDefaultInstance(props, null);
        try {
            MimeMessage msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress("tooduol@gmail.com", "MCP Alerts"));
            msg.addRecipient(Message.RecipientType.TO,
                    new InternetAddress(to, to));
            msg.setSubject(subject);
            msg.setContent(text, "text/html; charset=utf-8");
            Transport.send(msg);
        } catch (Exception e) {
            sendMessage(reqId, e.getLocalizedMessage());
        }
    }
    
    public static void main(String [] args){
        String appId = "s~mcp-node-0";
        int index = appId.indexOf("~");
        if (index > 0) {
            io.out(appId.substring(index + 1));
        }
        
    }
}
