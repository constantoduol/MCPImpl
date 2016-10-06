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
import com.quest.access.useraccess.services.Message;
import com.quest.access.useraccess.services.Serviceable;
import com.quest.access.useraccess.services.WebService;
import com.quest.servlets.ClientWorker;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.servlet.ServletConfig;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author conny
 */
@WebService(name="mcp_service",privileged = "no")
public class MCPService implements Serviceable {
    
    private final HashMap<String,String> allNodes = new HashMap();
    
    private HashMap<String, String> aggregatedData = new HashMap();
    
    private HashMap<String, ArrayList> eventLog = new HashMap(); 
    
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
        String node = ApiProxy.getCurrentEnvironment().getAppId();
        Queue queue = QueueFactory.getDefaultQueue();
        queue.add(TaskOptions
                .Builder
                .withPayload(new BackgroundTask(aggregator, requestId, script, node))
                .etaMillis(System.currentTimeMillis()));//start executing the task now!
    }
    
    @Endpoint(name="bg_message")
    public void bgMessage(Server serv, ClientWorker worker) {
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        String msg = request.optString("message");
        interceptMessages(worker);
        addLog(reqId, msg);
    }
    
    private void interceptMessages(ClientWorker worker) {
        JSONObject request = worker.getRequestData();
        String msg = request.optString("message");
        String reqId = request.optString("request_id");
        if(msg.equals("!!complete")){
            try {
                //run the onFinish handler
                String script = request.optString("script");
                
                ScriptEngineManager factory = new ScriptEngineManager();
                ScriptEngine engine = factory.getEngineByName("JavaScript");
                //call the aggregate function with the response
                engine.eval(mcpScript + script);
                engine.put("_all_data_", aggregatedData.get(reqId));
                engine.put("_request_id_", reqId);
                engine.put("_service_", this);
                engine.eval("if(onFinish) onFinish();");
                addLog(reqId, "on finish called");
            } catch (ScriptException ex) {
                addLog(reqId, ex.getMessage());
                Logger.getLogger(MCPService.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    public void addLog(String reqId, String event){
        ArrayList log = eventLog.get(reqId);
        if(log == null) log = new ArrayList();
        log.add(event);
        eventLog.put(reqId, log);
    }
    
    @Endpoint(name="fetch_messages")
    public void fetchMessages(Server serv, ClientWorker worker){
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        ArrayList log = eventLog.get(reqId);
        if(log == null) log = new ArrayList();
        serv.messageToClient(worker.setResponseData(new JSONArray(log)));
        eventLog.put(reqId, new ArrayList());
    }
    
    @Endpoint(name="aggregate")
    public synchronized void aggregate(Server serv, ClientWorker worker) {
        JSONObject request = worker.getRequestData();
        String reqId = request.optString("request_id");
        try {
            //the best way to aggregate is store the results
            //in java memory and load to javascript when needed
            String resp = request.optString("response");
            String script = request.optString("script");
            ScriptEngineManager factory = new ScriptEngineManager();
            ScriptEngine engine = factory.getEngineByName("JavaScript");
            //call the aggregate function with the response
            script = URLDecoder.decode(script, "utf-8");
            engine.eval(script);
            engine.put("_aggregated_data_", aggregatedData.get(reqId));
            engine.put("_new_data_", URLDecoder.decode(resp, "utf-8"));
            aggregatedData.put(reqId, (String)engine.eval("if(aggregate) aggregate();"));
        } catch (Exception ex) {
            addLog(reqId, ex.getLocalizedMessage());
            Logger.getLogger(MCPService.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    private void sendHandshake(Server serv, ClientWorker worker) throws Exception {
        String aggregator = ApiProxy.getCurrentEnvironment().getAppId();
        String script = worker.getRequestData().optString("script");
        String requestId = new UniqueRandom(20).nextMixedRandom();
        //inform other nodes that you are the aggregator
        String encodedScript = URLEncoder.encode(script, "utf-8");
        String params = "?svc=mcp_service&msg=handshake&"
                + "aggregator="+aggregator+"&script="+encodedScript+"&request_id="+requestId;
        //go through all the nodes and send a handshake signal
        for(String node : allNodes.keySet()){
            Server.get(allNodes.get(node) + params);
        }
        serv.messageToClient(worker.setResponseData(requestId));
    }
    
    public void loadMCPNodes(Server serv){
        ServletConfig c = serv.getConfig();
        String nodes = c.getInitParameter("nodes");
        StringTokenizer st = new StringTokenizer(nodes, ",");
        while(st.hasMoreTokens()){
            String node = st.nextToken().trim();
            String nodeAddress = "https://" + node + ".appspot.com/server";
            allNodes.put(node, nodeAddress);
        }
    }
    

}
