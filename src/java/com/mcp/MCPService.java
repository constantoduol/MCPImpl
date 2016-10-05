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
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.logging.Level;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.servlet.ServletConfig;
import org.json.JSONObject;

/**
 *
 * @author conny
 */
@WebService(name="mcp_service",privileged = "no")
public class MCPService implements Serviceable {
    
    private final HashMap<String,String> allNodes = new HashMap();
    
    private String aggregator;
    
    private String script; //javascript code
    
    private String requestId; //id of executing request

    
    @Override
    public void service() {}

    @Override
    public void onStart(Server serv) {
        loadMCPNodes(serv);
    }

    @Override
    public void onPreExecute(Server serv, ClientWorker worker) {

    }
    
    @Endpoint(name="run_script")
    public void runScript(Server serv, ClientWorker worker) throws Exception{
        //this node has received the start process message and will act
        //as the aggregator
        JSONObject request = worker.getRequestData();
        aggregator = ApiProxy.getCurrentEnvironment().getAppId();
        script = request.optString("script");
        requestId = new UniqueRandom(20).nextMixedRandom();
        serv.messageToClient(worker.setResponseData(Message.SUCCESS));
        sendHandshake();
    }
    
    @Endpoint(name="handshake")
    public void receiveHandshake(Server serv, ClientWorker worker){
        //this will receive a handshake with aggregator
        JSONObject request = worker.getRequestData();
        aggregator = request.optString("aggregator");
        script = request.optString("script");
        requestId = request.optString("request_id");
        Queue queue = QueueFactory.getDefaultQueue();
        queue.add(TaskOptions
                .Builder
                .withPayload(new BackgroundTask(aggregator, requestId, script))
                .etaMillis(System.currentTimeMillis()));//start executing the task now!
    }
    
    @Endpoint(name="aggregate")
    public void aggregate(Server serv, ClientWorker worker){
        //the best way to aggregate is store the results
        //in java memory and load to javascript when needed
    }
    
    private void sendHandshake() throws Exception {
        //inform other nodes that you are the aggregator
        String encodedScript = URLEncoder.encode(script, "utf-8");
        String params = "?svc=mcp_service&msg=handshake&"
                + "aggregator="+aggregator+"&script="+encodedScript+"&request_id="+requestId;
        //go through all the nodes and send a handshake signal
//        for(String node : allNodes.keySet()){
//            Server.get(allNodes.get(node) + params);
//        }
        Server.get("http://localhost:8200/server" + params);
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
    
    public static void main(String [] args) throws Exception{
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("JavaScript");
        engine.eval("var a = 20; print(a + 30)");
    }

}
