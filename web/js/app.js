/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

function App(){
    this.server = "/server";
}

App.prototype.allowedActions = ["app.renderGraph", "app.stopPolling"];

App.prototype.interval = 0;

App.prototype.runScript = function (editor) {
    var data = {
        script : encodeURIComponent(editor.getValue())
    };
    app.xhr(data, "mcp_service", "run_script", {
        load: false,
        success: function(resp){
            app.requestId = resp.response.data;
            app.addLog(["Request received, processing initiated"]);
            app.pollServer(app.requestId);
        },
        error: function(err){
            console.log(err);
        }
    });
};

App.prototype.killScript = function () {
    var sure = confirm("Are you sure you want to do this?");
    if(!sure) return;
    var data = {
        request_id: app.requestId
    };
    app.xhr(data, "mcp_service", "kill_script", {
        load: false,
        success: function (resp) {
            app.requestId = resp.response.data;
            app.addLog(["Kill script initiated, may take time to complete!"]);
        },
        error: function (err) {
            console.log(err);
        }
    });
};

App.prototype.addLog = function(logs){
    if(!logs) return;
    var logArea = $("#logs");
    $.each(logs,function(x, log){
       var date = new Date().toLocaleTimeString();
       var logHtml = $("<div><span class='time'>"+date+"</span><span class='log'>"+log+"</span></div>"); 
       logArea.prepend(logHtml);
       if(x === logs.length - 1) 
           logArea.prepend($("<div><span class='time'>"
               +date+"</span><span class='log'>---------------------------------------</span></div>"));
    });
};

App.prototype.pollServer = function (reqId) {
    var data = {
        request_id : reqId
    };
    app.interval = setInterval(function(){
        app.xhr(data, "mcp_service", "fetch_messages", {
            success: function (resp) {
                console.log(resp);
                var events = resp.response.data.event;
                if(events){
                    app.addLog(events);
                    app.processActions(events);
                }
            },
            error: function (err) {
                console.log(err);
            }
        });
    }, 5000);
    

};

App.prototype.stopPolling = function(){
    clearInterval(app.interval);
    console.log("polling stopped");
};



App.prototype.xhr = function (data, svc, msg, func) {
    var request = {};
    request.request_header = {};
    request.request_header.request_svc = svc;
    request.request_header.request_msg = msg;
    request.request_object = data;
    return $.ajax({
        type: "POST",
        url: app.server,
        data: "json=" + encodeURIComponent(JSON.stringify(request)),
        dataFilter: function (data, type) {
            var data = JSON.parse(data);
            return data;
        },
        success: function (data) {
            if (func.success)
                func.success(data, request.request_object);
        },
        error: function (err) {
            if (func.error)
                func.error(err, request.request_object);
        }
    });
};

App.prototype.processActions = function(events){
    for(var x = 0; x < events.length; x++){
        var msg = events[x];
        var actionIndex = msg.lastIndexOf("action:", 0);
        if (actionIndex > -1) {
            //has action
            var actions = msg.split(":");
            var methodName = actions[1].substring(0, actions[1].indexOf("("));
            if (app.allowedActions.indexOf(methodName)) {
                window.eval(actions[1]);
            }
        }
    }
};

App.prototype.renderGraph = function(type, data){
    Morris.Line({
        element: 'graph_area',
        parseTime: false,
        data: r, xkey: col1, ykeys: colNames, labels: colNames
    });  
};

window.app = new App();
