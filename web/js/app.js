/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

function App(){
    this.server = "/server";
}

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
    logs.reverse();
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
    setInterval(function(){
        app.xhr(data, "mcp_service", "fetch_messages", {
            success: function (resp) {
                console.log(resp);
                app.addLog(resp.response.data);
            },
            error: function (err) {
                console.log(err);
            }
        });
    }, 5000);

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

window.app = new App();
