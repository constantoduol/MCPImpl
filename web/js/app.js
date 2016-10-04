/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

function App(){
    this.server = "/server";
}

App.prototype.runScript = function () {
    var data = {
        script : encodeURIComponent($("#script").val())
    };
    app.xhr(data, "mcp_service", "run_script", {
        load: false,
        success: function(resp){
            console.log(resp);
        },
        error: function(err){
            console.log(err);
        }
    });
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
