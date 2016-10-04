//reserved variables __aggregator
//prepend this script by initializing kinds, filters, orders ... to undefined
kinds = ["Account", "Enterprise"];

filters = {
	"Account" : [{
		"l" : "direction = ", 
		"r" : 0
	}],
	"Enterprise" : [{
		"l" : "amount >= ", 
		"r" : 100
	}]
};

orders = {
	"Account" : ["-request_date", "age"],
	"Enterprise" : ["-amount"]
};

limits = {
	"Account" : 30, 
	"Enterprise" : 40
};

//this will run on every node

function run(data){
	var response = [];
	for var obj in data {
		if obj.age > 20{
			response.push(obj.name);
		}
	}
	exit(response); //send response to aggregator
	nextData(); //fetch the next set of data
}

//this will fetch the next set of data
//for every node
function nextData(){
	//get aggregator and send the data
	var echoUrl = "http://localhost:3000/api/cms/mcp";
	var xhr = new XMLHttpRequest();
	var params = "";
	if(kinds) params += "kinds="+JSON.stringify(kinds) + "&";
	if(filters) params += "filters="+JSON.stringify(filters) + "&";
	if(orders) params += "orders="+JSON.stringify(orders) + "&";
	if(limits) params += "limits="+JSON.stringify(limits);
	xhr.open('POST', echoUrl, true);
	xhr.setRequestHeader('Content-type', 'application/x-www-form-urlencoded');
	xhr.onload = function () {
	    // do something to response
	    var nextData = JSON.parse(this.responseText);
	    if(nextData.length === 0) return; //no more data to process
	    run(JSON.parse(this.responseText));
	};
	xhr.send(params);	
}

//this will run only on the aggregator

function aggregate(newData){
	prevData.add(newData);
}

//this will run on the aggregator when everything is complete
function onFinish(aggregatedData){

}



function exit(resp){
	//get aggregator and send the data
	var aggregator = "http://localhost:8200/server";
	var xhr = new XMLHttpRequest();
	var params = "svc=mcp_service&msg=aggregate&response="+JSON.stringify(resp);
	xhr.open('POST', aggregator, true);
	xhr.setRequestHeader('Content-type', 'application/x-www-form-urlencoded');
	xhr.onload = function () {
	    // do something to response
	    print(this.responseText);
	};
	xhr.send(params);
}

//append nextData(run)