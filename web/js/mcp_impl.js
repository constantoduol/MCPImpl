kinds = ["MsgLog", "Enterprise"];

limits = {
    "MsgLog": 30,
    "Enterprise": 10
};


//this will run on every node
//this runs in a background
function run() {
    var result = [];
    var msgLogs = _data_.MsgLog;
    for (var x in msgLogs) {
        result.push(msgLogs[x].ts_requested);
    }
    exit(result); //send response to aggregator then fetch next data
}

function beforeNextData(){
    //return true to fetch next data
    //return false to fail next data fetch
    _task_.sendMessage(JSON.stringify(_aggregator_state_));
    return true;
}

//this will run only on the aggregator not in a background task
//make sure it runs fast
function aggregate() {
    var myData = JSON.parse(_new_data_);
    var aggr = JSON.parse(_aggregated_data_);
    if(!aggr) aggr = [];
    aggr = aggr.concat(myData);
    return JSON.stringify(aggr);
}

//this will run on the aggregator when everything is complete
//it runs in the foreground
function onFinish() {
    //_service_.addLog(_request_id_, _all_data_);
    //_service_.addLog(_request_id_, "on finish called");
}
