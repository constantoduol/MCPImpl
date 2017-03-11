kinds = ["MsgLog"];

limits = {
    "MsgLog": 10
};


//this will run on every node
//this runs in a background
function run() {
    _task_.sendMessage("performing a run");
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
    _task_.sendMessage(_self_state_.fetch_count);
    if(_self_state_.fetch_count > 5) 
        return false; //stop after approximately 600 messages
    return true;
}

//this will run only on the aggregator not in a background task
//make sure it runs fast
function aggregate() {
    var myData = JSON.parse(_new_data_);
    _service_.addLog(_request_id_, "aggregating data");
    var aggr = JSON.parse(_aggregated_data_);
    if(!aggr) aggr = [];
    aggr = aggr.concat(myData);
    return JSON.stringify(aggr);
}

//this will run on the aggregator when everything is complete
//it runs in the foreground
function onFinish() {
    var data = JSON.parse(_all_data_);
    _service_.addLog(_request_id_, "on finish called");
    _service_.addLog(_request_id_, _all_data_);
    _service_.sendEmail(_request_id_, 
            "constant@echomobile.org", "Hello World", _all_data_);
    return true;
}
