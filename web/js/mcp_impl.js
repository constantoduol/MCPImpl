kinds = ["MsgLog", "Enterprise"];

limits = {
    "MsgLog": 30,
    "Enterprise": 10
};

//this will run on every node
//this runs in a bac
function run(data) {
    exit(data); //send response to aggregator
    nextData(); //fetch the next set of data
}

//this will run only on the aggregator not in a background task
function aggregate() {
    _aggregated_data_ = JSON.parse(_aggregated_data_);
    if(!_aggregated_data_) _aggregated_data_ = [];
     _new_data_ = JSON.parse(_new_data_);
    var msgLogs = _new_data_.results.MsgLog;
    for (var x in msgLogs){
         _aggregated_data_.push(msgLogs[x].ts_requested);
    }
    _aggregated_data_.push(_new_data_.results.MsgLog[0].ts_requested);
    return JSON.stringify(_aggregated_data_);
}

//this will run on the aggregator when everything is complete
function onFinish() {
    _service_.addLog(_request_id_, "inside onfinish");
    print("on finish called^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
    print(_all_data_);
}
