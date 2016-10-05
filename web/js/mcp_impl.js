//reserved variables __aggregator
//prepend this script by initializing kinds, filters, orders ... to undefined
kinds = ["Account", "Enterprise"];

filters = {
    "Account": [{
            "l": "direction = ",
            "r": 0
        }],
    "Enterprise": [{
            "l": "amount >= ",
            "r": 100
        }]
};

orders = {
    "Account": ["-request_date", "age"],
    "Enterprise": ["-amount"]
};

limits = {
    "Account": 30,
    "Enterprise": 40
};

//this will run on every node

function run(data) {
    var response = [];
    for (var x in data) {
        if (data[x].age > 20) {
            response.push(data);
        }
    }
    exit(response); //send response to aggregator
    nextData(); //fetch the next set of data
}

//this will run only on the aggregator
function aggregate(prevData, newData) {
    prevData.add(newData);
}

//this will run on the aggregator when everything is complete
function onFinish(aggregatedData) {

}
