
var express = require("express");
var app = express();
var bodyParser = require("body-parser");
var http = require('http');
//const avro = require('avsc');


// local modules
var eventHubListener = require("./EventHubListener.js");
var avroEventHubListener = require("./AvroEventHubListener.js");
var model = require("./model");

var PORT = process.env.APP_PORT || 8098;
var APP_VERSION = "0.0.7"
var APP_NAME = "EventMonitorMS"
console.log("Running " + APP_NAME + " version " + APP_VERSION);

eventHubListener.subscribeToEvents(
    (message) => {
        console.log("EventBridge: Received event from event hub");
        try {
            var event = JSON.parse(message);
            handleEventHubEvent(event);
        } catch (error) {
            console.log("failed to handle message from event hub", error);

        }
    }
);

avroEventHubListener.subscribeToEvents(
    (message) => {
        console.log("EventBridge: Received AVRO Product event from event hub");
        console.log(message)
        try {
            handleProductEventHubEvent(message);
        } catch (error) {
            console.log("failed to handle message from event hub", error);

        }

    }
);


async function handleProductEventHubEvent(message) {
    console.log("Event payload " + JSON.stringify(message));
    var event = {
        "eventType": "ProductEvent",
        "payload": {
            "productIdentifier": message.productId,
            "productCode": message.productCode.string,
            "productName": message.productName.string,
            "imageUrl": message.imageUrl?message.imageUrl.string:null,
            "price": message.price?message.price.double:null,
            "size": message.size?message.size.int:null,
            "weight": message.weight?message.weight.double:null,
            "categories": message.categories,
            "tags": message.tags,
            "dimension": message.dimension? { 
                "unit": message.dimension.unit?message.dimension.unit.string:null,
                "length": message.dimension.length?message.dimension.length.double:null,
                "height": message.dimension.height?message.dimension.height.double:null,
                "width": message.dimension.width?message.dimension.width.double:null
            }:null,
            "color": message.color?message.color.string:null
        
        }
        ,
        "module": "products.microservice",
        "transactionIdentifier": message.productId,
        "timestamp": getTimestampAsString()
    }
    // store event in Elastic Search Index
    var result = await model.saveProductEvent(event);
}


getTimestampAsString = function (theDate) {
    var sd = theDate ? theDate : new Date();
    try {
        var ts = sd.getUTCFullYear() + '-' + (sd.getUTCMonth() + 1) + '-' + sd.getUTCDate() + 'T' + sd.getUTCHours() + ':' + sd.getUTCMinutes() + ':' + sd.getSeconds();
        return ts;
    } catch (e) { "getTimestampAsString exception " + JSON.stringify(e) }
}

async function handleEventHubEvent(event) {
    console.log("Event payload " + JSON.stringify(event));
    try {
        // store event in Elastic Search Index
        var result = await model.saveEvent(event);

        console.log("Event was saved to index")
    } catch (e) {
        console.error("Error in saving event " + JSON.stringify(e))
    }

}


var app = express();
var server = http.createServer(app);
server.listen(PORT, function () {
    console.log('Soaring through the Clouds - the Sequel Microservice' + APP_NAME + ' running, Express is listening... at ' + PORT + " for /health, /about and /events API calls");
});

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json({ type: '*/*' }));
app.get('/about', function (req, res) {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.write("<h3>About " + APP_NAME + ", Version " + APP_VERSION + "</h3><br/>");
    res.write("Supported URLs:<br/>");
    res.write("/health (GET)<br/>");
    res.write("NodeJS runtime version " + process.version + "<br/>");
    res.write("incoming headers" + JSON.stringify(req.headers) + "<br/>");
    res.write("Environment variables: DEMO_GREETING: <br/>");
    res.write("ELASTIC_CONNECTOR: " + process.env.ELASTIC_CONNECTOR + "<br/>");
    res.end();
});


app.get('/health', function (req, res) {
    var health = { "status": "OK", "uptime": process.uptime(), "version": APP_VERSION }
    res.setHeader('Content-Type', 'application/json');
    res.send(health);
});
