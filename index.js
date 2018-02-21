
var express = require("express");
var app = express();
var bodyParser = require("body-parser");
var http = require('http');

// local modules
var eventHubListener = require("./EventHubListener.js");
var model = require("./model");

var PORT = process.env.APP_PORT || 8098;
var APP_VERSION = "0.0.3"
var APP_NAME = "EventMonitorMS"
console.log("Running " + APP_NAME + " version " + APP_VERSION);

eventHubListener.subscribeToEvents(
    (message) => {
      console.log("EventBridge: Received event from event hub");
      try {
        var event = JSON.parse(message);         
        handleEventHubEvent(event);
  
      } catch (error) {
        console.log("failed to parse message from event hub", error);
  
      }
    }
  );

  function handleEventHubEvent(event) {
    console.log("Event payload " + JSON.stringify(event));
    // store event in Elastic Search Index
    model.saveEvent( event).then((result) => {
console.log("Event was saved to index")
    }).catch( function(e){
        console.error(e)
    })
   
  }


var app = express();
var server = http.createServer(app);
server.listen(PORT, function () {
    console.log('Soaring through the Clouds - the Sequel Microservice' + APP_NAME + ' running, Express is listening... at ' + PORT + " for /health, /about and /shipping API calls");
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
    var health = { "status": "OK", "uptime": process.uptime(),"version": APP_VERSION }
    res.setHeader('Content-Type', 'application/json');
    res.send(health);
});
