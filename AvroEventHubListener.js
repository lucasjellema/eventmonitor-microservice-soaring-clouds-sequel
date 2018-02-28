var KafkaAvro = require('kafka-avro');
var fmt = require('bunyan-format');
var kafkaLog  = KafkaAvro.getLogger();

var KAFKA_ZK_SERVER_PORT = 2181;
var EVENT_HUB_PUBLIC_IP = process.env.EVENT_HUB_HOST || '129.150.77.116';
var SCHEMA_REGISTRY = 'http://129.150.114.134:8081'
var APP_NAME = 'AvroEventHubListener';
var APP_VERSION = '0.0.7';
var topics = [topicName];


var avroEventHubListener = module.exports;


var kafkaAvro = new KafkaAvro({
    kafkaBroker: '129.150.77.116:6667',
    schemaRegistry: 'http://129.150.114.134:8081',
    parseOptions: { wrapUnions: true }
});

kafkaAvro.init()
    .then(function() {
        console.log('Ready to use');
    });


kafkaLog.addStream({
    type: 'stream',
    stream: fmt({
        outputMode: 'short',
        levelInString: true,
    }),
    level: 'debug',
});


kafkaAvro.getConsumer({
  'group.id': 'librd-test2',
  'socket.keepalive.enable': true,
  'enable.auto.commit': true,
})
    // the "getConsumer()" method will return a bluebird promise.
    .then(function(consumer) {
        // Topic Name can be a string, or an array of strings
        var topicName = 'a516817-soaring-products';

        var stream = consumer.getReadStream(topicName, {
          waitInterval: 0
        });

        stream.on('error', function() {
          process.exit(1);
        });

        consumer.on('error', function(err) {
          console.log(err);
          process.exit(1);
        });

        stream.on('data', function(message) {
          console.log('Received message:', message.parsed);
          console.log('Received message offset:', message.offset);
          console.log('Received message raw value:', message.value);
          console.log('Received parsed message stringified:', JSON.stringify(message.parsed));
          subscribers.forEach((subscriber) => {
              subscriber(message.parsed);
      
          })
      
        });
    });
    
    
// from the Oracle Event Hub - Platform Cluster Connect Descriptor

var topicName = "a516817-soaring-products";

// // from the Oracle Event Hub - Platform Cluster Connect Descriptor
// var kafkaConnectDescriptor = process.env.EVENT_HUB_HOST||"129.150.77.116";

console.log("Running Module " + APP_NAME + " version " + APP_VERSION);
console.log("Event Hub Host " + process.env.EVENT_HUB_HOST);
console.log("Event Hub Topic " + topicName);

var subscribers = [];

avroEventHubListener.subscribeToEvents = function (callback) {
    subscribers.push(callback);
}


