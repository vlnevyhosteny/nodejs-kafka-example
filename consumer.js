var Kafka = require("node-rdkafka");

var consumer = new Kafka.KafkaConsumer({
  'debug': 'all',
  'metadata.broker.list': '192.168.1.104:9092',
  'group.id': 'node-rdkafka-consumer-flow-example',
  'enable.auto.commit': false
});

var topicName = 'test';

//logging debug messages, if debug is enabled
consumer.on('event.log', function(log) {
  console.log(log);
});

//logging all errors
consumer.on('event.error', function(err) {
  console.error('Error from consumer');
  console.error(err);
});

//counter to commit offsets every numMessages are received
var counter = 0;
var numMessages = 5;

consumer.on('ready', function(arg) {
  console.log('consumer ready.' + JSON.stringify(arg));

  consumer.subscribe([topicName]);
  //start consuming messages
  consumer.consume();
});

consumer.on('data', function(m) {
  counter++;

  //committing offsets every numMessages
  if (counter % numMessages === 0) {
    console.log('calling commit');
    consumer.commit(m);
  }

  // Output the actual message contents
  console.log(JSON.stringify(m));
  console.log(m.value.toString());

});

consumer.on('disconnected', function(arg) {
  console.log('consumer disconnected. ' + JSON.stringify(arg));
});

//starting the consumer
consumer.connect();

//stopping this example after 30s
setTimeout(function() {
  consumer.disconnect();
}, 3000000);