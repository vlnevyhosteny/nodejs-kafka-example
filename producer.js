import { Producer } from "node-rdkafka";

const topic = 'TopicOne';

const producer = new Producer({
  'metadata.broker.list': 'localhost:9092'
});

const maxMessages = 20;
const genMessage = i => new Buffer(`Kafka example, message number ${i}`);

producer.on("ready", function(arg) {
  console.log(`producer ${arg.name} ready.`);

  for (var i = 0; i < maxMessages; i++) {
    producer.produce(topic, -1, genMessage(i), i);
  }

  setTimeout(() => producer.disconnect(), 0);
});

producer.on("disconnected", function(arg) {
  process.exit();
});

producer.on('event.error', function(err) {
  console.error(err);
  process.exit(1);
});

producer.on('event.log', function(log) {
  console.log(log);
});

producer.connect();
