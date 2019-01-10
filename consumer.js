import { KafkaConsumer } from "node-rdkafka";

const consumer = new KafkaConsumer({
  'metadata.broker.list': 'localhost:9092',
  'group.id': 'librd-test',
  'socket.keepalive.enable': true,
  'enable.auto.commit': false
}, {
  topics: 'TopicOne',
  waitInterval: 0,
  objectMode: false
});

consumer.on("error", function(err) {
  console.error(err);
});

consumer.on("ready", function(arg) {
  console.log(`Consumer ${arg.name} ready`);
  consumer.subscribe(topics);
  consumer.consume();
});

consumer.on("data", function(m) {
  counter++;
  if (counter % numMessages === 0) {
    console.log("calling commit");
    consumer.commit(m);
  }
  console.log(m.value.toString());
});

consumer.on("disconnected", function(arg) {
  process.exit();
});

consumer.connect();

setTimeout(function() {
  consumer.disconnect();
}, 300000);
