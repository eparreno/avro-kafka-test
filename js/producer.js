const avro = require('avro-js');
const avsc = require('avsc');
const fs = require('fs');
const kafka = require('kafka-node');

const kafkaHost = 'localhost:9092'
const kafkaTopic = 'avro'

var txt_schema = fs.readFileSync("../schemas/user.avsc").toString();
var schema = JSON.parse(txt_schema);

// With avro-js
// var type = avro.parse(schema)

// With avsc
const type = avsc.Type.forSchema(schema)

var datum =  {created_at: "2015-10-21T09:47:50-04:00", name: "John", favorite_color: "red", favorite_number: 1}
var buffer = type.toBuffer(datum); // Serialized object

const Producer = kafka.Producer
var client = new kafka.KafkaClient({kafkaHost: kafkaHost})
var producer = new Producer(client);

producer.on('ready', function () {
  var payload = [{ topic: kafkaTopic, messages: buffer}]
  producer.send(payload, function (err, data) {
    console.log("Message sent")
    process.exit(0)
  });
});

producer.on('error', function (err) {
  console.log("Message error: " + err)
  process.exit(1)
})
