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
var type = avsc.Type.forSchema(schema)

const Consumer = kafka.Consumer
var client = new kafka.KafkaClient({kafkaHost: kafkaHost})
var consumer = new Consumer(client, [{topic: kafkaTopic}], {encoding:'buffer'})

consumer.on('message', function (message) {
  var msg = type.fromBuffer(message.value);

  console.log("Message processed at " + new Date())
  console.log(JSON.stringify(msg))
});

consumer.on('error', function (err) {
  console.log("Message error: " + err)
})

