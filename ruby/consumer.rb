require "avro"
require "kafka"

KAFKA_HOST = "localhost:9092"
KAFKA_TOPIC = "avro"

schema = Avro::Schema.parse(File.open("../schemas/user.avsc", "rb").read)

kafka = Kafka.new(seed_brokers: [KAFKA_HOST])
consumer = kafka.consumer(group_id: "avro-test")
consumer.subscribe(KAFKA_TOPIC, start_from_beginning: false)

consumer.each_message do |message|
  begin
    buffer = StringIO.new(message.value)
    decoder = Avro::IO::BinaryDecoder.new(buffer)
    datumreader = Avro::IO::DatumReader.new(schema)
    decoded_message = datumreader.read(decoder)
    puts "Message processed at #{Time.now.strftime("%Y-%m-%d %H:%M")}"
    puts decoded_message
  rescue
    puts "Can't process message"
  end
end
