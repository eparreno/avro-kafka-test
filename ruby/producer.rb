require "avro"
require "kafka"

KAFKA_HOST = "localhost:9092"
KAFKA_TOPIC = "avro"

kafka = Kafka.new(seed_brokers: [KAFKA_HOST])
schema = Avro::Schema.parse(File.open("../schemas/user.avsc", "rb").read)

def encode(schema)
  writer = Avro::IO::DatumWriter.new(schema)
  buffer = StringIO.new("".force_encoding("BINARY"))
  encoder = Avro::IO::BinaryEncoder.new(buffer)

  # use old hash style, won"t work otherwise
  datum = { "created_at" => "2015-10-21T09:47:50-04:00", "name" => "John", "favorite_color" => "red", "favorite_number" => 1 }

  writer.write(datum, encoder)
  buffer.string
end

kafka.deliver_message(encode(schema), topic: KAFKA_TOPIC)
