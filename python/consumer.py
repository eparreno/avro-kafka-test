# https://github.com/thanhson1085/python-kafka-avro
from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
import datetime

# Kafka settings
kafkaHost = "localhost:9092"
kafkaTopic = "avro"

consumer = KafkaConsumer(kafkaTopic, bootstrap_servers=[kafkaHost])

schema_path="../schemas/user.avsc"
schema = avro.schema.parse(open(schema_path).read())

for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    decoded_message = reader.read(decoder)
    print("Message processed at {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print(decoded_message)
