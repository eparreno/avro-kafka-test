from kafka import SimpleProducer, KafkaClient
import avro.schema
import io, random
from avro.io import DatumWriter
import cProfile

# Kafka settings
kafkaHost = "localhost:9092"
kafkaTopic = "avro"

kafka = KafkaClient(kafkaHost)
producer = SimpleProducer(kafka)

schema_path="../schemas/user.avsc"
schema = avro.schema.parse(open(schema_path).read())

writer = avro.io.DatumWriter(schema)
buffer = io.BytesIO()
encoder = avro.io.BinaryEncoder(buffer)

datum = {"created_at": "2015-10-21T09:47:50-04:00", "name": "123", "favorite_color": "111", "favorite_number": random.randint(0,10)}
writer.write(datum, encoder)

raw_bytes = buffer.getvalue()

producer.send_messages(kafkaTopic, raw_bytes)
