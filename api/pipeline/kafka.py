from confluent_kafka import SerializingProducer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, SchemaRegistryError
from confluent_kafka.serialization import SerializationError, StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import Schema
from .kafka_config import config
import json


class KafkaKantox:
    def __init__(self) -> None:
        self.config = config['kafka']
        self.sr_config: dict = config['schema_registry']
        self.avro_schema = config.get('avro_schema')
        self.valid_data_topic = 'acme.clickstream.raw.events'
        self.invalid_data_topic = 'acme.clickstream.invalid.events'
    
    def delivery_report(self, err, event):
        if err is not None:
            print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
        else:
            print(f'Temp reading for {event.key().decode("utf8")} produced to {event.topic()}')

    def send_message(
            self, 
            data: dict,
            key: str
    ) -> bool:
        success: bool = True

        schema_registry_client = SchemaRegistryClient(self.sr_config)

        try:
            latest_schema = schema_registry_client.get_latest_version("kantox-challenge")
        except SchemaRegistryError:
            schema = Schema(schema_str=self.avro_schema, schema_type='AVRO')
            schema_registry_client.register_schema('kantox-challenge', schema=schema)
            latest_schema = schema_registry_client.get_latest_version("kantox-challenge")

        kafka_config = self.config | {
            "key.serializer": StringSerializer(),
            "value.serializer": AvroSerializer(
                schema_str=latest_schema.schema.schema_str,
                schema_registry_client=schema_registry_client
            )
        }
        producer = SerializingProducer(kafka_config)

        try:
            producer.produce(
                topic=self.valid_data_topic,
                key=key,
                value=data,
                on_delivery=self.delivery_report
            )

            producer.flush()
            
        except SerializationError as e:
            print(f'Error trying to send message due to: {e}')

            success = False

            producer = Producer(self.config)
            producer.produce(
                topic=self.invalid_data_topic,
                key=key,
                value=json.dumps(data),
                on_delivery=self.delivery_report
            )

            producer.flush()

        # producer.flush()

        return success


# if __name__ == "__main__":
#     kafka = KafkaKantox()
#     data = {
#         "id": 101408,
#         "type": "page_view",
#         "event": {
#             "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/532.1 (KHTML, like Gecko) Chrome/38.0.832.0 Safari/532.1",
#             "ip": "73.220.204.243",
#             "customer_id": None,
#             "timestamp": "2022-05-20T10:38:18.648681",
#             "page": "https://xcc-webshop.com/products/553"
#         }
#     }
#
#     kafka.send_message(data=data, key='tumb')
