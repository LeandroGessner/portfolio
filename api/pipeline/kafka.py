from confluent_kafka import SerializingProducer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, SchemaRegistryError
from confluent_kafka.serialization import SerializationError, StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import Schema
from .kafka_config import config
import json


class Kafka:
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
            latest_schema = schema_registry_client.get_latest_version("portfolio")
        except SchemaRegistryError:
            schema = Schema(schema_str=self.avro_schema, schema_type='AVRO')
            schema_registry_client.register_schema('portfolio', schema=schema)
            latest_schema = schema_registry_client.get_latest_version("portfolio")

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

            # TODO: serialize invalid events
            producer = Producer(self.config)
            producer.produce(
                topic=self.invalid_data_topic,
                key=key,
                value=json.dumps(data),
                on_delivery=self.delivery_report
            )

            producer.flush()

        return success
