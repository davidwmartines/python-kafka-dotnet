import logging
import os
import sys
from datetime import datetime
from time import sleep

import envparse
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from cloudevents.kafka import to_binary
from cloudevents.http import CloudEvent


env = envparse.Env()
env.read_envfile()


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main(*argv):

    topic = "pullrequests"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/schemas/pull_request.avsc") as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": env("SCHEMA_REGISTRY_URL")})
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    producer = Producer({"bootstrap.servers": env("BOOTSTRAP_SERVERS")})

    id = 0
    while True:
        id += 1
        logger.info(f"opening pull request {id}")
        pullrequest_data = {
            "id": id,
            "url": f"https://github.com/bartsimpson/repo/pulls/{id}",
            "author": "Bart Simpson",
            "title": "test pr",
            "opened_on": datetime.utcnow(),
        }
        event = CloudEvent.create(
            {
                "type": "pullrequest_created",
                "source": "python-producer",
                "partitionkey": str(id),
                "content-type": "application/avro",
            },
            data=pullrequest_data,
        )
        message = to_binary(
            event,
            data_marshaller=lambda e: avro_serializer(
                e, SerializationContext(topic, MessageField.VALUE)
            ),
        )
        producer.produce(
            topic=topic, key=message.key, headers=message.headers, value=message.value
        )
        sleep(2)


if __name__ == "__main__":
    sys.exit(main(*sys.argv))
