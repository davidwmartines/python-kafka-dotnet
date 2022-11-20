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
        pr = {
            "id": id,
            "url": f"https://github.com/bartsimpson/repo/pulls/{id}",
            "author": "Bart Simpson",
            "title": "test pr",
            "opened_on": datetime.utcnow(),
        }
        producer.produce(
            topic=topic,
            key=str(id),
            value=avro_serializer(pr, SerializationContext(topic, MessageField.VALUE)),
        )
        sleep(2)


if __name__ == "__main__":
    sys.exit(main(*sys.argv))
