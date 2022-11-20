import logging
import os
import sys
from datetime import datetime
from time import sleep

import envparse
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (MessageField, SerializationContext,
                                           StringSerializer)

env = envparse.Env()
env.read_envfile()


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main(*argv):

    topic = "pullrequests"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/schemas/pull_request.avsc") as f:
        schema_str = f.read()

    schema_registry_conf = {"url": env("SCHEMA_REGISTRY_URL")}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    producer_conf = {"bootstrap.servers": env("BOOTSTRAP_SERVERS")}

    producer = Producer(producer_conf)

    id = 0
    while True:
        id += 1
        logger.info("opening a pull request...")
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
            value=avro_serializer(
                pr, SerializationContext(topic, MessageField.VALUE)
            ),
        )
        sleep(5)


if __name__ == "__main__":
    sys.exit(main(*sys.argv))
