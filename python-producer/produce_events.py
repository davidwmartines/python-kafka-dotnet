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

from faker import Faker


def main(*argv):
    env = envparse.Env()
    env.read_envfile()

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logger = logging.getLogger()

    Faker.seed(0)
    fake = Faker(["en-US"])

    topic = "pullrequests"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/schemas/pull_request.avsc") as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": env("SCHEMA_REGISTRY_URL")})
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    producer = Producer({"bootstrap.servers": env("BOOTSTRAP_SERVERS")})

    while True:

        pull_request_id = fake.pyint()
        pull_request = {
            "id": pull_request_id,
            "url": f"https://github.com/our-org/our-repo/pulls/{id}",
            "author": fake.profile()["username"],
            "title": fake.sentence(5),
            "opened_on": datetime.utcnow(),
            "status": "OPEN",
        }
        event = CloudEvent.create(
            {
                "type": "pullrequest_created",
                "source": "python-producer",
                "partitionkey": str(pull_request_id),
                "content-type": "application/avro",
            },
            data=pull_request,
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
        logger.info(
            f"Opened pull request {pull_request['id']} \"{pull_request['title']}\", by {pull_request['author']}, on {pull_request['opened_on']}."
        )
        sleep(2)


if __name__ == "__main__":
    sys.exit(main(*sys.argv))
