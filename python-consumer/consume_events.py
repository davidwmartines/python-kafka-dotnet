import logging
import os
import sys
import typing

import envparse
import fastavro_gen
from cloudevents.kafka import KafkaMessage, from_binary
from confluent_kafka import Consumer, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from github.events.pull_request import PullRequest


def get_headers(message: Message) -> typing.Dict[str, bytes]:
    headers_list = message.headers()
    if headers_list:
        return {h[0]: h[1] for h in headers_list}
    return {}


def main(*argv):
    env = envparse.Env()
    env.read_envfile()

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logger = logging.getLogger()

    topic = "pullrequests"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/schemas/pull_request.avsc") as f:
        schema_str = f.read()
    schema_registry_client = SchemaRegistryClient({"url": env("SCHEMA_REGISTRY_URL")})
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        schema_str,
        from_dict=lambda d, _: fastavro_gen.fromdict(PullRequest, d),
    )

    consumer = Consumer(
        {
            "bootstrap.servers": env("BOOTSTRAP_SERVERS"),
            "group.id": "python-consumer-1",
            "auto.offset.reset": "earliest",
        }
    )

    logger.info(f"subscribing to topic {topic}")
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            logger.warn(msg.error())
            continue
        else:
            event = from_binary(
                KafkaMessage(
                    headers=get_headers(msg), key=msg.key(), value=msg.value()
                ),
                data_unmarshaller=lambda e: avro_deserializer(
                    e, SerializationContext(topic, MessageField.VALUE)
                ),
            )
            pr = event.data
            logger.info(
                f"Received {event['type']} event for pull request {pr.id}, {pr.title}.  Status {pr.status}"
            )


if __name__ == "__main__":
    sys.exit(main(*sys.argv))
