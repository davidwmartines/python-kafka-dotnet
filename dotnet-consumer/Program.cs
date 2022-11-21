using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using github.events;

using CloudNative.CloudEvents.Kafka;
using System.Collections.Generic;
using CloudNative.CloudEvents;
using System.Net.Mime;

namespace Example.Consumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            const string topicName = "pullrequests";

            Console.WriteLine("dotnet consumer starting up");

            Console.WriteLine("Ensuring topics exist in Kafka");
            await EnsureTopic(topicName);

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = Environment.GetEnvironmentVariable("BOOTSTRAP_SERVERS"),
                GroupId = Environment.GetEnvironmentVariable("CONSUMER_GROUP_ID")
            };
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL")
            };

            CancellationTokenSource cts = new CancellationTokenSource();

            await Task.Run(() =>
            {
                using (var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var consumer =
                    new ConsumerBuilder<string?, byte[]>(consumerConfig)
                        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                        .Build())
                {
                    var cloudEventFormatter = new AvroSchemaRegistryCloudEventFormatter<PullRequest>(schemaRegistryClient, topicName);

                    Console.WriteLine("Subscribing to topic");
                    consumer.Subscribe(topicName);

                    Console.WriteLine("Starting consume loop");
                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(cts.Token);
                                var cloudEvent = consumeResult.Message.ToCloudEvent(cloudEventFormatter);
                                if (cloudEvent.Data != null)
                                {
                                    var pr = (PullRequest)cloudEvent.Data;
                                    Console.WriteLine($"Received {cloudEvent.Type}  {pr.id} '{pr.title}' from {pr.author}, opened on {pr.opened_on}.");
                                }
                            }
                            catch (ConsumeException e)
                            {
                                Console.WriteLine($"Consume error: {e.Error.Reason}");
                                throw e;
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                    }
                }
            });

            cts.Cancel();
        }

        private static async Task EnsureTopic(string topicName)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Environment.GetEnvironmentVariable("BOOTSTRAP_SERVERS") }).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                        new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = 1 } });
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occurred creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }
        }

    }

    class AvroSchemaRegistryCloudEventFormatter<T> : CloudEventFormatter
    {
        private readonly IDeserializer<T> deserializer;
        private readonly string topic;

        public AvroSchemaRegistryCloudEventFormatter(ISchemaRegistryClient schemaRegistryClient, string topic)
        {
            this.topic = topic;
            this.deserializer = new AvroDeserializer<T>(schemaRegistryClient).AsSyncOverAsync();
        }

        public override IReadOnlyList<CloudEvent> DecodeBatchModeMessage(ReadOnlyMemory<byte> body, ContentType? contentType, IEnumerable<CloudEventAttribute>? extensionAttributes)
        {
            throw new NotImplementedException();
        }

        public override void DecodeBinaryModeEventData(ReadOnlyMemory<byte> body, CloudEvent cloudEvent)
        {
            cloudEvent.Data = this.deserializer.Deserialize(body.Span, false, new SerializationContext(MessageComponentType.Value, topic));
        }

        public override CloudEvent DecodeStructuredModeMessage(ReadOnlyMemory<byte> body, ContentType? contentType, IEnumerable<CloudEventAttribute>? extensionAttributes)
        {
            throw new NotImplementedException();
        }

        public override ReadOnlyMemory<byte> EncodeBatchModeMessage(IEnumerable<CloudEvent> cloudEvents, out ContentType contentType)
        {
            throw new NotImplementedException();
        }

        public override ReadOnlyMemory<byte> EncodeBinaryModeEventData(CloudEvent cloudEvent)
        {
            throw new NotImplementedException();
        }

        public override ReadOnlyMemory<byte> EncodeStructuredModeMessage(CloudEvent cloudEvent, out ContentType contentType)
        {
            throw new NotImplementedException();
        }
    }
}
