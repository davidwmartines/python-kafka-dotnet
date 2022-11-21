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
        const string topicName = "pullrequests";
        private static readonly ConsumerConfig consumerConfig = new ConsumerConfig
        {
            BootstrapServers = Environment.GetEnvironmentVariable("BOOTSTRAP_SERVERS"),
            GroupId = Environment.GetEnvironmentVariable("CONSUMER_GROUP_ID")
        };
        private static readonly SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL")
        };
        private static readonly ProducerConfig producerConfig = new ProducerConfig
        {
            BootstrapServers = Environment.GetEnvironmentVariable("BOOTSTRAP_SERVERS")
        };

        private static readonly AdminClientConfig adminClientConfig = new AdminClientConfig
        {
            BootstrapServers = Environment.GetEnvironmentVariable("BOOTSTRAP_SERVERS")
        };

        private static readonly ISchemaRegistryClient schemaRegistryClient;
        private static readonly CloudEventFormatter cloudEventFormatter;

        static Program()
        {
            schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);
            cloudEventFormatter = new AvroSchemaRegistryCloudEventFormatter<PullRequest>(schemaRegistryClient, topicName);
        }


        static async Task Main(string[] args)
        {

            Console.WriteLine("dotnet consumer starting up");

            Console.WriteLine("Ensuring topics exist in Kafka");
            await EnsureTopic(topicName);
            
            CancellationTokenSource cts = new CancellationTokenSource();

            await Task.Run(async () =>
            {
                using (var consumer =
                    new ConsumerBuilder<string?, byte[]>(consumerConfig)
                        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                        .Build())
                {
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
                                if (consumeResult == null)
                                {
                                    continue;
                                }
                                var cloudEvent = consumeResult.Message.ToCloudEvent(cloudEventFormatter);
                                if (cloudEvent.Data != null)
                                {
                                    var pr = (PullRequest)cloudEvent.Data;
                                    Console.WriteLine($"Received {cloudEvent.Type} event for Pull Request {pr.id} '{pr.title}', by {pr.author}, opened on {pr.opened_on}.  Status: {pr.status?.ToString() ?? ""}");

                                    switch (cloudEvent.Type)
                                    {
                                        case "pullrequest_created":
                                            await ReviewPullRequest(pr);
                                            break;

                                        default:
                                            break;
                                    }

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

        private static async Task ReviewPullRequest(PullRequest pr)
        {
            if (pr.id % 2 == 0)
            {
                pr.comments.Add(new Comment { author = "dotnet-consumer", body = "LGTM!", time = DateTime.UtcNow });
                pr.status = Status.APPROVED;
            }
            else
            {
                pr.comments.Add(new Comment { author = "dotnet-consumer", body = "Please address these issues.", time = DateTime.UtcNow });
            }

            var ce = new CloudEvent()
            {
                Id = Guid.NewGuid().ToString(),
                Type = "pullrequest_reviewed",
                Time = DateTime.UtcNow,
                Source = new Uri("producer://dotnet-consumer"),
                Data = pr
            };
            ce["partitionkey"] = pr.id.ToString();

            using (var producer = new ProducerBuilder<string, byte[]>(producerConfig).Build())
            {
                var message = ce.ToKafkaMessage(ContentMode.Binary, cloudEventFormatter) as Message<string, byte[]>;
                await producer
                    .ProduceAsync(topicName, message)
                    .ContinueWith(task =>
                        {
                            if (!task.IsFaulted)
                            {
                                Console.WriteLine($"produced to: {task.Result.TopicPartitionOffset}");
                                return;
                            }
                            Console.WriteLine($"error producing message: {task.Exception?.InnerException}");
                        });
                producer.Flush();
            }
        }

        private static async Task EnsureTopic(string topicName)
        {
            using (var adminClient = new AdminClientBuilder(adminClientConfig).Build())
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
        private readonly ISerializer<T> serializer;
        private readonly string topic;

        public AvroSchemaRegistryCloudEventFormatter(ISchemaRegistryClient schemaRegistryClient, string topic)
        {
            this.topic = topic;
            this.deserializer = new AvroDeserializer<T>(schemaRegistryClient).AsSyncOverAsync();
            this.serializer = new AvroSerializer<T>(schemaRegistryClient).AsSyncOverAsync();
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
            if (cloudEvent.Data != null && cloudEvent.Data is T)
            {
                return this.serializer.Serialize((T)cloudEvent.Data, new SerializationContext(MessageComponentType.Value, topic));
            }
            return null;
        }

        public override ReadOnlyMemory<byte> EncodeStructuredModeMessage(CloudEvent cloudEvent, out ContentType contentType)
        {
            throw new NotImplementedException();
        }

        protected override string? InferDataContentType(object data)
        {
            return "application/avro";
        }
    }
}
