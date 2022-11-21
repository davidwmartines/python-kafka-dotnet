using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using github.events;

namespace Example.Consumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("dotnet consumer starting up");

            Console.WriteLine("Ensuring topics exist in Kafka");
            await EnsureTopics();

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
                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var consumer =
                    new ConsumerBuilder<string, PullRequest>(consumerConfig)
                        .SetValueDeserializer(new AvroDeserializer<PullRequest>(schemaRegistry).AsSyncOverAsync())
                        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                        .Build())
                {
                    Console.WriteLine("Subscribing to topic");

                    consumer.Subscribe("pullrequests");

                    Console.WriteLine("Starting consume loop");
                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(cts.Token);

                                var eventType = "";
                                if (consumeResult.Message.Headers.TryGetLastBytes("ce_type", out var bytes))
                                {
                                    eventType = Encoding.UTF8.GetString(bytes);
                                }

                                var pr = consumeResult.Message.Value;
                                Console.WriteLine($"Received {eventType}  {pr.id} '{pr.title}' from {pr.author}, opened on {pr.opened_on}.");
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

        private static async Task EnsureTopics()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Environment.GetEnvironmentVariable("BOOTSTRAP_SERVERS") }).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                        new TopicSpecification { Name = "pullrequests", ReplicationFactor = 1, NumPartitions = 1 } });
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occurred creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }
        }

    }
}
