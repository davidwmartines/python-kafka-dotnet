# Python - Kafka - .NET

Demonstration of Python and .NET programs exchanging events through Kafka.

 - Avro schema as the contract.
 - Schema stored in Confluent Schema Registry
 - [CloudEvents](https://github.com/cloudevents/spec)-standardized event structures, with Binary content-mode and Avro data serialization.   
 - C# classes generated using [avrogen](https://www.nuget.org/packages/Apache.Avro.Tools).
 - Python classes generated using [fastavro-gen](https://github.com/gudjonragnar/fastavro-gen).



## Run
```sh
docker-compose up
```

