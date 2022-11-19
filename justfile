# Generates the C# classes from the avro schema files in the .NET programs.
gen_schemas:
    cd ./dotnet-consumer && \
        avrogen -s ./schemas/pull_request_opened.avsc . \
            --namespace "github.events:Events" && \
    cd ..