# Generates the C# classes from the avro schema files in the .NET programs.
gen_classes:
    cd ./dotnet-consumer && \
        avrogen -s ./schemas/pull_request.avsc . && \
    cd ..