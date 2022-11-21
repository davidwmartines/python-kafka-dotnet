# Generates the C# and python classes from the avro schema files.
gen_classes:
    cd ./dotnet-consumer && \
        avrogen -s ./schemas/pull_request.avsc . && \
    cd ..
    cd ./python-producer && \
        fastavro_gen --class-type dataclass \
        --output-dir . \
        ./schemas/pull_request.avsc && \
    cd ..
    cd ./python-consumer && \
        fastavro_gen --class-type dataclass \
        --output-dir . \
        ./schemas/pull_request.avsc && \
    cd ..


# build the docker containers
build:
    docker-compose build

# start up
start:
    docker-compose up

# kill
terminate:
    docker-compose down