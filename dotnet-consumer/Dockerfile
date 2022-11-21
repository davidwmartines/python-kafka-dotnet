FROM mcr.microsoft.com/dotnet/sdk:7.0 AS restore-env

WORKDIR /App
COPY ./dotnet-consumer.csproj ./
RUN dotnet restore

FROM restore-env as build-env
COPY . ./
RUN dotnet publish -c Release -o out

FROM mcr.microsoft.com/dotnet/runtime:7.0
WORKDIR /App
COPY --from=build-env /App/out .
CMD ["dotnet", "dotnet-consumer.dll"]
