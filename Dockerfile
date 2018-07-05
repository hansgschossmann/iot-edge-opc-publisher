FROM microsoft/dotnet:2.0-runtime-stretch AS base

COPY /src /build/src

WORKDIR /build/src
RUN dotnet restore
RUN dotnet publish --configuration Release --output /build/out

WORKDIR /docker
ENTRYPOINT ["dotnet", "/build/out/OpcPublisher.dll"]
