package testutil

const otelConfigYamlData = `receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"
      http:
        endpoint: "0.0.0.0:4318"

exporters:
  logging:
    loglevel: debug
  file:
    path: /var/log/otel/otel_collector.log

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: []
      exporters: [logging, file]

    metrics:
      receivers: [otlp]
      processors: []
      exporters: [logging, file]
`

const otelCollectorDockerDef = `version: '3.8'

services:
  otel_basic-collector:
    container_name: otel_basic-collector
    image: otel/opentelemetry-collector-contrib:latest
    ports:
      - "4317:4317"  #gRPC port
      - "4318:4318" #HTTP port
    volumes:
      - ./otel_config.yaml:/etc/otel/config.yaml
      - ./otel_logs:/var/log/otel

    command: ["--config", "/etc/otel/config.yaml"]
`
