module github.com/paypal/hera

go 1.20

require juno v1.1.0

replace juno => ./juno

require (
	github.com/go-sql-driver/mysql v1.7.1
	github.com/godror/godror v0.26.3
	github.com/golang/snappy v0.0.4
	github.com/kffl/speedbump v1.1.0
	github.com/klauspost/compress v1.17.11
	github.com/lib/pq v1.10.3
	github.com/stretchr/testify v1.8.4
	go.opentelemetry.io/otel v1.24.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.24.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.24.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.24.0
	go.opentelemetry.io/otel/metric v1.24.0
	go.opentelemetry.io/otel/sdk v1.24.0
	go.opentelemetry.io/otel/sdk/metric v1.24.0
	go.opentelemetry.io/proto/otlp v1.2.0
	golang.org/x/crypto v0.31.0
	google.golang.org/protobuf v1.34.1
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/codahale/sss v0.0.0-20160501174526-0cb9f6d3f7f1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/go-logfmt/logfmt v0.5.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.20.0 // indirect
	github.com/hashicorp/go-hclog v1.2.1 // indirect
	github.com/jbarham/cdb v0.0.0-20200301055225-9d6f6caadef0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/paypal/go.crypto v0.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	go.opentelemetry.io/otel/trace v1.24.0 // indirect
	golang.org/x/net v0.33.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/term v0.27.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240520151616-dc85e6b867a5 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240515191416-fc5f0ca64291 // indirect
	google.golang.org/grpc v1.64.1 // indirect
        github.com/davecgh/go-spew v1.1.1 // indirect
        github.com/pmezard/go-difflib v1.0.0 // indirect
        github.com/spacemonkeygo/openssl v0.0.0-20181017203307-c2dcc5cca94a // indirect
        github.com/spacemonkeygo/spacelog v0.0.0-20180420211403-2296661a0572 // indirect
        gopkg.in/yaml.v3 v3.0.1 // indirect
)
