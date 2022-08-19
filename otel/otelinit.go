package otel

import (
	"context"
	"log"
	"runtime"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/histogram"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	"go.opentelemetry.io/otel/sdk/metric/export/aggregation"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var apiHistogramOnce sync.Once
var execHistogramOnce sync.Once
var fetchHistogramOnce sync.Once
var commitHistogramOnce sync.Once
var rollbackHistogramOnce sync.Once

var apiHistogram syncint64.Histogram
var execHistogram syncint64.Histogram
var fetchHistogram syncint64.Histogram
var commitHistogram syncint64.Histogram
var rollbackHistogram syncint64.Histogram

var DEFAULT_OTEL_COLLECTOR_PROTOCOL string = "http"
var DEFAULT_OTEL_COLLECTOR__IP string = "127.0.0.1"
var DEFAULT_GRPC_OTEL_COLLECTOR_PORT string = "4317"
var DEFAULT_HTTP_OTEL_COLLECTOR_PORT string = "4318"
var COLLECTOR_POLLING_INTERVAL_SECONDS int32 = 5

const METRIC_NAME_PREFIX = "pp."

var OTEL_COLLECTOR_PROTOCOL string = DEFAULT_OTEL_COLLECTOR_PROTOCOL

func initMetricProvider() func() {
	ctx := context.Background()

	var metricClient otlpmetric.Client = nil
	if OTEL_COLLECTOR_PROTOCOL == DEFAULT_OTEL_COLLECTOR_PROTOCOL {

		metricClient = otlpmetrichttp.NewClient(
			otlpmetrichttp.WithInsecure(),
			otlpmetrichttp.WithEndpoint(DEFAULT_OTEL_COLLECTOR__IP+":"+DEFAULT_HTTP_OTEL_COLLECTOR_PORT),
		)
	} else {

		metricClient = otlpmetricgrpc.NewClient(
			otlpmetricgrpc.WithInsecure(),
			otlpmetricgrpc.WithEndpoint(DEFAULT_OTEL_COLLECTOR__IP+":"+DEFAULT_GRPC_OTEL_COLLECTOR_PORT),
		)
	}

	metricExp, err := otlpmetric.New(ctx, metricClient, otlpmetric.WithMetricAggregationTemporalitySelector(aggregation.DeltaTemporalitySelector()))
	handleErr(err, "Failed to create the collector metric exporter")

	resource := resource.NewWithAttributes("empty resource",
		// semconv.TelemetrySDKLanguageGo,
		semconv.ProcessRuntimeVersionKey.String(runtime.Version()),
		semconv.TelemetrySDKVersionKey.String(otel.Version()),
	)

	pusher := controller.New(
		processor.NewFactory(
			// to capture histogram sum , counter with allocated bucket
			// simple.NewWithHistogramDistribution(histogram.WithExplicitBoundaries([]float64{5, 10, 15})),
			// to capture histogram sum
			//simple.NewWithInexpensiveDistribution(),
			// to capture histogram sum and counter
			simple.NewWithHistogramDistribution(histogram.WithExplicitBoundaries([]float64{})),
			aggregation.DeltaTemporalitySelector(),
		),
		controller.WithExporter(metricExp),
		controller.WithCollectPeriod(time.Duration(COLLECTOR_POLLING_INTERVAL_SECONDS)*time.Second),
		controller.WithResource(resource),
	)

	global.SetMeterProvider(pusher)

	err = pusher.Start(ctx)
	handleErr(err, "Failed to start metric pusher")

	return func() {
		cxt, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		// pushes any last exports to the receiver
		if err := pusher.Stop(cxt); err != nil {
			otel.Handle(err)
		}
	}

}

func handleErr(err error, message string) {
	if err != nil {
		log.Fatalf("%s: %v", message, err)
	}
}

func InitOtel() func() {
	return initMetricProvider()
}

func GetHistogramForAPI() (syncint64.Histogram, error) {
	apiHistogramOnce.Do(func() {
		meter := global.Meter("hera-server-meter")
		apiHistogram, _ = meter.SyncInt64().Histogram(
			populateMetricNamePrefix("hera.api"),
			instrument.WithDescription("Histogram for Hera API"),
			instrument.WithUnit(unit.Milliseconds),
		)

	})
	return apiHistogram, nil
}

func GetHistogramForExec() (syncint64.Histogram, error) {
	execHistogramOnce.Do(func() {
		meter := global.Meter("hera-server-meter")
		execHistogram, _ = meter.SyncInt64().Histogram(
			populateMetricNamePrefix("hera.exec"),
			instrument.WithDescription("Histogram for Hera Exec"),
			instrument.WithUnit(unit.Milliseconds),
		)

	})
	return execHistogram, nil
}

func GetHistogramForFetch() (syncint64.Histogram, error) {
	fetchHistogramOnce.Do(func() {
		meter := global.Meter("hera-server-meter")
		fetchHistogram, _ = meter.SyncInt64().Histogram(
			populateMetricNamePrefix("hera.fetch"),
			instrument.WithDescription("Histogram for Hera fetch"),
			instrument.WithUnit(unit.Milliseconds),
		)

	})
	return fetchHistogram, nil
}

func GetHistogramForCommit() (syncint64.Histogram, error) {
	commitHistogramOnce.Do(func() {
		meter := global.Meter("hera-server-meter")
		commitHistogram, _ = meter.SyncInt64().Histogram(
			populateMetricNamePrefix("hera.commit"),
			instrument.WithDescription("Histogram for Hera commit"),
			instrument.WithUnit(unit.Milliseconds),
		)

	})
	return commitHistogram, nil
}

func GetHistogramForRollback() (syncint64.Histogram, error) {
	rollbackHistogramOnce.Do(func() {
		meter := global.Meter("hera-server-meter")
		rollbackHistogram, _ = meter.SyncInt64().Histogram(
			populateMetricNamePrefix("hera.rollback"),
			instrument.WithDescription("Histogram for Hera rollback"),
			instrument.WithUnit(unit.Milliseconds),
		)

	})
	return rollbackHistogram, nil
}

func populateMetricNamePrefix(metricName string) string {
	return METRIC_NAME_PREFIX + metricName
}
