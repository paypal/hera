package otel

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	"go.opentelemetry.io/otel/sdk/metric/export/aggregation"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
)

var apiHistogramOnce sync.Once

func initMetricProvider() func() {
	ctx := context.Background()

	otelAgentAddr, ok := os.LookupEnv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if !ok {
		otelAgentAddr = "0.0.0.0:4317"
	}

	metricClient := otlpmetricgrpc.NewClient(
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithEndpoint(otelAgentAddr))

	// metricClient := otlpmetrichttp.NewClient(
	// 	otlpmetrichttp.WithInsecure(),
	// )

	metricExp, err := otlpmetric.New(ctx, metricClient, otlpmetric.WithMetricAggregationTemporalitySelector(aggregation.DeltaTemporalitySelector()))
	handleErr(err, "Failed to create the collector metric exporter")

	pusher := controller.New(
		processor.NewFactory(
			// to capture histogram sum , counter with allocated bucket
			// simple.NewWithHistogramDistribution(histogram.WithExplicitBoundaries([]float64{5, 10, 15})),
			// to capture histogram sum
			simple.NewWithInexpensiveDistribution(),
			// to capture histogram sum and counter
			// simple.NewWithHistogramDistribution(histogram.WithExplicitBoundaries([]float64{})),
			aggregation.DeltaTemporalitySelector(),
		),
		controller.WithExporter(metricExp),
		controller.WithCollectPeriod(1*time.Second),
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
	var apiHistogram syncint64.Histogram

	apiHistogramOnce.Do(func() {
		meter := global.Meter("hera-server-meter")
		apiHistogram, _ = meter.SyncInt64().Histogram(
			"pp.hera.api",
			instrument.WithDescription("Histogram for Hera API"),
			instrument.WithUnit(unit.Milliseconds),
		)

	})
	return apiHistogram, nil
}
