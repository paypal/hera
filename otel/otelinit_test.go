package otel

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	v1 "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestCounter(t *testing.T) {

	mc := runMockCollector(t, mockCollectorConfig{
		Port: 4318,
	})
	defer mc.MustStop(t)

	defer initMetricProvider()()

	ctx := context.Background()
	meter := global.Meter("herapoc-demo-client-meter")

	requestCount, _ := meter.SyncInt64().Counter(
		"heratest_otel_counter",
		instrument.WithDescription("The number of requests processed"),
	)

	commonLabels := []attribute.KeyValue{
		attribute.String("method", "repl"),
		attribute.String("client", "cli"),
	}

	for i := 1; i <= 50; i++ {
		requestCount.Add(ctx, 1, commonLabels...)
		time.Sleep(1 * time.Second)
		fmt.Println("Counter..it:==>" + strconv.Itoa(i))
	}

	// wait for pusher
	time.Sleep(6 * time.Second)

	v1m := mc.GetMetrics()

	for _, v1mElement := range v1m {
		var val *v1.NumberDataPoint_AsInt = v1mElement.GetSum().DataPoints[0].Value.(*v1.NumberDataPoint_AsInt)
		fmt.Println(val.AsInt)
		fmt.Println("--------------------------")

	}

}

func TestVariableDimentionCounter(t *testing.T) {
	mc := runMockCollector(t, mockCollectorConfig{
		Port: 4318,
	})
	defer mc.MustStop(t)

	defer initMetricProvider()()

	ctx := context.Background()
	meter := global.Meter("herapoc-demo-client-meter")

	requestCount, _ := meter.SyncInt64().Counter(
		"heratest_demo_mdvar_counts",
		instrument.WithDescription("The number of requests processed"),
	)

	commonLabels := []attribute.KeyValue{
		attribute.String("method", "repl"),
		attribute.String("client", "cli"),
	}

	for i := 1; i <= 5; i++ {

		min := 0
		max := 50
		sqlHash := strconv.Itoa(rand.Intn(max-min) + min)
		fmt.Println("sqlHash:==>", sqlHash)
		commonLabelsLocal := append(commonLabels, attribute.String("sqlhash", sqlHash))

		requestCount.Add(ctx, 1, commonLabelsLocal...)
		time.Sleep(1 * time.Second)
		fmt.Println("Counter:==>" + strconv.Itoa(i))
	}

}

func TestHistogram(t *testing.T) {
	mc := runMockCollector(t, mockCollectorConfig{
		Port: 4318,
	})
	defer mc.MustStop(t)

	defer initMetricProvider()()

	ctx := context.Background()
	meter := global.Meter("herapoc-demo-client-meter")

	// Recorder metric example
	requestLatency, _ := meter.SyncFloat64().Histogram(
		"heratest_demo_histogram",
		instrument.WithDescription("The latency of requests processed"),
	)

	commonLabels := []attribute.KeyValue{
		attribute.String("method", "repl"),
		attribute.String("client", "cli"),
	}

	for i := 1; i <= 5; i++ {
		min := 1
		max := 15
		duration := float64(rand.Intn(max-min) + min)
		fmt.Println("duration:==>", duration)
		requestLatency.Record(ctx, duration, commonLabels...)
		time.Sleep(1 * time.Second)
		fmt.Println("Counter:==>" + strconv.Itoa(i))
	}

}

func TestGauage(t *testing.T) {
	mc := runMockCollector(t, mockCollectorConfig{
		Port: 4318,
	})
	defer mc.MustStop(t)
	defer initMetricProvider()()

	meter := global.Meter("herapoc-demo-client-meter")

	gauge, _ := meter.AsyncInt64().Gauge(
		"heratest_demo_guage",
		// instrument.WithUnit("1"),
		// instrument.WithDescription("TODO"),
	)
	min := 1
	max := 15

	if err := meter.RegisterCallback(
		[]instrument.Asynchronous{
			gauge,
		},
		func(ctx context.Context) {
			fmt.Println("Gauge::" + time.Now().String())

			duration := rand.Intn(max-min) + min
			// debug.PrintStack()
			gauge.Observe(ctx, int64(duration))
		},
	); err != nil {
		panic(err)
	}

	time.Sleep(20 * time.Second)

}
