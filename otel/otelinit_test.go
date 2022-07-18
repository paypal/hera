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
)

func TestCounter(t *testing.T) {
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

	for i := 1; i <= 2500; i++ {
		requestCount.Add(ctx, 1, commonLabels...)
		time.Sleep(1 * time.Second)
		fmt.Println("Counter..it:==>" + strconv.Itoa(i))
	}

}

func TestVariableDimentionCounter(t *testing.T) {
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

	for i := 1; i <= 2500; i++ {

		min := 0
		max := 50
		commonLabelsLocal := append(commonLabels, attribute.String("sqlhash", strconv.Itoa(rand.Intn(max-min)+min)))

		requestCount.Add(ctx, 1, commonLabelsLocal...)
		time.Sleep(1 * time.Second)
		fmt.Println("Counter:==>" + strconv.Itoa(i))
	}

}

func TestHistogram(t *testing.T) {
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

	for i := 1; i <= 2500; i++ {
		min := 1
		max := 15
		duration := float64(rand.Intn(max-min) + min)
		fmt.Println("duration:==>", duration)
		requestLatency.Record(ctx, duration, commonLabels...)
		time.Sleep(1 * time.Second)
		fmt.Println("Counter:==>" + strconv.Itoa(i))
	}

}
