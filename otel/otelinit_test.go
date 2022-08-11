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
	"go.opentelemetry.io/otel/metric/unit"
	v1 "go.opentelemetry.io/proto/otlp/metrics/v1"
)

var pushinterval int = 5

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

	expected := 5
	for i := 1; i <= 5; i++ {
		requestCount.Add(ctx, 1, commonLabels...)
		time.Sleep(1 * time.Second)
	}

	// wait for pusher
	time.Sleep(time.Duration(pushinterval) * time.Second)

	v1m := mc.GetMetrics()
	var val *v1.NumberDataPoint_AsInt = v1m[0].GetSum().DataPoints[0].Value.(*v1.NumberDataPoint_AsInt)
	actual := val.AsInt

	if int(actual) != expected {
		t.Errorf("got %q, wanted %q", actual, expected)
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

	expectedFirstSqlHash := ""
	for i := 1; i <= 5; i++ {

		min := 0
		max := 50
		sqlHash := strconv.Itoa(rand.Intn(max-min) + min)
		fmt.Println("sqlHash:==>", sqlHash)
		if i == 1 {
			expectedFirstSqlHash = sqlHash
		}
		commonLabelsLocal := append(commonLabels, attribute.String("sqlhash", sqlHash))

		requestCount.Add(ctx, 1, commonLabelsLocal...)
		time.Sleep(1 * time.Second)
		fmt.Println("Counter:==>" + strconv.Itoa(i))
	}
	time.Sleep(time.Duration(pushinterval) * time.Second)
	v1m := mc.GetMetrics()

	for _, attri := range v1m[0].GetSum().DataPoints[0].Attributes {
		if attri.Key == "sqlhash" {
			actual := attri.Value.GetStringValue()
			if actual != expectedFirstSqlHash {
				t.Errorf("got %q, wanted %q", actual, expectedFirstSqlHash)
			}
		}
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
	requestLatency, _ := meter.SyncInt64().Histogram(
		"heratest_demo_histogram",
		instrument.WithDescription("The latency of requests processed"),
		instrument.WithUnit(unit.Milliseconds),
	)

	commonLabels := []attribute.KeyValue{
		attribute.String("method", "repl"),
		attribute.String("client", "cli"),
	}
	expected := 0
	for i := 1; i <= 5; i++ {
		// min := 1
		// max := 15
		// duration := float64(rand.Intn(max-min) + min)
		expected = expected + i
		duration := int64(i)
		fmt.Println("duration:==>", duration)
		requestLatency.Record(ctx, duration, commonLabels...)
		time.Sleep(1 * time.Second)
		fmt.Println("Counter:==>" + strconv.Itoa(i))
	}

	time.Sleep(time.Duration(pushinterval) * time.Second)

	v1m := mc.GetMetrics()
	// fmt.Println(v1m[0].GetSum().DataPoints[0].Value.(*v1.NumberDataPoint_AsDouble).AsDouble)
	actual := v1m[0].GetHistogram().DataPoints[0].GetSum()
	fmt.Println(actual)

	if expected != int(actual) {
		t.Errorf("got %q, wanted %q", int(actual), expected)
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
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("UT TestGauage"),
	)
	// min := 1
	// max := 15
	var duration int64 = 10
	var expected int64 = duration

	if err := meter.RegisterCallback(
		[]instrument.Asynchronous{
			gauge,
		},
		func(ctx context.Context) {
			fmt.Println("Gauge::" + time.Now().String())
			// duration := rand.Intn(max-min) + min
			// debug.PrintStack()
			gauge.Observe(ctx, duration)
			duration = duration + 5
		},
	); err != nil {
		panic(err)
	}

	time.Sleep(time.Duration(pushinterval*2) * time.Second)
	v1m := mc.GetMetrics()
	var actual = v1m[0].GetGauge().DataPoints[0].Value.(*v1.NumberDataPoint_AsInt).AsInt
	fmt.Println(">>>>>>>>>>", actual)
	if expected != actual {
		t.Errorf("got %q, wanted %q", actual, expected)
	}

}
