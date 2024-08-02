package otel

import (
	"context"
	"fmt"
	otellogger "github.com/paypal/hera/utility/logger/otel"
	otelconfig "github.com/paypal/hera/utility/logger/otel/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/paypal/hera/utility/logger"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
)

// This initializes console exported for metrics
func initializeConsoleExporter() (*metric.MeterProvider, error) {
	otelconfig.OTelConfigData = &otelconfig.OTelConfig{
		Host:                       "localhost",
		MetricsPort:                4318,
		TracePort:                  4318,
		Enabled:                    true,
		OtelMetricGRPC:             false,
		OtelTraceGRPC:              false,
		ResolutionTimeInSec:        6,
		OTelErrorReportingInterval: 10,
		PoolName:                   "occ-testapp",
		MetricNamePrefix:           "pp.occ",
		MetricsURLPath:             DefaultMetricsPath,
	}
	hostname, _ := os.Hostname()

	resource := resource.NewWithAttributes("OCC resource",
		attribute.String("container_host", hostname),
		attribute.String("az", "devTest"),
		attribute.String("environment", "dev"),
		attribute.String("application", "occ-testapp"),
	)
	metricExporter, err := stdoutmetric.New(stdoutmetric.WithPrettyPrint())
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "failed to initialize metric stdout exporter:", err)
		return nil, err
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithResource(resource),
		metric.WithReader(metric.NewPeriodicReader(metricExporter,
			// Default is 1m. Set to 3s for demonstrative purposes.
			metric.WithInterval(3*time.Second))),
	)
	otel.SetMeterProvider(meterProvider)
	return meterProvider, nil
}

func initializeCustomOTelExporter(t *testing.T) func(ctx context.Context) error {
	otelconfig.OTelConfigData = &otelconfig.OTelConfig{
		Host:                       "localhost",
		MetricsPort:                4318,
		TracePort:                  4318,
		Enabled:                    true,
		OtelMetricGRPC:             false,
		OtelTraceGRPC:              false,
		ResolutionTimeInSec:        3,
		OTelErrorReportingInterval: 10,
		PoolName:                   "occ-testapp",
		MetricNamePrefix:           "pp.occ",
		MetricsURLPath:             DefaultMetricsPath,
	}
	otelconfig.SetOTelIngestToken("welcome123")
	ctx := context.Background()
	shutdownFn, err := otellogger.Init(ctx)

	if err != nil {
		t.Log(fmt.Sprintf("failed to initialize OTEL sdk during test, error: %v", err))
		t.Fatalf("failed to initialize OTEL sdk during test, error: %v", err)
	}
	return shutdownFn
}

func TestVerifyStateLogMetricsInitilization(t *testing.T) {
	mc := runMockCollector(t, mockCollectorConfig{
		Port: 4318,
	})
	defer mc.MustStop(t)

	_, err := initializeConsoleExporter()
	if err != nil {
		t.Fail()
	}

	err = otellogger.StartMetricsCollection(context.Background(), 5, otellogger.WithMetricProvider(otel.GetMeterProvider()), otellogger.WithAppName("occ-testapp"))

	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to initialize Metric Collection service")
		t.Fail()
	}
	time.Sleep(15 * time.Second)
	otellogger.StopMetricCollection()
}

func TestVerifyStateLogMetricsInitilizationAndContextWithTimeout(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mc := runMockCollector(t, mockCollectorConfig{
		Port: 4318,
	})
	defer mc.MustStop(t)

	_, err := initializeConsoleExporter()
	if err != nil {
		t.Fail()
	}

	err = otellogger.StartMetricsCollection(context.Background(), 5, otellogger.WithMetricProvider(otel.GetMeterProvider()), otellogger.WithAppName("occ-testapp"))
	defer otellogger.StopMetricCollection()

	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to initialize Metric Collection service")
		t.Fail()
	}
}

func TestSendingStateLogMetrics(t *testing.T) {
	mc := runMockCollector(t, mockCollectorConfig{
		Port:    4318,
		WithTLS: false,
	})
	defer mc.MustStop(t)

	shutDownFn := initializeCustomOTelExporter(t)
	defer shutDownFn(context.Background())

	time.Sleep(2 * time.Second)

	err := otellogger.StartMetricsCollection(context.Background(), 5, otellogger.WithMetricProvider(otel.GetMeterProvider()), otellogger.WithAppName("occ-testapp"))

	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to initialize Metric Collection service")
		t.Fail()
	}
	//"init", "acpt", "wait", "busy", "schd", "fnsh", "quce", "asgn", "idle", "bklg", "strd", "cls"}
	var stateData = map[string]int64{
		"init": 6,
		"acpt": 10,
		"wait": 5,
		"busy": 2,
		"idle": 5,
		"bklg": 0,
		"req":  5,
		"resp": 5,
	}
	workersStateData := otellogger.WorkersStateData{
		ShardId:    1,
		WorkerType: 1,
		InstanceId: 0,
		StateData:  stateData,
	}
	otellogger.AddDataPointToOTELStateDataChan(&workersStateData)

	defer otellogger.StopMetricCollection() //Clean channel

	logger.GetLogger().Log(logger.Info, "Data Sent successfully for instrumentation")
	time.Sleep(5 * time.Second)
	metricsData := mc.GetMetrics()
	if len(metricsData) < 11 {
		t.Fatalf("got %d, wanted %d", len(metricsData), 11)
	}
}

func TestSendingStateLogMetricsConsoleExporter(t *testing.T) {
	cont, err := initializeConsoleExporter()
	if err != nil {
		t.Fail()
	}

	err2 := otellogger.StartMetricsCollection(context.Background(), 100, otellogger.WithMetricProvider(otel.GetMeterProvider()), otellogger.WithAppName("occ-testapp2"))

	if err2 != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to initialize Metric Collection service")
		t.Fail()
	}

	var stateData = map[string]int64{
		"init": 0,
		"acpt": 15,
		"wait": 10,
		"busy": 4,
		"idle": 7,
		"bklg": 0,
	}

	var stateData2 = map[string]int64{
		"init": 2,
		"acpt": 15,
		"wait": 10,
		"busy": 4,
		"idle": 8,
		"bklg": 0,
	}
	workersStateData := otellogger.WorkersStateData{
		ShardId:    0,
		WorkerType: 0,
		InstanceId: 0,
		StateData:  stateData,
	}

	workersStateData2 := otellogger.WorkersStateData{
		ShardId:    2,
		WorkerType: 0,
		InstanceId: 0,
		StateData:  stateData2,
	}

	otellogger.AddDataPointToOTELStateDataChan(&workersStateData)
	time.Sleep(150 * time.Millisecond)
	otellogger.AddDataPointToOTELStateDataChan(&workersStateData2)
	logger.GetLogger().Log(logger.Info, "Data Sent successfully for instrumentation")
	time.Sleep(2 * time.Second)

	var stateData3 = map[string]int64{
		"init": 0,
		"acpt": 1,
		"wait": 10,
		"busy": 4,
		"idle": 17,
		"bklg": 0,
	}

	var stateData4 = map[string]int64{
		"init": 2,
		"acpt": 0,
		"wait": 10,
		"busy": 4,
		"idle": 8,
		"bklg": 5,
	}
	workersStateData3 := otellogger.WorkersStateData{
		ShardId:    0,
		WorkerType: 0,
		InstanceId: 0,
		StateData:  stateData3,
	}

	workersStateData4 := otellogger.WorkersStateData{
		ShardId:    2,
		WorkerType: 0,
		InstanceId: 0,
		StateData:  stateData4,
	}
	otellogger.AddDataPointToOTELStateDataChan(&workersStateData3)
	time.Sleep(150 * time.Millisecond)
	otellogger.AddDataPointToOTELStateDataChan(&workersStateData4)
	otellogger.StopMetricCollection()
	if err3 := cont.Shutdown(context.Background()); err3 != nil {
		logger.GetLogger().Log(logger.Info, "failed to stop the metric controller:", err3)
	}
}

func TestOCCStateLogGeneratorWithRandomValues(t *testing.T) {
	cont, err := initializeConsoleExporter()
	if err != nil {
		t.Fail()
	}
	defer cont.Shutdown(context.Background())

	err2 := otellogger.StartMetricsCollection(context.Background(), 1000, otellogger.WithMetricProvider(otel.GetMeterProvider()), otellogger.WithAppName("occ-testapp"))
	defer otellogger.StopMetricCollection()
	go dataGenerator()

	if err2 != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to initialize Metric Collection service")
		t.Fatalf("TestOCCStatelogGenerator failed with error %v", err)
	}
	<-time.After(time.Second * time.Duration(30))
}

func dataGenerator() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	waitTime := time.Second * 1

	metricNames := [11]string{"init", "acpt", "wait", "busy", "schd", "fnsh", "quce", "asgn", "idle", "bklg", "strd"}

	timer := time.NewTimer(waitTime)

	defer timer.Stop()

mainloop:
	for {
		select {
		case <-timer.C:
			// Initialize statedata object
			workerStatesData := otellogger.WorkersStateData{
				ShardId:    0,
				WorkerType: 1,
				InstanceId: 0,
				StateData:  make(map[string]int64),
			}
			var numberofMetrics int = 11
			var totalSum int = 100
			var tempSum int = 0
			for index := 0; index < numberofMetrics; index++ {
				exactpart := int(totalSum / numberofMetrics)
				randVal := rand.Intn(exactpart)
				randomValue := int(int(exactpart/2) + randVal)
				value := If(tempSum+randomValue > totalSum, totalSum-tempSum, randomValue)
				workerStatesData.StateData[metricNames[index]] = int64(value)
				tempSum += value
			}
			//Random index
			randIndex := rand.Intn(len(metricNames))
			workerStatesData.StateData[metricNames[randIndex]] += int64(totalSum - tempSum)
			otellogger.AddDataPointToOTELStateDataChan(&workerStatesData)
			timer.Reset(waitTime)
		case <-ctx.Done():
			logger.GetLogger().Log(logger.Info, "Timedout, so context closed")
			break mainloop
		}
	}
}

// Go terenary inplementation
func If[T any](cond bool, vtrue, vfalse T) T {
	if cond {
		return vtrue
	}
	return vfalse
}
