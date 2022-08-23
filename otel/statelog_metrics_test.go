package otel

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/paypal/hera/utility/logger"
	stdout "go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric/global"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
)

//This initializes console exported for metrics
func initializeConsoleExporter() (*controller.Controller, error) {
	exporter, err := stdout.New(stdout.WithPrettyPrint())
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "failed to initialize metric stdout exporter:", err)
		return nil, err
	}

	cont := controller.New(
		processor.NewFactory(
			simple.NewWithInexpensiveDistribution(),
			exporter,
		),
		controller.WithExporter(exporter),
		controller.WithCollectPeriod(3*time.Second),
	)

	if err := cont.Start(context.Background()); err != nil {
		logger.GetLogger().Log(logger.Alert, "failed to start the metric controller:", err)
		return nil, err
	}
	global.SetMeterProvider(cont)
	return cont, nil
}

func TestVerifyStatelogMetricsInitilization(t *testing.T) {
	mc := runMockCollector(t, mockCollectorConfig{
		Port: 4318,
	})
	defer mc.MustStop(t)

	defer initMetricProvider()()

	dataChannel := make(chan WorkersStateData, 5)

	err := StartMetricsCollection(dataChannel, WitthMetricProvider(global.MeterProvider()), WithOCCName("occ-testapp"))

	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to initialize Metric Collection service")
		t.Fail()
	}
	time.Sleep(15 * time.Second)
	close(dataChannel)
}

func TestVerifyStatelogMetricsInitilizationAndContextWithTimeout(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mc := runMockCollector(t, mockCollectorConfig{
		Port: 4318,
	})
	defer mc.MustStop(t)

	defer initMetricProvider()()

	dataChannel := make(chan WorkersStateData, 5)

	err := StartMetricsCollection(dataChannel, WitthMetricProvider(global.MeterProvider()), WithOCCName("occ-testapp"))

	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to initialize Metric Collection service")
		t.Fail()
	}
}

func TestSendingStateLogMetrics(t *testing.T) {
	mc := runMockCollector(t, mockCollectorConfig{
		Port: 4318,
	})
	defer mc.MustStop(t)

	defer initMetricProvider()()

	dataChannel := make(chan WorkersStateData, 5)

	err := StartMetricsCollection(dataChannel, WitthMetricProvider(global.MeterProvider()), WithOCCName("occ-testapp"))

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
	}
	workersStateData := WorkersStateData{
		ShardId:    1,
		WorkerType: 1,
		InstanceId: 0,
		StateData:  stateData,
	}

	dataChannel <- workersStateData
	logger.GetLogger().Log(logger.Info, "Data Sent successfully for instrumentation")
	time.Sleep(15 * time.Second)
	//logger.GetLogger().Log(logger.Info, fmt.Sprintf("Length of channel is: %d", len(dataChannel)))
	if len(dataChannel) > 0 {
		t.Fail()
	}
	close(dataChannel)
}

func TestSendingStateLogMetricsConsoleExporter(t *testing.T) {
	cont, err := initializeConsoleExporter()
	if err != nil {
		t.Fail()
	}
	dataChannel := make(chan WorkersStateData, 5)

	err2 := StartMetricsCollection(dataChannel, WitthMetricProvider(global.MeterProvider()), WithOCCName("occ-testapp2"))

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
	workersStateData := WorkersStateData{
		ShardId:    0,
		WorkerType: 0,
		InstanceId: 0,
		StateData:  stateData,
	}

	workersStateData2 := WorkersStateData{
		ShardId:    2,
		WorkerType: 0,
		InstanceId: 0,
		StateData:  stateData2,
	}

	dataChannel <- workersStateData
	time.Sleep(150 * time.Millisecond)
	dataChannel <- workersStateData2
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
	workersStateData3 := WorkersStateData{
		ShardId:    0,
		WorkerType: 0,
		InstanceId: 0,
		StateData:  stateData3,
	}

	workersStateData4 := WorkersStateData{
		ShardId:    2,
		WorkerType: 0,
		InstanceId: 0,
		StateData:  stateData4,
	}
	dataChannel <- workersStateData3
	time.Sleep(150 * time.Millisecond)
	dataChannel <- workersStateData4

	close(dataChannel)
	if err3 := cont.Stop(context.Background()); err3 != nil {
		logger.GetLogger().Log(logger.Info, "failed to stop the metric controller:", err3)
	}
}

func TestOCCStatelogGenerator(t *testing.T) {
	cont, err := initializeConsoleExporter()
	if err != nil {
		t.Fail()
	}
	dataChannel := make(chan WorkersStateData, 5)

	defer func(dataChan chan WorkersStateData) {
		//close channel
		close(dataChan)
		logger.GetLogger().Log(logger.Info, "Channel `dataChan` has been closed.")
	}(dataChannel)

	go dataGenerator(dataChannel)

	err2 := StartMetricsCollection(dataChannel, WitthMetricProvider(global.MeterProvider()), WithOCCName("occ-testapp"))

	if err2 != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to initialize Metric Collection service")
		t.Fail()
	}
	<-time.After(time.Second * time.Duration(25))
	if err3 := cont.Stop(context.Background()); err3 != nil {
		logger.GetLogger().Log(logger.Info, "failed to stop the metric controller:", err3)
	}
}

func dataGenerator(workersStatesDataChan chan<- WorkersStateData) {
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
			workerStatesData := WorkersStateData{
				ShardId:    int(0),
				WorkerType: int(1),
				InstanceId: int(0),
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
			workersStatesDataChan <- workerStatesData
			timer.Reset(waitTime)
		case <-ctx.Done():
			logger.GetLogger().Log(logger.Info, "Timedout, so context closed")
			break mainloop
		}
	}
}

//Go terenary inplementation
func If[T any](cond bool, vtrue, vfalse T) T {
	if cond {
		return vtrue
	}
	return vfalse
}
