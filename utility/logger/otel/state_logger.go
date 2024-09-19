package otel

import (
	"context"
	"fmt"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
	otelconfig "github.com/paypal/hera/utility/logger/otel/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"os"
	"sync"
	"time"
)

const defaultAppName string = "occ"

// This lock prevents a race between batch observer and instrument registration
var registerStateMetrics sync.Once
var metricsStateLogger *StateLogMetrics
var totalConnectionStateDataLogger *TotalConnectionsGaugeData

// Implement apply function in to configure stateLogMeter provider
func (o MetricProviderOption) apply(c *stateLogMetricsConfig) {
	if o.MeterProvider != nil {
		c.MeterProvider = o.MeterProvider
	}
}

// Implement apply function in to configure pool name
func (appName AppNameOption) apply(c *stateLogMetricsConfig) {
	if appName != "" {
		c.appName = string(appName)
	}
}

// WithAppName Create StateLogMetrics with OCC Name
func WithAppName(appName string) StateLogOption {
	return AppNameOption(appName)
}

// WithMetricProvider Create StateLogMetrics with provided stateLogMeter Provider
func WithMetricProvider(provider metric.MeterProvider) StateLogOption {
	return MetricProviderOption{provider}
}

// newConfig computes a config from the supplied Options.
func newConfig(opts ...StateLogOption) stateLogMetricsConfig {
	statesConfig := stateLogMetricsConfig{
		MeterProvider: otel.GetMeterProvider(),
		appName:       defaultAppName,
	}

	for _, opt := range opts {
		opt.apply(&statesConfig)
	}
	return statesConfig
}

// StartMetricsCollection initializes reporting of stateLogMetrics using the supplied config.
func StartMetricsCollection(ctx context.Context, totalWorkersCount int, opt ...StateLogOption) error {
	stateLogMetricsConfig := newConfig(opt...)

	//Verification of config data
	if stateLogMetricsConfig.appName == "" {
		stateLogMetricsConfig.appName = defaultAppName
	}

	if stateLogMetricsConfig.MeterProvider == nil {
		stateLogMetricsConfig.MeterProvider = otel.GetMeterProvider()
	}

	var err error
	//Registers instrumentation for metrics
	registerStateMetrics.Do(func() {
		hostName, hostErr := os.Hostname()
		if hostErr != nil {
			logger.GetLogger().Log(logger.Alert, "Failed to fetch hostname for current container", err)
		}
		stateLogMeter := stateLogMetricsConfig.MeterProvider.Meter(StateLogMeterName,
			metric.WithInstrumentationVersion(OtelInstrumentationVersion))
		//Initialize state-log metrics
		metricsStateLogger = &StateLogMetrics{
			stateLogMeter:  stateLogMeter,
			hostname:       hostName,
			mStateDataChan: make(chan *WorkersStateData, totalWorkersCount*otelconfig.OTelConfigData.ResolutionTimeInSec*5), //currently OTEL polling interval hardcoded as 10. Size of bufferred channel = totalWorkersCount * pollingInterval * 2,
			doneCh:         make(chan struct{}),
		}

		totalConnectionStateDataLogger = &TotalConnectionsGaugeData{
			stateLogMeter:        stateLogMeter,
			hostname:             hostName,
			totalConnDataChannel: make(chan *GaugeMetricData, totalWorkersCount*otelconfig.OTelConfigData.ResolutionTimeInSec*20), //currently OTEL polling interval hardcoded as 10. Size of bufferred channel = totalWorkersCount * pollingInterval * 2,
			stopPublish:          make(chan struct{}),
		}
		err = registerMetrics(metricsStateLogger, totalConnectionStateDataLogger)
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "Failed to register state metrics collector", err)
		} else {
			err = totalConnectionStateDataLogger.registerCallbackForTotalConnectionsData()
			if err != nil {
				logger.GetLogger().Log(logger.Alert, "Failed to register callback for totalConnectionStateDataLogger gauge metric", err)
			}
			if err == nil {
				go metricsStateLogger.startStateLogMetricsPoll(ctx) //Goroutine to poll HERA states data
			}
		}
	})
	return err
}

// StopMetricCollection Send notification to stateLogMetrics.stopPublish to stop metric collection
func StopMetricCollection() {
	var wg sync.WaitGroup
	wg.Add(2)
	//Goroutine 1
	go func() {
		defer wg.Done()
		select {
		case metricsStateLogger.doneCh <- struct{}{}:
			logger.GetLogger().Log(logger.Info, "this stop metric collection for state-log data on channel metricsStateLogger.")
			return
		default:
			logger.GetLogger().Log(logger.Info, "metricsStateLogger done channel has already been closed.")
			return
		}
	}()

	go func() {
		defer wg.Done()
		select {
		case totalConnectionStateDataLogger.stopPublish <- struct{}{}:
			logger.GetLogger().Log(logger.Info, "this stop metric collection for state-log data on channel totalConnectionStateDataLogger.")
			return
		default:
			logger.GetLogger().Log(logger.Info, "totalConnectionStateDataLogger.stopPublish channel has already been closed.")
			return
		}
	}()
	wg.Wait()
}

// AddDataPointToOTELStateDataChan Send data to stateLogMetrics.mStateDataChan channel
func AddDataPointToOTELStateDataChan(dataPoint *WorkersStateData) {
	defer func() {
		if r := recover(); r != nil {
			logger.GetLogger().Log(logger.Info, "Panic while adding data-points to StateDataChannel, Recovered from panic: ", r)
		}
	}()
	select {
	case metricsStateLogger.mStateDataChan <- dataPoint:
		return
	case <-time.After(time.Second * 1):
		logger.GetLogger().Log(logger.Info, "timeout occurred while adding record to stats data channel")
		event := cal.NewCalEvent("OTEL", "DATA_TIMEOUT", "1", "timeout occurred while adding record to mStateDataChan channel")
		event.AddDataInt("loggedTime", time.Now().Unix())
		event.Completed()
		return
	}
}

// AddDataPointToTotalConnectionsDataChannel Send data to totalConnectionStateDataLogger.totalConnDataChannel channel
func AddDataPointToTotalConnectionsDataChannel(totalConnectionData *GaugeMetricData) {
	defer func() {
		if r := recover(); r != nil {
			logger.GetLogger().Log(logger.Info, "Panic while adding data-points to totalConnDataChannel, Recovered from panic: ", r)
		}
	}()
	select {
	case totalConnectionStateDataLogger.totalConnDataChannel <- totalConnectionData:
		return
	case <-time.After(time.Second * 1):
		logger.GetLogger().Log(logger.Info, "timeout occurred while adding guage data record to totalConnDataChannel channel")
		event := cal.NewCalEvent("OTEL", "DATA_TIMEOUT", "1", "timeout occurred while adding guage data record to totalConnDataChannel channel")
		event.AddDataInt("loggedTime", time.Now().Unix())
		event.Completed()
		return
	}
}

// Define Instrumentation for each metrics and register with StateLogMetrics
func registerMetrics(stateLogMetrics *StateLogMetrics, totalConnectionsMetrics *TotalConnectionsGaugeData) error {

	//"init", "acpt", "wait", "busy", "schd", "fnsh", "quce", "asgn", "idle", "bklg", "strd", "cls"
	var err error
	if stateLogMetrics.initState, err = stateLogMetrics.stateLogMeter.Int64Histogram(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(InitConnMetric),
		metric.WithDescription("Number of workers in init state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for init state", err)
		return err
	}

	if stateLogMetrics.acptState, err = stateLogMetrics.stateLogMeter.Int64Histogram(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(AccptConnMetric),
		metric.WithDescription("Number of workers in accept state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for accept state", err)
		return err
	}

	if stateLogMetrics.waitState, err = stateLogMetrics.stateLogMeter.Int64Histogram(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(WaitConnMetric),
		metric.WithDescription("Number of workers in wait state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for wait state", err)
		return err
	}

	if stateLogMetrics.busyState, err = stateLogMetrics.stateLogMeter.Int64Histogram(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(BusyConnMetric),
		metric.WithDescription("Number of workers in busy state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for busy state", err)
		return err
	}

	if stateLogMetrics.schdState, err = stateLogMetrics.stateLogMeter.Int64Histogram(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(ScheduledConnMetric),
		metric.WithDescription("Number of workers in scheduled state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for scheduled state", err)
		return err
	}

	if stateLogMetrics.fnshState, err = stateLogMetrics.stateLogMeter.Int64Histogram(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(FinishedConnMetric),
		metric.WithDescription("Number of workers in finished state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for finished state", err)
		return err
	}

	if stateLogMetrics.quceState, err = stateLogMetrics.stateLogMeter.Int64Histogram(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(QuiescedConnMetric),
		metric.WithDescription("Number of workers in quiesced state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for quiesced state", err)
		return err
	}

	if stateLogMetrics.asgnState, err = stateLogMetrics.stateLogMeter.Int64Histogram(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(AssignedConnMetric),
		metric.WithDescription("Number of workers in assigned state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for assigned state", err)
		return err
	}

	if stateLogMetrics.idleState, err = stateLogMetrics.stateLogMeter.Int64Histogram(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(IdleConnMetric),
		metric.WithDescription("Number of workers in idle state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for idle state", err)
		return err
	}

	if stateLogMetrics.bklgState, err = stateLogMetrics.stateLogMeter.Int64Histogram(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(BacklogConnMetric),
		metric.WithDescription("Number of workers in backlog state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for backlog state", err)
		return err
	}

	if stateLogMetrics.strdState, err = stateLogMetrics.stateLogMeter.Int64Histogram(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(StrdConnMetric),
		metric.WithDescription("Number of connections in stranded state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for stranded state", err)
		return err
	}

	if stateLogMetrics.freePercentage, err = stateLogMetrics.stateLogMeter.Float64Histogram(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(freePercentage),
		metric.WithDescription("Free connections percentage"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for free connections percentage state", err)
		return err
	}

	//Register Gauge metric
	if totalConnectionsMetrics.totalConnections, err = totalConnectionsMetrics.stateLogMeter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(totalConnections),
		metric.WithDescription("Total Connection"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register total connection guage metric", err)
		return err
	}
	if err != nil {
		return err
	}
	return nil
}

/*
 * asyncStateLogMetricsPoll poll operation involved periodically by OTEL collector based-on its polling interval
 * it poll metrics from channel do aggregation or compute max based combination of shardId + workerType + InstanceId
 */
func (stateLogMetrics *StateLogMetrics) startStateLogMetricsPoll(ctx context.Context) {
mainloop:
	for {
		select {
		case workersState, more := <-stateLogMetrics.mStateDataChan:
			stateLogsData := make(map[string]map[string]int64)
			var stateLogTitle string
			if !more {
				logger.GetLogger().Log(logger.Info, "Statelog metrics data channel 'mStateDataChan' has been closed.")
				break mainloop
			}
			keyName := fmt.Sprintf("%d-%d-%d", workersState.ShardId, workersState.WorkerType, workersState.InstanceId)

			if stateLogsData[keyName] == nil {
				stateLogsData[keyName] = make(map[string]int64)
			}
			//Update metadata information
			stateLogTitle = workersState.StateTitle
			stateLogsData[keyName][ShardId] = int64(workersState.ShardId)
			stateLogsData[keyName][WorkerType] = int64(workersState.WorkerType)
			stateLogsData[keyName][InstanceId] = int64(workersState.InstanceId)
			stateLogsData[keyName][Datapoints] += 1
			for key, value := range workersState.StateData {
				stateLogsData[keyName][key] = value
			}
			if len(stateLogsData) > 0 {
				stateLogMetrics.sendMetricsDataToCollector(ctx, &stateLogTitle, stateLogsData)
			}
		case <-stateLogMetrics.doneCh:
			logger.GetLogger().Log(logger.Alert, "received stopped signal for processing statelog metric. "+
				"so stop sending data and closing data channel")
			close(stateLogMetrics.mStateDataChan)
			break mainloop
		}
	}
}

/*
 *  Send metrics datat data-points to collector
 */
func (stateLogMetrics *StateLogMetrics) sendMetricsDataToCollector(ctx context.Context, stateLogTitle *string, stateLogsData map[string]map[string]int64) {
	for key, aggStatesData := range stateLogsData {
		logger.GetLogger().Log(logger.Info, fmt.Sprintf("publishing state logs histogram data for shardid-workertype-instanceId: %s using datapoints size: %d", key, aggStatesData[Datapoints]))
		commonLabels := []attribute.KeyValue{
			attribute.Int(ShardId, int(aggStatesData[ShardId])),
			attribute.String(WorkerType, WorkerTypeMap[int(aggStatesData[WorkerType])]),
			attribute.Int(InstanceId, int(aggStatesData[InstanceId])),
			attribute.String(OccWorkerParamName, *stateLogTitle),
			attribute.String(HostDimensionName, stateLogMetrics.hostname),
		}
		//Observe states data
		//1. Worker States
		stateLogMetrics.initState.Record(ctx, aggStatesData["init"], metric.WithAttributes(commonLabels...))
		stateLogMetrics.acptState.Record(ctx, aggStatesData["acpt"], metric.WithAttributes(commonLabels...))
		stateLogMetrics.waitState.Record(ctx, aggStatesData["wait"], metric.WithAttributes(commonLabels...))
		stateLogMetrics.busyState.Record(ctx, aggStatesData["busy"], metric.WithAttributes(commonLabels...))
		stateLogMetrics.schdState.Record(ctx, aggStatesData["schd"], metric.WithAttributes(commonLabels...))
		stateLogMetrics.fnshState.Record(ctx, aggStatesData["fnsh"], metric.WithAttributes(commonLabels...))
		stateLogMetrics.quceState.Record(ctx, aggStatesData["quce"], metric.WithAttributes(commonLabels...))

		//2. Free Percentage
		freePercentageVal := (float64(aggStatesData["acpt"]+aggStatesData["fnsh"]) / float64(aggStatesData["totalConnections"])) * 100
		stateLogMetrics.freePercentage.Record(ctx, freePercentageVal, metric.WithAttributes(commonLabels...))

		//2. Connection States
		stateLogMetrics.asgnState.Record(ctx, aggStatesData["asgn"], metric.WithAttributes(commonLabels...))
		stateLogMetrics.idleState.Record(ctx, aggStatesData["idle"], metric.WithAttributes(commonLabels...))
		stateLogMetrics.bklgState.Record(ctx, aggStatesData["bklg"], metric.WithAttributes(commonLabels...))
		stateLogMetrics.strdState.Record(ctx, aggStatesData["strd"], metric.WithAttributes(commonLabels...))
	}
}

// This registerCallbackForTotalConnectionsData register callback function to pull totalConnection data for each worker type
func (totalConnectionGauge *TotalConnectionsGaugeData) registerCallbackForTotalConnectionsData() error {
	var regError error
	totalConnectionGauge.registration, regError = totalConnectionGauge.stateLogMeter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			finalDataMap := make(map[string]*GaugeMetricData)
		totalConLoop:
			for {
				select {
				case totalConnData, dataPresent := <-totalConnectionGauge.totalConnDataChannel:
					if !dataPresent {
						logger.GetLogger().Log(logger.Info, "totalConnection gauge data channel 'totalConnDataChannel' has been closed.")
					} else {
						keyName := fmt.Sprintf("%d-%d-%d", totalConnData.ShardId, totalConnData.WorkerType, totalConnData.InstanceId)
						finalDataMap[keyName] = totalConnData
					}
					break totalConLoop
				case <-totalConnectionGauge.stopPublish:
					logger.GetLogger().Log(logger.Alert, "received stopped signal for processing statelog total workers metric. "+
						"so stop sending data to totalConnectionGauge.totalConnDataChannel and closing data channel")
					close(totalConnectionGauge.totalConnDataChannel)
					if totalConnectionGauge.registration != nil {
						logger.GetLogger().Log(logger.Info, "received stopped signal for processing statelog total worker metric. "+
							"so unregister callback function")
						go totalConnectionGauge.registration.Unregister()
					}
					break totalConLoop
				case <-ctx.Done():
					logger.GetLogger().Log(logger.Alert, "parent context has been canceled")
					break totalConLoop
				}
			}
			if len(finalDataMap) > 0 {
				for key, dataPoint := range finalDataMap {
					logger.GetLogger().Log(logger.Debug, fmt.Sprintf("publishing total connection gauge for key: %s, worker type: %s with datapoints value: %d", key, dataPoint.StateTitle, dataPoint.StateData))
					commonLabels := []attribute.KeyValue{
						attribute.Int(ShardId, dataPoint.ShardId),
						attribute.Int(WorkerType, dataPoint.WorkerType),
						attribute.Int(InstanceId, dataPoint.InstanceId),
						attribute.String(OccWorkerParamName, dataPoint.StateTitle),
					}
					observer.ObserveInt64(totalConnectionGauge.totalConnections, dataPoint.StateData, metric.WithAttributes(commonLabels...))
				}
			}
			return nil
		}, totalConnectionGauge.totalConnections)
	if regError != nil {
		logger.GetLogger().Log(logger.Alert, fmt.Sprintf("Failed to register total connection gauge for total worker metric. error %v", regError))
		return regError
	}
	return nil
}
