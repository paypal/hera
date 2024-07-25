package otel

import (
	"context"
	"fmt"
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

// Implement apply function in to configure meter provider
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

// WithMetricProvider Create StateLogMetrics with provided meter Provider
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
func StartMetricsCollection(totalWorkersCount int, opt ...StateLogOption) error {
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
		//Initialize state-log metrics
		metricsStateLogger = &StateLogMetrics{
			meter: stateLogMetricsConfig.MeterProvider.Meter(StateLogMeterName,
				metric.WithInstrumentationVersion(OtelInstrumentationVersion)),
			metricsConfig:  stateLogMetricsConfig,
			hostname:       hostName,
			mStateDataChan: make(chan *WorkersStateData, totalWorkersCount*otelconfig.OTelConfigData.ResolutionTimeInSec*2), //currently OTEL polling interval hardcoded as 10. Size of bufferred channel = totalWorkersCount * pollingInterval * 2,
			doneCh:         make(chan struct{}),
		}
		err = metricsStateLogger.register()
	})
	return err
}

// StopMetricCollection Send notification to stateLogMetrics.doneCh to stop metric collection
func StopMetricCollection() {
	select {
	case metricsStateLogger.doneCh <- struct{}{}:
		return
	default:
		logger.GetLogger().Log(logger.Info, "channel has already been closed.")
		return
	}
}

// AddDataPointToOTELStateDataChan Send data to stateLogMetrics.mStateDataChan channel
func AddDataPointToOTELStateDataChan(dataPoint *WorkersStateData) {
	select {
	case metricsStateLogger.mStateDataChan <- dataPoint:
		return
	case <-time.After(time.Millisecond * 100):
		logger.GetLogger().Log(logger.Alert, "timeout occurred while adding record to stats data channel")
	}
}

// Define Instrumentation for each metrics and register with StateLogMetrics
func (stateLogMetrics *StateLogMetrics) register() error {

	//"init", "acpt", "wait", "busy", "schd", "fnsh", "quce", "asgn", "idle", "bklg", "strd", "cls"
	var err error
	if stateLogMetrics.initState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(InitConnGuageMetric),
		metric.WithDescription("Number of workers in init state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for init state", err)
		return err
	}

	if stateLogMetrics.acptState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(AccptConnGuageMetric),
		metric.WithDescription("Number of workers in accept state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for accept state", err)
		return err
	}

	if stateLogMetrics.waitState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(WaitConnGuageMetric),
		metric.WithDescription("Number of workers in wait state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for wait state", err)
		return err
	}

	if stateLogMetrics.busyState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(BusyConnGuageMetric),
		metric.WithDescription("Number of workers in busy state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for busy state", err)
		return err
	}

	if stateLogMetrics.schdState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(ScheduledConnGuageMetric),
		metric.WithDescription("Number of workers in scheduled state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for scheduled state", err)
		return err
	}

	if stateLogMetrics.fnshState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(FinishedConnGuageMetric),
		metric.WithDescription("Number of workers in finished state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for finished state", err)
		return err
	}

	if stateLogMetrics.quceState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(QuiescedConnGuageMetric),
		metric.WithDescription("Number of workers in quiesced state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for quiesced state", err)
		return err
	}

	if stateLogMetrics.asgnState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(AssignedConnGuageMetric),
		metric.WithDescription("Number of workers in assigned state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for assigned state", err)
		return err
	}

	if stateLogMetrics.idleState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(IdleConnGuageMetric),
		metric.WithDescription("Number of workers in idle state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for idle state", err)
		return err
	}

	if stateLogMetrics.bklgState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(BacklogConnGuageMetric),
		metric.WithDescription("Number of workers in backlog state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for backlog state", err)
		return err
	}

	if stateLogMetrics.strdState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(StrdConnGuageMetric),
		metric.WithDescription("Number of connections in stranded state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for stranded state", err)
		return err
	}
	//Initialize max metrics
	if stateLogMetrics.initStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(InitMaxGuageMetric),
		metric.WithDescription("Maximum Number of workers in init state within resolution time"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for init max state", err)
		return err
	}

	if stateLogMetrics.waitStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(WaitMaxGuageMetric),
		metric.WithDescription("Maximum Number of workers in wait state within resolution time"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for max wait state", err)
		return err
	}

	if stateLogMetrics.busyStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(BusyMaxGuageMetric),
		metric.WithDescription("Maximum Number of workers in busy state within resolution time"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for max busy state", err)
		return err
	}

	if stateLogMetrics.schdStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(SchdMaxGuageMetric),
		metric.WithDescription("Maximum Number of workers in scheduled state within resolution time"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for scheduled state", err)
		return err
	}

	if stateLogMetrics.quceStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(QuiescedMaxGuageMetric),
		metric.WithDescription("Maximum Number of workers in quiesced state within resolution time"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for quiesced state", err)
		return err
	}

	if stateLogMetrics.idleStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(IdleMaxGuageMetric),
		metric.WithDescription("Maximum Number of client connections in idle state within resolution time"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for max idle state", err)
		return err
	}

	if stateLogMetrics.bklgStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(BacklogMaxGuageMetric),
		metric.WithDescription("Maximum Number of client connections in backlog state within resolution time"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for max backlog state", err)
		return err
	}

	if stateLogMetrics.strdStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(StrdMaxGuageMetric),
		metric.WithDescription("Maximum Number of client connections in idle state within resolution time"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for max stranded state", err)
		return err
	}

	//Initialize min for accpet
	if stateLogMetrics.acptStateMin, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(AcceptMinGuageMetric),
		metric.WithDescription("Minimum Number of workers in accept state within resolution time"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for min accept state", err)
		return err
	}

	stateLogMetrics.registration, err = stateLogMetrics.meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			return stateLogMetrics.asyncStateLogMetricsPoll(observer)
		},
		[]metric.Observable{
			stateLogMetrics.initState,
			stateLogMetrics.acptState,
			stateLogMetrics.waitState,
			stateLogMetrics.busyState,
			stateLogMetrics.schdState,
			stateLogMetrics.fnshState,
			stateLogMetrics.quceState,
			stateLogMetrics.asgnState,
			stateLogMetrics.idleState,
			stateLogMetrics.bklgState,
			stateLogMetrics.strdState,

			stateLogMetrics.initStateMax, //Max
			stateLogMetrics.waitStateMax,
			stateLogMetrics.busyStateMax,
			stateLogMetrics.schdStateMax,
			stateLogMetrics.quceStateMax,
			stateLogMetrics.idleStateMax,
			stateLogMetrics.bklgStateMax,
			stateLogMetrics.strdStateMax,

			stateLogMetrics.acptStateMin, //Min
		}...)

	if err != nil {
		return err
	}
	return nil
}

/*
 * AasyncStatelogMetricsPoll poll operation involved periodically by OTEL collector based-on its polling interval
 * it poll metrics from channel do aggregation or compute max based combination of shardId + workerType + InstanceId
 */
func (stateLogMetrics *StateLogMetrics) asyncStateLogMetricsPoll(observer metric.Observer) (err error) {
	stateLogMetrics.stateLock.Lock()
	defer stateLogMetrics.stateLock.Unlock()
	stateLogsData := make(map[string]map[string]int64)
	var stateLogTitle string
	//Infinite loop read through the channel and send metrics
mainloop:
	for {
		select {
		case workersState, more := <-stateLogMetrics.mStateDataChan:
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
				if key == "req" || key == "resp" {
					stateLogsData[keyName][key] += value
				} else {
					maxKey := key + "Max"
					minKey := key + "Min"
					stateLogsData[keyName][key] = value
					//check max update max value
					_, keyPresent := stateLogsData[keyName][maxKey]
					if !keyPresent {
						stateLogsData[keyName][maxKey] = value
					}
					if stateLogsData[keyName][maxKey] < value {
						stateLogsData[keyName][maxKey] = value
					}
					//Min value
					_, keyPresent = stateLogsData[keyName][minKey]
					if !keyPresent {
						stateLogsData[keyName][minKey] = value
					}
					if stateLogsData[keyName][minKey] > value {
						stateLogsData[keyName][minKey] = value
					}
				}
			}
		case <-stateLogMetrics.doneCh:
			logger.GetLogger().Log(logger.Info, "received stopped signal for processing statelog metric. "+
				"so unregistering callback for sending data and closing data channel")
			close(stateLogMetrics.mStateDataChan)
			stateLogMetrics.registration.Unregister()
		default:
			break mainloop
		}
	}
	//Process metrics data
	if len(stateLogsData) > 0 {
		err = stateLogMetrics.sendMetricsDataToCollector(observer, &stateLogTitle, stateLogsData)
	}
	return err
}

/*
 *  Send metrics datat data-points to collector
 */
func (stateLogMetrics *StateLogMetrics) sendMetricsDataToCollector(observer metric.Observer, stateLogTitle *string, stateLogsData map[string]map[string]int64) (err error) {
	for key, aggStatesData := range stateLogsData {
		logger.GetLogger().Log(logger.Info, fmt.Sprintf("publishing metric with calculated max value and aggregation of gauge for shardid-workertype-instanceId: %s using datapoints size: %d", key, aggStatesData[Datapoints]))
		commonLabels := []attribute.KeyValue{
			attribute.Int(ShardId, int(aggStatesData[ShardId])),
			attribute.Int(WorkerType, int(aggStatesData[WorkerType])),
			attribute.Int(InstanceId, int(aggStatesData[InstanceId])),
			attribute.String(OccWorkerParamName, *stateLogTitle),
			attribute.String(HostDimensionName, stateLogMetrics.hostname),
		}
		//Observe states data
		//1. Worker States
		observer.ObserveInt64(stateLogMetrics.initState, aggStatesData["init"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.acptState, aggStatesData["acpt"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.waitState, aggStatesData["wait"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.busyState, aggStatesData["busy"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.schdState, aggStatesData["schd"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.fnshState, aggStatesData["fnsh"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.quceState, aggStatesData["quce"], metric.WithAttributes(commonLabels...))

		//2. Connection States
		observer.ObserveInt64(stateLogMetrics.asgnState, aggStatesData["asgn"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.idleState, aggStatesData["idle"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.bklgState, aggStatesData["bklg"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.strdState, aggStatesData["strd"], metric.WithAttributes(commonLabels...))

		//3. Worker States Max values
		observer.ObserveInt64(stateLogMetrics.initStateMax, aggStatesData["initMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.waitStateMax, aggStatesData["waitMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.busyStateMax, aggStatesData["busyMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.schdStateMax, aggStatesData["schdMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.quceStateMax, aggStatesData["quceMax"], metric.WithAttributes(commonLabels...))

		//4. Connection States Max values
		observer.ObserveInt64(stateLogMetrics.idleStateMax, aggStatesData["idleMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.bklgStateMax, aggStatesData["bklgMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.strdStateMax, aggStatesData["strdMax"], metric.WithAttributes(commonLabels...))

		//5. Min accept state
		observer.ObserveInt64(stateLogMetrics.acptStateMin, aggStatesData["acptMin"], metric.WithAttributes(commonLabels...))
	}
	return nil
}
