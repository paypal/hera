package otel

import (
	"context"
	"fmt"
	"sync"

	"github.com/paypal/hera/utility/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/asyncint64"
)

//"init", "acpt", "wait", "busy", "schd", "fnsh", "quce", "asgn", "idle", "bklg", "strd", "cls"}
//Following Metric Names will get instrumented as part of StateLogMetrics

const (
	// Worker States
	INIT_CONN_COUNT_METRIC      = "hera.init_connection.count"
	ACCPT_CONN_COUNT_METRIC     = "hera.accept_connection.count"
	WAIT_CONN_COUNT_METRIC      = "hera.wait_connection.count"
	BUSY_CONN_COUNT_METRIC      = "hera.busy_connection.count"
	SCHEDULED_CONN_COUNT_METRIC = "hera.schd.connection.count"
	FINISHED_CONN_COUNT_METRIC  = "hera.fnsh.connection.count"
	QUIESCED_CONN_COUNT_METRIC  = "hera.quce.connection.count"
	// Connection States
	ASSIGNED_CONN_COUNT_METRIC = "hera.asgn.connection.count"
	IDLE_CONN_COUNT_METRIC     = "hera.idle_connection.count"
	BACKLOG_CONN_COUNT_METRIC  = "hera.backlog_connection.count"
	STRD_CONN_COUNT_METRIC     = "hera.strd_connection.count"

	// Max Worker States
	INIT_CONN_COUNT_METRIC_MAX      = "hera.init_connection.count.max"
	ACCPT_CONN_COUNT_METRIC_MAX     = "hera.accept_connection.count.max"
	WAIT_CONN_COUNT_METRIC_MAX      = "hera.wait_connection.count.max"
	BUSY_CONN_COUNT_METRIC_MAX      = "hera.busy_connection.count.max"
	SCHEDULED_CONN_COUNT_METRIC_MAX = "hera.schd.connection.count.max"
	FINISHED_CONN_COUNT_METRIC_MAX  = "hera.fnsh.connection.count.max"
	QUIESCED_CONN_COUNT_METRIC_MAX  = "hera.quce.connection.count.max"
	// Connection States
	ASSIGNED_CONN_COUNT_METRIC_MAX = "hera.asgn.connection.count.max"
	IDLE_CONN_COUNT_METRIC_MAX     = "hera.idle_connection.count.max"
	BACKLOG_CONN_COUNT_METRIC_MAX  = "hera.backlog_connection.count.max"
	STRD_CONN_COUNT_METRIC_MAX     = "hera.strd_connection.count.max"

	//Worker Request Response Count
	WORKER_REQUEST_COUNT_METRIC  = "hera.worker.req.count"
	WORKER_RESPONSE_COUNT_METRIC = "hera.worker.resp.count"
)

// Object represents the workers states data for worker belongs to specific shardId and workperType with flat-map
// between statename vs count.
type WorkersStateData struct {
	ShardId    int
	WorkerType int
	InstanceId int
	StateData  map[string]int64
}

// state_log_metrics reports workers states
type StateLogMetrics struct {

	//Statelog metrics configuration data
	metricsConfig stateLogMetricsConfig

	meter metric.Meter

	//Channel to receive statelog data
	mStateDataChan <-chan WorkersStateData

	//This lock prevents a race between batch observer and instrument registration
	lock sync.Mutex

	initState asyncint64.Gauge
	acptState asyncint64.Gauge
	waitState asyncint64.Gauge
	busyState asyncint64.Gauge
	schdState asyncint64.Gauge
	fnshState asyncint64.Gauge
	quceState asyncint64.Gauge
	asgnState asyncint64.Gauge
	idleState asyncint64.Gauge
	bklgState asyncint64.Gauge
	strdState asyncint64.Gauge

	initStateMax asyncint64.Gauge
	acptStateMax asyncint64.Gauge
	waitStateMax asyncint64.Gauge
	busyStateMax asyncint64.Gauge
	schdStateMax asyncint64.Gauge
	fnshStateMax asyncint64.Gauge
	quceStateMax asyncint64.Gauge
	asgnStateMax asyncint64.Gauge
	idleStateMax asyncint64.Gauge
	bklgStateMax asyncint64.Gauge
	strdStateMax asyncint64.Gauge

	workerReqCount  asyncint64.UpDownCounter
	workerRespCount asyncint64.UpDownCounter
}

type stateLogMetricsConfig struct {

	// MeterProvider sets the metric.MeterProvider.  If nil, the global
	// Provider will be used.
	MeterProvider metric.MeterProvider
	OCCName       string
}

//Interface define configuration parameters for statelog metrics agent
type Option interface {
	apply(*stateLogMetricsConfig)
}

//Define confuration for metric Provider Option
type MetricProviderOption struct {
	metric.MeterProvider
}

//Implement apply function in to configure meter provider
func (o MetricProviderOption) apply(c *stateLogMetricsConfig) {
	if o.MeterProvider != nil {
		c.MeterProvider = o.MeterProvider
	}
}

//Define Option for OCCName
type OCCNameOption string

const defaultOCCName string = "occ"

func (occName OCCNameOption) apply(c *stateLogMetricsConfig) {
	if occName != "" {
		c.OCCName = string(occName)
	}
}

//Create StateLogMetrics with OCC Name
func WithOCCName(occName string) Option {
	return OCCNameOption(occName)
}

//Create StateLogMetrics with provided meter Provider
func WitthMetricProvider(provider metric.MeterProvider) Option {
	return MetricProviderOption{provider}
}

// newConfig computes a config from the supplied Options.
func newConfig(opts ...Option) stateLogMetricsConfig {
	statesConfig := stateLogMetricsConfig{
		MeterProvider: global.MeterProvider(),
		OCCName:       defaultOCCName,
	}

	for _, opt := range opts {
		opt.apply(&statesConfig)
	}
	return statesConfig
}

// Start initializes reporting of stateLogMetrics using the supplied config.
func StartMetricsCollection(stateLogDataChan <-chan WorkersStateData, opt ...Option) error {
	stateLogMetricsConfig := newConfig(opt...)

	//Verfication of config data
	if stateLogMetricsConfig.OCCName == "" {
		stateLogMetricsConfig.OCCName = defaultOCCName
	}

	if stateLogMetricsConfig.MeterProvider == nil {
		stateLogMetricsConfig.MeterProvider = global.MeterProvider()
	}

	//Initialize statelog mterics
	stateLogMetrics := &StateLogMetrics{
		meter: stateLogMetricsConfig.MeterProvider.Meter("occ-statelog-data",
			metric.WithInstrumentationVersion("v1.0")),
		metricsConfig:  stateLogMetricsConfig,
		mStateDataChan: stateLogDataChan,
	}

	//Registers instrumentation for metrics
	return stateLogMetrics.register()
}

// Define Instrumentation for each metrics and register with StateLogMetrics
func (stateLogMetrics *StateLogMetrics) register() (err error) {

	//"init", "acpt", "wait", "busy", "schd", "fnsh", "quce", "asgn", "idle", "bklg", "strd", "cls"}
	stateLogMetrics.lock.Lock()
	defer stateLogMetrics.lock.Unlock()

	if stateLogMetrics.initState, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(INIT_CONN_COUNT_METRIC),
		instrument.WithDescription("Number of workers in init state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for init state", err)
		return err
	}

	if stateLogMetrics.acptState, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(ACCPT_CONN_COUNT_METRIC),
		instrument.WithDescription("Number of workers in accept state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for accept state", err)
		return err
	}

	if stateLogMetrics.waitState, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(WAIT_CONN_COUNT_METRIC),
		instrument.WithDescription("Number of workers in wait state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for wait state", err)
		return err
	}

	if stateLogMetrics.busyState, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(BUSY_CONN_COUNT_METRIC),
		instrument.WithDescription("Number of workers in busy state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for busy state", err)
		return err
	}

	if stateLogMetrics.schdState, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(SCHEDULED_CONN_COUNT_METRIC),
		instrument.WithDescription("Number of workers in scheduled state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for scheduled state", err)
		return err
	}

	if stateLogMetrics.fnshState, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(FINISHED_CONN_COUNT_METRIC),
		instrument.WithDescription("Number of workers in finished state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for finished state", err)
		return err
	}

	if stateLogMetrics.quceState, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(QUIESCED_CONN_COUNT_METRIC),
		instrument.WithDescription("Number of workers in quiesced state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for quiesced state", err)
		return err
	}

	if stateLogMetrics.asgnState, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(ASSIGNED_CONN_COUNT_METRIC),
		instrument.WithDescription("Number of workers in assigned state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for assigned state", err)
		return err
	}

	if stateLogMetrics.idleState, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(IDLE_CONN_COUNT_METRIC),
		instrument.WithDescription("Number of workers in idle state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for idle state", err)
		return err
	}

	if stateLogMetrics.bklgState, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(BACKLOG_CONN_COUNT_METRIC),
		instrument.WithDescription("Number of workers in backlog state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for backlog state", err)
		return err
	}

	if stateLogMetrics.strdState, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(STRD_CONN_COUNT_METRIC),
		instrument.WithDescription("Number of connections in stranded state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for stranded state", err)
		return err
	}

	if stateLogMetrics.initStateMax, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(INIT_CONN_COUNT_METRIC_MAX),
		instrument.WithDescription("Number of workers in init state max count value"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for init state max count", err)
		return err
	}

	if stateLogMetrics.acptStateMax, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(ACCPT_CONN_COUNT_METRIC_MAX),
		instrument.WithDescription("Number of workers in accept state count max"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for accept state max count", err)
		return err
	}

	if stateLogMetrics.waitStateMax, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(WAIT_CONN_COUNT_METRIC_MAX),
		instrument.WithDescription("Number of workers in wait state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for wait state max count", err)
		return err
	}

	if stateLogMetrics.busyStateMax, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(BUSY_CONN_COUNT_METRIC_MAX),
		instrument.WithDescription("Number of workers in busy state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for busy state max count", err)
		return err
	}

	if stateLogMetrics.schdStateMax, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(SCHEDULED_CONN_COUNT_METRIC_MAX),
		instrument.WithDescription("Number of workers in scheduled state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for scheduled state max count", err)
		return err
	}

	if stateLogMetrics.fnshStateMax, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(FINISHED_CONN_COUNT_METRIC_MAX),
		instrument.WithDescription("Number of workers in finished state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for finished state max count", err)
		return err
	}

	if stateLogMetrics.quceStateMax, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(QUIESCED_CONN_COUNT_METRIC_MAX),
		instrument.WithDescription("Number of workers in quiesced state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for quiesced state max count", err)
		return err
	}

	if stateLogMetrics.asgnStateMax, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(ASSIGNED_CONN_COUNT_METRIC_MAX),
		instrument.WithDescription("Number of workers in assigned state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for assigned state max count", err)
		return err
	}

	if stateLogMetrics.idleStateMax, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(IDLE_CONN_COUNT_METRIC_MAX),
		instrument.WithDescription("Number of workers in idle state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for idle state max count", err)
		return err
	}

	if stateLogMetrics.bklgStateMax, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(BACKLOG_CONN_COUNT_METRIC_MAX),
		instrument.WithDescription("Number of workers in backlog state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for backlog state max count", err)
		return err
	}

	if stateLogMetrics.strdStateMax, err = stateLogMetrics.meter.AsyncInt64().Gauge(
		populateMetricNamePrefix(STRD_CONN_COUNT_METRIC_MAX),
		instrument.WithDescription("Number of connections in stranded state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for stranded state max count", err)
		return err
	}

	if stateLogMetrics.workerReqCount, err = stateLogMetrics.meter.AsyncInt64().UpDownCounter(
		populateMetricNamePrefix(WORKER_REQUEST_COUNT_METRIC),
		instrument.WithDescription("Number requests handled by worker between pooling period"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register updown counter metric for request handled by worker", err)
		return err
	}

	if stateLogMetrics.workerRespCount, err = stateLogMetrics.meter.AsyncInt64().UpDownCounter(
		populateMetricNamePrefix(WORKER_RESPONSE_COUNT_METRIC),
		instrument.WithDescription("Number responses served by worker between pooling period"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register updown counter metric for responses served by worker", err)
		return err
	}

	err = stateLogMetrics.meter.RegisterCallback(
		[]instrument.Asynchronous{
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
			stateLogMetrics.initStateMax,
			stateLogMetrics.acptStateMax,
			stateLogMetrics.waitStateMax,
			stateLogMetrics.busyStateMax,
			stateLogMetrics.schdStateMax,
			stateLogMetrics.fnshStateMax,
			stateLogMetrics.quceStateMax,
			stateLogMetrics.asgnStateMax,
			stateLogMetrics.idleStateMax,
			stateLogMetrics.bklgStateMax,
			stateLogMetrics.strdStateMax,
			stateLogMetrics.workerReqCount,
			stateLogMetrics.workerRespCount,
		}, func(ctx context.Context) {
			stateLogMetrics.asyncStatelogMetricsPoll(ctx)
		})

	if err != nil {
		return err
	}
	return nil
}

/*
 * Async statelog poll operation involved periodically by OTEL collector based-on its polling interval
 * it poll metrics from channel do aggregation or compute max based combination of shardId + workerType + InstanceId
 */
func (stateLogMetrics *StateLogMetrics) asyncStatelogMetricsPoll(ctx context.Context) (err error) {
	stateLogMetrics.lock.Lock()
	defer stateLogMetrics.lock.Unlock()

	stateLogsData := make(map[string]map[string]int64)
	//Infinite loop read through the channel and send metrics
mainloop:
	for {
		select {
		case workersState, more := <-stateLogMetrics.mStateDataChan:
			if !more { // TODO:: check zero value for workersState
				logger.GetLogger().Log(logger.Info, "Statelog metrics data channel 'mStateDataChan' has been closed.")
				break mainloop
			}
			keyName := fmt.Sprintf("%d-%d-%d", workersState.ShardId, workersState.WorkerType, workersState.InstanceId)

			if stateLogsData[keyName] == nil {
				stateLogsData[keyName] = make(map[string]int64)
			}
			//Update metadata information
			stateLogsData[keyName]["shardId"] = int64(workersState.ShardId)
			stateLogsData[keyName]["WorkerType"] = int64(workersState.WorkerType)
			stateLogsData[keyName]["InstanceId"] = int64(workersState.InstanceId)
			stateLogsData[keyName]["datapoints"] += 1

			for key, value := range workersState.StateData {
				if key == "req" || key == "resp" {
					stateLogsData[keyName][key] += value
				} else {
					maxKey := key + "Max"
					stateLogsData[keyName][key] = value
					//check max update max value
					if stateLogsData[keyName][maxKey] < value {
						stateLogsData[keyName][maxKey] = value
					}
				}

			}
		default:
			break mainloop
		}
	}
	//Process metrics data
	err = stateLogMetrics.sendMetricsDataToCollector(ctx, stateLogsData)
	return err
}

/*
 *  Send metrics datat data-points to collector
 */
func (stateLogMetrics *StateLogMetrics) sendMetricsDataToCollector(ctx context.Context, stateLogsData map[string]map[string]int64) (err error) {
	for key, aggStatesData := range stateLogsData {
		logger.GetLogger().Log(logger.Info, fmt.Sprintf("calculated max value and aggregation of updown counter for key: %s using datapoints size: %d", key, aggStatesData["datapoints"]))
		commonLabels := []attribute.KeyValue{
			attribute.String("Application", stateLogMetrics.metricsConfig.OCCName),
			attribute.Int("ShardId", int(aggStatesData["ShardId"])),
			attribute.Int("HeraWorkerType", int(aggStatesData["WorkerType"])),
			attribute.Int("InstanceId", int(aggStatesData["InstanceId"])),
		}

		//Observe states data
		// 1. Worker States
		stateLogMetrics.initState.Observe(ctx, aggStatesData["init"], commonLabels...)
		stateLogMetrics.acptState.Observe(ctx, aggStatesData["acpt"], commonLabels...)
		stateLogMetrics.waitState.Observe(ctx, aggStatesData["wait"], commonLabels...)
		stateLogMetrics.busyState.Observe(ctx, aggStatesData["schd"], commonLabels...)
		stateLogMetrics.fnshState.Observe(ctx, aggStatesData["fnsh"], commonLabels...)
		stateLogMetrics.quceState.Observe(ctx, aggStatesData["quce"], commonLabels...)

		// 2. Connection States
		stateLogMetrics.asgnState.Observe(ctx, aggStatesData["asgn"], commonLabels...)
		stateLogMetrics.idleState.Observe(ctx, aggStatesData["idle"], commonLabels...)
		stateLogMetrics.bklgState.Observe(ctx, aggStatesData["bklg"], commonLabels...)
		stateLogMetrics.strdState.Observe(ctx, aggStatesData["strd"], commonLabels...)

		//3. Max Worker States
		stateLogMetrics.initStateMax.Observe(ctx, aggStatesData["initMax"], commonLabels...)
		stateLogMetrics.acptStateMax.Observe(ctx, aggStatesData["acptMax"], commonLabels...)
		stateLogMetrics.waitStateMax.Observe(ctx, aggStatesData["waitMax"], commonLabels...)
		stateLogMetrics.busyStateMax.Observe(ctx, aggStatesData["schdMax"], commonLabels...)
		stateLogMetrics.fnshStateMax.Observe(ctx, aggStatesData["fnshMax"], commonLabels...)
		stateLogMetrics.quceStateMax.Observe(ctx, aggStatesData["quceMax"], commonLabels...)

		// 4. Max Connection States
		stateLogMetrics.asgnStateMax.Observe(ctx, aggStatesData["asgnMax"], commonLabels...)
		stateLogMetrics.idleStateMax.Observe(ctx, aggStatesData["idleMax"], commonLabels...)
		stateLogMetrics.bklgStateMax.Observe(ctx, aggStatesData["bklgMax"], commonLabels...)
		stateLogMetrics.strdStateMax.Observe(ctx, aggStatesData["strdMax"], commonLabels...)

		//Workers stats
		stateLogMetrics.workerReqCount.Observe(ctx, aggStatesData["req"], commonLabels...)
		stateLogMetrics.workerRespCount.Observe(ctx, aggStatesData["resp"], commonLabels...)
	}
	return nil
}
