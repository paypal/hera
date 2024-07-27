package otel

import (
	"go.opentelemetry.io/otel/metric"
	"sync"
)

// "init", "acpt", "wait", "busy", "schd", "fnsh", "quce", "asgn", "idle", "bklg", "strd", "cls"
// Following Metric Names will get instrumented as part of StateLogMetrics
const (
	// Worker States
	InitConnGuageMetric      = "init_connection.cnt"
	AccptConnGuageMetric     = "accept_connection.cnt"
	WaitConnGuageMetric      = "wait_connection.cnt"
	BusyConnGuageMetric      = "busy_connection.cnt"
	ScheduledConnGuageMetric = "scheduled_connection.cnt"
	FinishedConnGuageMetric  = "finished_connection.cnt"
	QuiescedConnGuageMetric  = "quiesced_connection.cnt"

	// Connection States
	AssignedConnGuageMetric = "assigned_connection.cnt"
	IdleConnGuageMetric     = "idle_connection.cnt"
	BacklogConnGuageMetric  = "backlog_connection.cnt"
	StrdConnGuageMetric     = "stranded_connection.cnt"

	InitMaxGuageMetric     = "init_connection.cnt.max"
	AcceptMinGuageMetric   = "accept_connection.cnt.min"
	WaitMaxGuageMetric     = "wait_connection.cnt.max"
	BusyMaxGuageMetric     = "busy_connection.cnt.max"
	SchdMaxGuageMetric     = "scheduled_connection.cnt.max"
	QuiescedMaxGuageMetric = "quiesced_connection.cnt.max"

	IdleMaxGuageMetric    = "idle_connection.cnt.max"
	BacklogMaxGuageMetric = "backlog_connection.cnt.max"
	StrdMaxGuageMetric    = "stranded_connection.cnt.max"
)

const (
	Target               = string("target")
	Endpoint             = string("target_ip_port")
	TLS_version          = string("tls_version")
	ApplicationDimName   = string("application")
	ShardId              = string("ShardId")
	WorkerType           = string("WorkerType")
	InstanceId           = string("InstanceId")
	Datapoints           = string("datapoints")
	OtelSourceName       = string("source")
	otelSource           = string("otel")
	OccWorkerParamName   = string("occ_worker")
	HostDimensionName    = string("host")
	ContainerHostDimName = string("container_host")
)

const OtelInstrumentationVersion string = "v1.0"

// DEFAULT_OTEL_COLLECTOR_PROTOCOL default OTEL configurations point to QA collector
const DEFAULT_OTEL_COLLECTOR_PROTOCOL string = "grpc"
const DEFAULT_OTEL_COLLECTOR__IP string = "0.0.0.0"
const DEFAULT_GRPC_OTEL_COLLECTOR_PORT string = "4317"
const DEFAULT_HTTP_OTEL_COLLECTOR_PORT string = "4318"
const COLLECTOR_POLLING_INTERVAL_SECONDS int32 = 5

const StateLogMeterName = "occ-statelog-data"

// LoggingOTELPublishingInterval This controls how frequently log OTEL publishing error
const LoggingOTELPublishingInterval = 15

//****************************** variables ***************************

type Tags struct {
	TagName  string
	TagValue string
}

type WorkersStateData struct {
	StateTitle string
	ShardId    int
	WorkerType int
	InstanceId int
	StateData  map[string]int64
}

type (
	ServerType int
)

// StateData Represents stats by a worker
type StateData struct {
	Name       string
	Value      float64
	Dimensions metric.MeasurementOption
}

type DataPoint struct {
	attr metric.MeasurementOption
	data int64
}

// StateLogMetrics state_log_metrics reports workers states
type StateLogMetrics struct {

	//Statelog metrics configuration data
	metricsConfig stateLogMetricsConfig

	hostname string

	meter metric.Meter

	//Channel to receive statelog data
	mStateDataChan chan *WorkersStateData

	//Channel to close sending data
	doneCh chan struct{}

	stateLock sync.Mutex

	registration metric.Registration

	initState metric.Int64ObservableGauge
	acptState metric.Int64ObservableGauge
	waitState metric.Int64ObservableGauge
	busyState metric.Int64ObservableGauge
	schdState metric.Int64ObservableGauge
	fnshState metric.Int64ObservableGauge
	quceState metric.Int64ObservableGauge
	asgnState metric.Int64ObservableGauge
	idleState metric.Int64ObservableGauge
	bklgState metric.Int64ObservableGauge
	strdState metric.Int64ObservableGauge

	initStateMax metric.Int64ObservableGauge
	waitStateMax metric.Int64ObservableGauge
	busyStateMax metric.Int64ObservableGauge
	schdStateMax metric.Int64ObservableGauge
	quceStateMax metric.Int64ObservableGauge

	idleStateMax metric.Int64ObservableGauge
	bklgStateMax metric.Int64ObservableGauge
	strdStateMax metric.Int64ObservableGauge

	acptStateMin metric.Int64ObservableGauge
}

// Object represents the workers states data for worker belongs to specific shardId and workperType with flat-map
// between statename vs count.
type stateLogMetricsConfig struct {
	// MeterProvider sets the metric.MeterProvider.  If nil, the global
	// Provider will be used.
	MeterProvider metric.MeterProvider
	appName       string
}

// MetricProviderOption Define confuration for metric Provider Option
type MetricProviderOption struct {
	metric.MeterProvider
}

// StateLogOption Option Interface define configuration parameters for statelog metrics agent
type StateLogOption interface {
	apply(*stateLogMetricsConfig)
}

// AppNameOption Define Option for OCCName
type AppNameOption string

// Headers
const IngestTokenHeader = "X-Sf-Token"
