package otel

import (
	"go.opentelemetry.io/otel/metric"
	"sync"
)

// "init", "acpt", "wait", "busy", "schd", "fnsh", "quce", "asgn", "idle", "bklg", "strd", "cls"
// Following Metric Names will get instrumented as part of StateLogMetrics
const (
	// Worker States
	InitConnMetric      = "init_connection"
	AccptConnMetric     = "accept_connection"
	WaitConnMetric      = "wait_connection"
	BusyConnMetric      = "busy_connection"
	ScheduledConnMetric = "scheduled_connection"
	FinishedConnMetric  = "finished_connection"
	QuiescedConnMetric  = "quiesced_connection"

	// Connection States
	AssignedConnMetric = "assigned_connection"
	IdleConnMetric     = "idle_connection"
	BacklogConnMetric  = "backlog_connection"
	StrdConnMetric     = "stranded_connection"
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

var StatelogBucket = []float64{0, 5, 10, 15, 20, 25, 30, 40, 50, 60, 80, 100, 120, 160, 200}
var ConnectionStateBucket = []float64{0, 25, 50, 75, 100, 150, 200, 300, 400, 500, 600, 700, 800, 1200, 2400, 4800, 9600, 19200, 39400, 65536}

// WorkerTypeMap This map represents worker type configured in lib.HeraWorkerType variable. If any changes in worker type this definition need to get updated.
var WorkerTypeMap = map[int]string{
	0: "rw",
	1: "ro",
	2: "standby_ro",
}

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

	initState metric.Int64Histogram
	acptState metric.Int64Histogram
	waitState metric.Int64Histogram
	busyState metric.Int64Histogram
	schdState metric.Int64Histogram
	fnshState metric.Int64Histogram
	quceState metric.Int64Histogram
	asgnState metric.Int64Histogram
	idleState metric.Int64Histogram
	bklgState metric.Int64Histogram
	strdState metric.Int64Histogram
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
