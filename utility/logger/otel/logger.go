package otel

import (
	"context"
	"errors"
	"fmt"
	"github.com/paypal/hera/utility/logger"
	"github.com/paypal/hera/utility/logger/otel/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"os"
	"sync"
	"time"
)

var oTelInitializeOnce sync.Once

// Init takes care of initialize OTEL SDK once during startup
func Init(ctx context.Context) (shutdown func(ctx context.Context) error, err error) {
	oTelInitializeOnce.Do(func() {
		shutdown, err = initializeOTelSDK(ctx)
	})
	return shutdown, err
}

// InitializeOTelSDK SetupOTelSDK bootstrap the OTEL SDK pipeline initialization
func initializeOTelSDK(ctx context.Context) (shutdown func(ctx context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error
	//shutdown calls cleanup function registered via shutdown functions
	//The errors from calls are joined
	shutdown = func(ctx context.Context) error {
		var localErr error
		for _, fn := range shutdownFuncs {
			if fnErr := fn(ctx); fnErr != nil {
				localErr = errors.Join(localErr, fnErr) // You can use other error accumulation strategies if needed
			}
		}
		if localErr != nil {
			logger.GetLogger().Log(logger.Warning, fmt.Sprintf("error while performing otel shutdown, err: %v", localErr))
		}
		shutdownFuncs = nil
		return localErr
	}

	//handle error calls shutdown for cleanup and make sure all errors returned
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	errorTicker = time.NewTicker(time.Duration(config.OTelConfigData.OTelErrorReportingInterval) * time.Second)

	errorDataMap := make(map[string]*OTelErrorData) //Initialize the map after process it.
	gErrorDataMap.Store(errorDataMap)

	//Setup meter provider
	meterProvider, err := newMeterProvider(ctx)
	otel.SetMeterProvider(meterProvider)
	if err != nil {
		handleErr(err)
		return nil, err
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)

	oTelErrorHandler := OTelErrorHandler{}
	otel.SetErrorHandler(oTelErrorHandler)  //Register custom error handler
	oTelErrorHandler.processOTelErrorsMap() //Spawn Go routine peridically process OTEL errors
	shutdownFuncs = append(shutdownFuncs, func(ctx context.Context) error {
		errorTicker.Stop()
		return nil
	})
	return shutdown, err
}

// Initialize newMeterProvider respective exporter either HTTP or GRPC exporter
func newMeterProvider(ctx context.Context) (*metric.MeterProvider, error) {
	metricExporter, err := getMetricExporter(ctx)

	if err != nil {
		logger.GetLogger().Log(logger.Alert, "failed to initialize metric exporter, error %v", err)
		return nil, err
	}
	metricViews := getStateLogMetricsViews()
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(getResourceInfo(config.OTelConfigData.PoolName)),
		metric.WithReader(metric.NewPeriodicReader(metricExporter,
			metric.WithInterval(time.Duration(config.OTelConfigData.ResolutionTimeInSec)*time.Second))),
		metric.WithView(metricViews...),
	)
	return meterProvider, nil
}

func getStateLogMetricsViews() []metric.View {
	initView := metric.NewView(
		metric.Instrument{
			Name:  config.OTelConfigData.PopulateMetricNamePrefix(InitConnMetric),
			Scope: instrumentation.Scope{Name: StateLogMeterName},
		},
		metric.Stream{
			Aggregation: metric.AggregationBase2ExponentialHistogram{MaxSize: 32, MaxScale: 20},
		},
	)

	acptStateView := metric.NewView(
		metric.Instrument{
			Name:  config.OTelConfigData.PopulateMetricNamePrefix(AccptConnMetric),
			Scope: instrumentation.Scope{Name: StateLogMeterName},
		},
		metric.Stream{
			Aggregation: metric.AggregationBase2ExponentialHistogram{MaxSize: 32, MaxScale: 20},
		},
	)

	waitStateView := metric.NewView(
		metric.Instrument{
			Name:  config.OTelConfigData.PopulateMetricNamePrefix(WaitConnMetric),
			Scope: instrumentation.Scope{Name: StateLogMeterName},
		},
		metric.Stream{
			Aggregation: metric.AggregationBase2ExponentialHistogram{MaxSize: 32, MaxScale: 20},
		},
	)

	busyStateView := metric.NewView(
		metric.Instrument{
			Name:  config.OTelConfigData.PopulateMetricNamePrefix(BusyConnMetric),
			Scope: instrumentation.Scope{Name: StateLogMeterName},
		},
		metric.Stream{
			Aggregation: metric.AggregationBase2ExponentialHistogram{MaxSize: 32, MaxScale: 20},
		},
	)

	schdStateView := metric.NewView(
		metric.Instrument{
			Name:  config.OTelConfigData.PopulateMetricNamePrefix(ScheduledConnMetric),
			Scope: instrumentation.Scope{Name: StateLogMeterName},
		},
		metric.Stream{
			Aggregation: metric.AggregationBase2ExponentialHistogram{MaxSize: 32, MaxScale: 20},
		},
	)

	fnshStateView := metric.NewView(
		metric.Instrument{
			Name:  config.OTelConfigData.PopulateMetricNamePrefix(FinishedConnMetric),
			Scope: instrumentation.Scope{Name: StateLogMeterName},
		},
		metric.Stream{
			Aggregation: metric.AggregationBase2ExponentialHistogram{MaxSize: 32, MaxScale: 20},
		},
	)

	quceStateView := metric.NewView(
		metric.Instrument{
			Name:  config.OTelConfigData.PopulateMetricNamePrefix(QuiescedConnMetric),
			Scope: instrumentation.Scope{Name: StateLogMeterName},
		},
		metric.Stream{
			Aggregation: metric.AggregationBase2ExponentialHistogram{MaxSize: 32, MaxScale: 20},
		},
	)

	asgnStateView := metric.NewView(
		metric.Instrument{
			Name:  config.OTelConfigData.PopulateMetricNamePrefix(AssignedConnMetric),
			Scope: instrumentation.Scope{Name: StateLogMeterName},
		},
		metric.Stream{
			Aggregation: metric.AggregationBase2ExponentialHistogram{MaxSize: 32, MaxScale: 20},
		},
	)

	idleStateView := metric.NewView(
		metric.Instrument{
			Name:  config.OTelConfigData.PopulateMetricNamePrefix(IdleConnMetric),
			Scope: instrumentation.Scope{Name: StateLogMeterName},
		},
		metric.Stream{
			Aggregation: metric.AggregationBase2ExponentialHistogram{MaxSize: 32, MaxScale: 20},
		},
	)

	bklgStateView := metric.NewView(
		metric.Instrument{
			Name:  config.OTelConfigData.PopulateMetricNamePrefix(BacklogConnMetric),
			Scope: instrumentation.Scope{Name: StateLogMeterName},
		},
		metric.Stream{
			Aggregation: metric.AggregationBase2ExponentialHistogram{MaxSize: 32, MaxScale: 20},
		},
	)

	strdStateView := metric.NewView(
		metric.Instrument{
			Name:  config.OTelConfigData.PopulateMetricNamePrefix(StrdConnMetric),
			Scope: instrumentation.Scope{Name: StateLogMeterName},
		},
		metric.Stream{
			Aggregation: metric.AggregationBase2ExponentialHistogram{MaxSize: 32, MaxScale: 20},
		},
	)
	return []metric.View{initView, acptStateView, waitStateView, busyStateView, schdStateView,
		fnshStateView, quceStateView, asgnStateView, idleStateView, bklgStateView, strdStateView}
}

// getMetricExporter Initialize metric exporter based protocol selected by user.
func getMetricExporter(ctx context.Context) (metric.Exporter, error) {
	if config.OTelConfigData.OtelMetricGRPC {
		return newGRPCExporter(ctx)
	}
	return newHTTPExporter(ctx)
}

// newHTTPExporter Initilizes The "otlpmetrichttp" exporter in OpenTelemetry is used to export metrics data using the
// OpenTelemetry Protocol (OTLP) over HTTP.
func newHTTPExporter(ctx context.Context) (metric.Exporter, error) {
	headers := make(map[string]string)
	headers[IngestTokenHeader] = config.GetOTelIngestToken()

	//Currently all metrics uses delta-temporality: Delta Temporality: Use when you are interested in the rate of change
	//over time or when you need to report only the differences (deltas) between measurements.
	//This is useful for metrics like CPU usage, request rates, or other metrics where the rate of change is important.
	var temporalitySelector = func(instrument metric.InstrumentKind) metricdata.Temporality { return metricdata.DeltaTemporality }

	if config.OTelConfigData.UseTls {
		return otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpoint(fmt.Sprintf("%s:%d", config.OTelConfigData.Host, config.OTelConfigData.MetricsPort)),
			otlpmetrichttp.WithTimeout(time.Duration(config.OTelConfigData.ExporterTimeout)*time.Second),
			otlpmetrichttp.WithCompression(otlpmetrichttp.NoCompression),
			otlpmetrichttp.WithTemporalitySelector(temporalitySelector),
			otlpmetrichttp.WithHeaders(headers),
			otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
				// Enabled indicates whether to not retry sending batches in case
				// of export failure.
				Enabled: false,
				// InitialInterval the time to wait after the first failure before
				// retrying.
				InitialInterval: 1 * time.Second,
				// MaxInterval is the upper bound on backoff interval. Once this
				// value is reached the delay between consecutive retries will
				// always be `MaxInterval`.
				MaxInterval: 10 * time.Second,
				// MaxElapsedTime is the maximum amount of time (including retries)
				// spent trying to send a request/batch. Once this value is
				// reached, the data is discarded.
				MaxElapsedTime: 20 * time.Second,
			}),
			otlpmetrichttp.WithURLPath(config.OTelConfigData.MetricsURLPath),
		)
	} else {
		return otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpoint(fmt.Sprintf("%s:%d", config.OTelConfigData.Host, config.OTelConfigData.MetricsPort)),
			otlpmetrichttp.WithTimeout(time.Duration(config.OTelConfigData.ExporterTimeout)*time.Second),
			otlpmetrichttp.WithCompression(otlpmetrichttp.NoCompression),
			otlpmetrichttp.WithTemporalitySelector(temporalitySelector),
			otlpmetrichttp.WithHeaders(headers),
			otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
				// Enabled indicates whether to not retry sending batches in case
				// of export failure.
				Enabled: false,
				// InitialInterval the time to wait after the first failure before
				// retrying.
				InitialInterval: 1 * time.Second,
				// MaxInterval is the upper bound on backoff interval. Once this
				// value is reached the delay between consecutive retries will
				// always be `MaxInterval`.
				MaxInterval: 10 * time.Second,
				// MaxElapsedTime is the maximum amount of time (including retries)
				// spent trying to send a request/batch. Once this value is
				// reached, the data is discarded.
				MaxElapsedTime: 20 * time.Second,
			}),
			otlpmetrichttp.WithURLPath(config.OTelConfigData.MetricsURLPath),
			otlpmetrichttp.WithInsecure(), //Since agent is local
		)
	}
}

// newGRPCExporter Initializes The "otlpmetricgrpc" exporter in OpenTelemetry is used to export metrics data using the
// OpenTelemetry Protocol (OTLP) over GRPC.
func newGRPCExporter(ctx context.Context) (metric.Exporter, error) {

	headers := make(map[string]string)
	headers[IngestTokenHeader] = config.GetOTelIngestToken()

	//Currently all metrics uses delta-temporality: Delta Temporality: Use when you are interested in the rate of change
	//over time or when you need to report only the differences (deltas) between measurements.
	//This is useful for metrics like CPU usage, request rates, or other metrics where the rate of change is important.
	var temporalitySelector = func(instrument metric.InstrumentKind) metricdata.Temporality { return metricdata.DeltaTemporality }
	if config.OTelConfigData.UseTls {
		return otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(fmt.Sprintf("%s:%d", config.OTelConfigData.Host, config.OTelConfigData.MetricsPort)),
			otlpmetricgrpc.WithTimeout(time.Duration(config.OTelConfigData.ExporterTimeout)*time.Second),
			otlpmetricgrpc.WithHeaders(headers),
			otlpmetricgrpc.WithReconnectionPeriod(time.Duration(5)*time.Second),
			otlpmetricgrpc.WithTemporalitySelector(temporalitySelector),
			otlpmetricgrpc.WithRetry(otlpmetricgrpc.RetryConfig{
				// Enabled indicates whether to not retry sending batches in case
				// of export failure.
				Enabled: false,
				// InitialInterval the time to wait after the first failure before
				// retrying.
				InitialInterval: 1 * time.Second,
				// MaxInterval is the upper bound on backoff interval. Once this
				// value is reached the delay between consecutive retries will
				// always be `MaxInterval`.
				MaxInterval: 10 * time.Second,
				// MaxElapsedTime is the maximum amount of time (including retries)
				// spent trying to send a request/batch. Once this value is
				// reached, the data is discarded.
				MaxElapsedTime: 20 * time.Second,
			}),
		)

	} else {
		return otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(fmt.Sprintf("%s:%d", config.OTelConfigData.Host, config.OTelConfigData.MetricsPort)),
			otlpmetricgrpc.WithTimeout(time.Duration(config.OTelConfigData.ExporterTimeout)*time.Second),
			otlpmetricgrpc.WithHeaders(headers),
			otlpmetricgrpc.WithReconnectionPeriod(time.Duration(5)*time.Second),
			otlpmetricgrpc.WithTemporalitySelector(temporalitySelector),
			otlpmetricgrpc.WithRetry(otlpmetricgrpc.RetryConfig{
				// Enabled indicates whether to not retry sending batches in case
				// of export failure.
				Enabled: false,
				// InitialInterval the time to wait after the first failure before
				// retrying.
				InitialInterval: 1 * time.Second,
				// MaxInterval is the upper bound on backoff interval. Once this
				// value is reached the delay between consecutive retries will
				// always be `MaxInterval`.
				MaxInterval: 10 * time.Second,
				// MaxElapsedTime is the maximum amount of time (including retries)
				// spent trying to send a request/batch. Once this value is
				// reached, the data is discarded.
				MaxElapsedTime: 20 * time.Second,
			}),
			otlpmetricgrpc.WithInsecure(), //Since agent is local
		)
	}
}

// getResourceInfo provide application context level attributes during initialization
func getResourceInfo(appName string) *resource.Resource {
	hostname, _ := os.Hostname()

	// Create a slice to hold the attributes
	attributes := []attribute.KeyValue{
		attribute.String(ContainerHostDimName, hostname),
		attribute.String(ApplicationDimName, appName),
		attribute.String(OtelSourceName, otelSource),
	}

	environment, isEnvPresent := os.LookupEnv("ENVIRONMENT")
	az, isAzPresent := os.LookupEnv("AVAILABILITY_ZONE")
	if isEnvPresent {
		attributes = append(attributes, attribute.String("environment", environment))
	}
	if isAzPresent {
		attributes = append(attributes, attribute.String("az", az))
	}
	resource := resource.NewWithAttributes(fmt.Sprintf("%s resource", config.OTelConfigData.ResourceType),
		attributes...,
	)
	return resource
}
