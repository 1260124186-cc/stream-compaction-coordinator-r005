package httpapi

import (
	"net/http"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
)

const (
	defaultMaxRequestBytes = int64(1 << 20)
	defaultRequestTimeout  = 5 * time.Second
)

type Options struct {
	MaxRequestBytes int64
	RequestTimeout  time.Duration
	Logger          observability.Logger
}

type API struct {
	service         *application.Service
	maxRequestBytes int64
	requestTimeout  time.Duration
	logger          observability.Logger
}

func New(service *application.Service, options Options) *API {
	if options.MaxRequestBytes <= 0 {
		options.MaxRequestBytes = defaultMaxRequestBytes
	}
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = defaultRequestTimeout
	}
	if options.Logger == nil {
		options.Logger = observability.Discard{}
	}
	return &API{
		service:         service,
		maxRequestBytes: options.MaxRequestBytes,
		requestTimeout:  options.RequestTimeout,
		logger:          options.Logger,
	}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc(http.MethodGet+" /v1/healthz", a.handleHealth)
	mux.HandleFunc(http.MethodPost+" /v1/streams", a.handleCreateStream)
	mux.HandleFunc(http.MethodGet+" /v1/streams/{stream_id}", a.handleGetStream)
	mux.HandleFunc(
		http.MethodPost+" /v1/streams/{stream_id}/records",
		a.handleAppendRecords,
	)
	mux.HandleFunc(
		http.MethodGet+" /v1/streams/{stream_id}/segments",
		a.handleListSegments,
	)
	mux.HandleFunc(
		http.MethodGet+" /v1/streams/{stream_id}/watermark",
		a.handleWatermark,
	)
	mux.HandleFunc(
		http.MethodPost+" /v1/streams/{stream_id}/compaction-plans",
		a.handleCreateCompactionPlan,
	)
	mux.HandleFunc(
		http.MethodPost+" /v1/compaction-plans/{plan_id}/start",
		a.handleStartCompactionPlan,
	)
	mux.HandleFunc(
		http.MethodPost+" /v1/compaction-plans/{plan_id}/finish",
		a.handleFinishCompactionPlan,
	)
	mux.HandleFunc(
		http.MethodGet+" /v1/compaction-plans/{plan_id}",
		a.handleGetCompactionPlan,
	)
	mux.HandleFunc(http.MethodGet+" /v1/segments/{segment_id}", a.handleGetSegment)
	mux.HandleFunc(
		http.MethodPost+" /v1/segments/{segment_id}/replica-acks",
		a.handleAcknowledgeReplica,
	)
	mux.HandleFunc(http.MethodPost+" /v1/admin/recover", a.handleRecover)

	return a.withMiddleware(mux)
}
