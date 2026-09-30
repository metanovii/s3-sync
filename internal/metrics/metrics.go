// Package metrics defines the Prometheus metrics and the HTTP server with
// /metrics, /healthz and /readyz.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var labels = []string{"source", "target"}

// Metrics are shared by all syncs; every series carries source and target.
type Metrics struct {
	LastSuccess       *prometheus.GaugeVec
	PassDuration      *prometheus.GaugeVec
	Passes            *prometheus.CounterVec
	Objects           *prometheus.GaugeVec
	CopiedObjects     *prometheus.CounterVec
	CopiedBytes       *prometheus.CounterVec
	AdoptedObjects    *prometheus.CounterVec
	SkippedObjects    *prometheus.CounterVec
	DeletedObjects    *prometheus.CounterVec
	DeletionsPending  *prometheus.GaugeVec
	DeletionsHeld     *prometheus.GaugeVec
	Errors            *prometheus.CounterVec
	ACLNotCopied      *prometheus.CounterVec
	TargetUnexpected  *prometheus.GaugeVec
	FullCheckRecopied *prometheus.CounterVec
	LastPassFailed    *prometheus.GaugeVec
}

// New creates and registers the metrics.
func New(reg prometheus.Registerer) *Metrics {
	f := func(c prometheus.Collector) { reg.MustRegister(c) }
	gauge := func(name, help string, extra ...string) *prometheus.GaugeVec {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "s3sync", Name: name, Help: help}, append(labels, extra...))
		f(g)
		return g
	}
	counter := func(name, help string, extra ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "s3sync", Name: name, Help: help}, append(labels, extra...))
		f(c)
		return c
	}
	return &Metrics{
		LastSuccess:       gauge("last_success_timestamp_seconds", "End of the last successful pass, unix time."),
		PassDuration:      gauge("pass_duration_seconds", "Duration of the last pass."),
		Passes:            counter("passes_total", "Finished passes by result (success, failure).", "result"),
		Objects:           gauge("objects", "Objects recorded in state."),
		CopiedObjects:     counter("copied_objects_total", "Objects copied to the target."),
		CopiedBytes:       counter("copied_bytes_total", "Bytes copied to the target."),
		AdoptedObjects:    counter("adopted_objects_total", "Objects found already copied in the target on a first pass."),
		SkippedObjects:    counter("skipped_objects_total", "Objects that changed during copying and wait for the next pass."),
		DeletedObjects:    counter("deleted_objects_total", "Objects deleted in the target."),
		DeletionsPending:  gauge("deletions_pending", "Objects missing in the source, waiting for delete.delay."),
		DeletionsHeld:     gauge("deletions_held", "Deletions not executed because they exceed delete.max_count or delete.max_fraction."),
		Errors:            counter("errors_total", "Failed operations by kind.", "operation"),
		ACLNotCopied:      counter("acl_not_copied_total", "Objects copied without ACL because their grants match no canned ACL."),
		TargetUnexpected:  gauge("target_unexpected_objects", "Target objects unknown to state, found by the last full check."),
		FullCheckRecopied: counter("full_check_recopied_total", "Objects copied again because the full check found the target copy missing or different."),
		LastPassFailed:    gauge("last_pass_failed_objects", "Objects whose copy, deletion or ACL update failed in the last pass."),
	}
}

// Server serves the metrics and health endpoints.
type Server struct {
	srv   *http.Server
	ready atomic.Bool
}

// NewServer builds the server on listen with the metrics of reg.
func NewServer(listen string, reg *prometheus.Registry) *Server {
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	s := &Server{}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	s.srv = &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	return s
}

// SetReady marks the process as ready.
func (s *Server) SetReady() { s.ready.Store(true) }

// ListenAndServe serves until Shutdown.
func (s *Server) ListenAndServe() error {
	if err := s.srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
