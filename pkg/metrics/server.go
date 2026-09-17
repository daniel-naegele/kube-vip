package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	log "log/slog"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// shutdownTimeout bounds how long the server waits for in-flight requests to
// finish once the context is cancelled.
const shutdownTimeout = 5 * time.Second

// ServerConfig defines the Prometheus server configuration.
type ServerConfig struct {
	// Addr sets the http server address used to expose the metric endpoint
	Addr string
}

// Serve exposes the Prometheus metrics endpoint on the configured address.
func Serve(ctx context.Context, config ServerConfig) error {
	ln, err := net.Listen("tcp", config.Addr)
	if err != nil {
		return fmt.Errorf("listening on %q: %w", config.Addr, err)
	}

	return serve(ctx, ln)
}

// serve starts the metrics endpoint on the provided listener
func serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           newServeMux(),
		ReadHeaderTimeout: 2 * time.Second,
	}

	wg := sync.WaitGroup{}
	defer wg.Wait()

	serveErr := make(chan error, 1)
	wg.Go(func() {
		err := srv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	})

	log.Info("prometheus HTTP server started", "addr", ln.Addr().String())

	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("serving prometheus metrics: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	// create prometheus shutdown context (independent of other contexts)
	ctxShutDown, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctxShutDown); err != nil {
		return fmt.Errorf("shutting down prometheus HTTP server: %w", err)
	}

	log.Info("prometheus HTTP server stopped")

	return nil
}

func newServeMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	// net/http/pprof's init() only registers itself on http.DefaultServeMux, which this server
	// deliberately doesn't use -- register the handlers explicitly instead. Useful for tracking
	// down CPU/goroutine issues (e.g. https://github.com/daniel-naegele/kube-vip/issues/5) without
	// needing strace/SIGQUIT-triggered goroutine dumps from outside the process.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`<html>
			<head><title>kube-vip</title></head>
			<body>
			<h1>kube-vip Metrics</h1>
			<p><a href="/metrics">Metrics</a></p>
			<p><a href="/debug/pprof/">pprof</a></p>
			</body>
			</html>`))
	})

	return mux
}
