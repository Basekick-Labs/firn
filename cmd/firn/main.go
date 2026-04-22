package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/metrics"
	"github.com/basekick-labs/firn/internal/scheduler"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	root := &cobra.Command{
		Use:   "firn",
		Short: "Open source Iceberg table maintenance daemon",
		RunE:  run,
	}
	root.Flags().String("config", "firn.yaml", "Path to config file")

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) error {
	cfgPath, _ := cmd.Flags().GetString("config")

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}

	if cfg.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// Build Prometheus registry and Firn metrics.
	promReg := prometheus.NewRegistry()
	promReg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	metricsReg := metrics.NewRegistry(promReg)

	s, err := scheduler.New(cfg, metricsReg)
	if err != nil {
		return err
	}

	// Start metrics + health HTTP server if configured.
	if cfg.Scheduler.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(promReg, promhttp.HandlerOpts{}))
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok\n"))
		})
		mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
			cs := s.LastCycle()
			if cs == nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":"no cycle completed yet"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(cs)
		})
		srv := &http.Server{
			Addr:    cfg.Scheduler.MetricsAddr,
			Handler: mux,
		}
		go func() {
			<-cmd.Context().Done()
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutCtx)
		}()

		startErr := make(chan error, 1)
		go func() {
			log.Info().Str("addr", cfg.Scheduler.MetricsAddr).Msg("metrics server starting")
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				startErr <- err
			}
		}()
		select {
		case err := <-startErr:
			return fmt.Errorf("metrics server: %w", err)
		case <-time.After(100 * time.Millisecond):
		}
	}

	return s.Run(cmd.Context())
}
