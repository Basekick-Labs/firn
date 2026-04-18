package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/compaction"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/policy"
	"github.com/rs/zerolog/log"
)

// Scheduler drives the maintenance cycle on a fixed interval.
type Scheduler struct {
	catalog  catalog.Client
	engine   *compaction.Engine
	policy   *policy.Resolver
	cfg      *config.Config
	interval time.Duration
}

func New(cfg *config.Config) (*Scheduler, error) {
	interval, err := time.ParseDuration(cfg.Scheduler.Interval)
	if err != nil {
		return nil, fmt.Errorf("parse scheduler interval %q: %w", cfg.Scheduler.Interval, err)
	}

	cat, err := buildCatalog(cfg)
	if err != nil {
		return nil, fmt.Errorf("build catalog: %w", err)
	}

	stor, err := buildStorage(cfg)
	if err != nil {
		return nil, fmt.Errorf("build storage: %w", err)
	}

	return &Scheduler{
		catalog:  cat,
		engine:   compaction.NewEngine(cat, stor, cfg),
		policy:   policy.NewResolver(&cfg.Maintenance),
		cfg:      cfg,
		interval: interval,
	}, nil
}

// Run starts the scheduler loop and blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	log.Info().Str("interval", s.interval.String()).Msg("firn scheduler starting")

	if err := s.cycle(ctx); err != nil {
		log.Error().Err(err).Msg("initial cycle failed")
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("firn scheduler stopped")
			return nil
		case <-ticker.C:
			if err := s.cycle(ctx); err != nil {
				log.Error().Err(err).Msg("cycle failed")
			}
		}
	}
}

func (s *Scheduler) cycle(ctx context.Context) error {
	start := time.Now()
	log.Info().Msg("maintenance cycle starting")

	namespaces, err := s.catalog.ListNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("list namespaces: %w", err)
	}

	var tables []catalog.TableIdentifier
	for _, ns := range namespaces {
		tt, err := s.catalog.ListTables(ctx, ns)
		if err != nil {
			log.Error().Err(err).Str("namespace", ns).Msg("list tables failed")
			continue
		}
		tables = append(tables, tt...)
	}

	sem := make(chan struct{}, s.cfg.Scheduler.MaxConcurrentJobs)
	var wg sync.WaitGroup

	for _, t := range tables {
		t := t
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.maintain(ctx, t)
		}()
	}

	wg.Wait()
	log.Info().
		Dur("duration", time.Since(start)).
		Int("tables", len(tables)).
		Msg("maintenance cycle complete")
	return nil
}

func (s *Scheduler) maintain(ctx context.Context, id catalog.TableIdentifier) {
	p := s.policy.For(id)

	if p.Compaction.Enabled {
		candidates, err := s.engine.FindCandidates(ctx, id, p.Compaction)
		if err != nil {
			log.Error().Err(err).Str("table", id.String()).Msg("find candidates failed")
			return
		}
		log.Debug().Str("table", id.String()).Int("candidates", len(candidates)).Msg("compaction candidates")
		// TODO: execute compaction jobs for each candidate
	}

	// TODO: snapshot expiry
	// TODO: orphan cleanup
}
