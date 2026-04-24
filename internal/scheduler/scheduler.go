package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/basekick-labs/firn/internal/catalog"
	"github.com/basekick-labs/firn/internal/compaction"
	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/expiry"
	"github.com/basekick-labs/firn/internal/metrics"
	"github.com/basekick-labs/firn/internal/orphan"
	"github.com/basekick-labs/firn/internal/policy"
	"github.com/rs/zerolog/log"
)

// Scheduler drives the maintenance cycle on a fixed interval.
type Scheduler struct {
	catalog  catalog.Client
	engine   *compaction.Engine
	expiry   *expiry.Engine
	orphan   *orphan.Engine
	policy   *policy.Resolver
	metrics  *metrics.Registry
	cfg      *config.Config
	interval time.Duration

	mu   sync.RWMutex
	last *CycleStatus
}

// LastCycle returns the status of the most recently completed cycle, or nil if
// no cycle has completed yet.
func (s *Scheduler) LastCycle() *CycleStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.last
}

func New(cfg *config.Config, metricsReg *metrics.Registry) (*Scheduler, error) {
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

	retryer, err := buildRetryer(cfg)
	if err != nil {
		return nil, fmt.Errorf("build retryer: %w", err)
	}

	return &Scheduler{
		catalog:  cat,
		engine:   compaction.NewEngine(cat, stor, cfg, retryer),
		expiry:   expiry.NewEngine(cat, stor, retryer),
		orphan:   orphan.NewEngine(cat, stor),
		policy:   policy.NewResolver(&cfg.Maintenance),
		metrics:  metricsReg,
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
	var (
		wg       sync.WaitGroup
		statusMu sync.Mutex
		statuses = make([]TableStatus, 0, len(tables))
	)

	for _, t := range tables {
		t := t
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			ts := s.maintain(ctx, t)
			statusMu.Lock()
			statuses = append(statuses, ts)
			statusMu.Unlock()
		}()
	}

	wg.Wait()
	finished := time.Now()
	duration := finished.Sub(start)
	s.metrics.RecordCycle(duration, len(tables))

	cs := &CycleStatus{
		StartedAt:  start,
		FinishedAt: finished,
		Duration:   duration.Round(time.Millisecond).String(),
		Tables:     statuses,
	}
	s.mu.Lock()
	s.last = cs
	s.mu.Unlock()

	log.Info().
		Dur("duration", duration).
		Int("tables", len(tables)).
		Msg("maintenance cycle complete")
	return nil
}

func (s *Scheduler) maintain(ctx context.Context, id catalog.TableIdentifier) TableStatus {
	p := s.policy.For(id)
	table := id.String()
	ts := TableStatus{Table: table}

	if p.Compaction.IsEnabled() {
		candidates, err := s.engine.FindCandidates(ctx, id, p.Compaction)
		if err != nil {
			log.Error().Err(err).Str("table", table).Msg("find candidates failed")
			ts.Compaction = &CompactionStatus{Errors: 1}
		} else {
			log.Debug().Str("table", table).Int("candidates", len(candidates)).Msg("compaction candidates")
			cs := &CompactionStatus{}
			for _, c := range candidates {
				result, err := s.engine.ExecuteJob(ctx, c)
				s.metrics.RecordCompaction(table, result, err)
				if err != nil {
					log.Error().Err(err).Str("table", table).Str("partition", c.Partition).Msg("compaction job failed")
					cs.Errors++
					continue
				}
				cs.Jobs++
				cs.FilesMerged += len(result.InputFiles)
				cs.BytesBefore += result.BytesBefore
				cs.BytesAfter += result.BytesAfter
				log.Info().
					Str("table", table).
					Str("partition", c.Partition).
					Int("files_merged", len(result.InputFiles)).
					Int64("bytes_before", result.BytesBefore).
					Int64("bytes_after", result.BytesAfter).
					Dur("duration", result.Duration).
					Msg("compaction complete")
			}
			if cs.Jobs > 0 || cs.Errors > 0 {
				ts.Compaction = cs
			}
		}
	}

	if p.SnapshotExpiry.IsEnabled() {
		result, err := s.expiry.ExecuteExpiry(ctx, id, p.SnapshotExpiry)
		if err != nil {
			log.Error().Err(err).Str("table", table).Msg("snapshot expiry failed")
			ts.Expiry = &ExpiryStatus{Error: err.Error()}
		} else {
			s.metrics.RecordExpiry(table, result, nil)
			if result.ExpiredSnapshots > 0 {
				log.Info().
					Str("table", table).
					Int("expired_snapshots", result.ExpiredSnapshots).
					Int("deleted_manifests", result.DeletedManifests).
					Int("deleted_data_files", result.DeletedDataFiles).
					Dur("duration", result.Duration).
					Msg("snapshot expiry complete")
				ts.Expiry = &ExpiryStatus{
					ExpiredSnapshots: result.ExpiredSnapshots,
					DeletedManifests: result.DeletedManifests,
					DeletedDataFiles: result.DeletedDataFiles,
				}
			}
		}
	}

	if p.OrphanCleanup.IsEnabled() {
		result, err := s.orphan.ExecuteCleanup(ctx, id, p.OrphanCleanup)
		if err != nil {
			log.Error().Err(err).Str("table", table).Msg("orphan cleanup failed")
			ts.Orphan = &OrphanStatus{Error: err.Error()}
		} else {
			s.metrics.RecordOrphan(table, result, nil)
			if result.DeletedFiles > 0 {
				log.Info().
					Str("table", table).
					Int("scanned", result.ScannedFiles).
					Int("deleted", result.DeletedFiles).
					Int("skipped", result.SkippedFiles).
					Dur("duration", result.Duration).
					Msg("orphan cleanup complete")
				ts.Orphan = &OrphanStatus{
					ScannedFiles: result.ScannedFiles,
					DeletedFiles: result.DeletedFiles,
					SkippedFiles: result.SkippedFiles,
				}
			}
		}
	}

	return ts
}
