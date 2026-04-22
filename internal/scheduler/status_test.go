package scheduler

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestScheduler returns a zero-value Scheduler sufficient for status tests.
// It does not initialise engines or catalog — only the mu/last fields are used.
func newTestScheduler() *Scheduler {
	return &Scheduler{}
}

func TestLastCycle_NilBeforeFirstCycle(t *testing.T) {
	s := newTestScheduler()
	assert.Nil(t, s.LastCycle())
}

func TestLastCycle_PopulatedAfterCycle(t *testing.T) {
	s := newTestScheduler()

	before := time.Now()
	cs := &CycleStatus{
		StartedAt:  before,
		FinishedAt: before.Add(2 * time.Second),
		Duration:   "2s",
		Tables: []TableStatus{
			{Table: "analytics.events", Compaction: &CompactionStatus{Jobs: 1, FilesMerged: 5}},
			{Table: "analytics.users"},
		},
	}
	s.mu.Lock()
	s.last = cs
	s.mu.Unlock()

	got := s.LastCycle()
	require.NotNil(t, got)
	assert.Equal(t, "2s", got.Duration)
	assert.Len(t, got.Tables, 2)
	assert.Equal(t, "analytics.events", got.Tables[0].Table)
	require.NotNil(t, got.Tables[0].Compaction)
	assert.Equal(t, 1, got.Tables[0].Compaction.Jobs)
	assert.Equal(t, 5, got.Tables[0].Compaction.FilesMerged)
	assert.True(t, got.FinishedAt.After(got.StartedAt))
}

func TestLastCycle_ReplacedOnNextCycle(t *testing.T) {
	s := newTestScheduler()

	first := &CycleStatus{Duration: "1s", Tables: []TableStatus{{Table: "ns.t1"}}}
	s.mu.Lock()
	s.last = first
	s.mu.Unlock()

	require.Equal(t, "1s", s.LastCycle().Duration)

	second := &CycleStatus{Duration: "3s", Tables: []TableStatus{{Table: "ns.t1"}, {Table: "ns.t2"}}}
	s.mu.Lock()
	s.last = second
	s.mu.Unlock()

	got := s.LastCycle()
	assert.Equal(t, "3s", got.Duration)
	assert.Len(t, got.Tables, 2)
}

func TestLastCycle_ConcurrentReadWrite(t *testing.T) {
	s := newTestScheduler()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			s.mu.Lock()
			s.last = &CycleStatus{Duration: "1s"}
			s.mu.Unlock()
		}(i)
		go func() {
			defer wg.Done()
			_ = s.LastCycle()
		}()
	}
	wg.Wait()
}
