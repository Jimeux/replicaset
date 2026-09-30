package replicaset

import (
	"context"
	"testing"
	"time"
)

// mockDB implements DBMonitor for testing
type mockDB struct {
	pingError error
}

func (m *mockDB) PingContext(ctx context.Context) error { return m.pingError }

func TestReplica_RecordFailure(t *testing.T) {
	t.Run("marks replica unhealthy after threshold failures", func(t *testing.T) {
		mock := &mockDB{}
		replica := newReplica("test-replica", mock, 2)

		// Initially healthy
		if !replica.isUsable() {
			t.Error("expected replica to be initially healthy")
		}
		if got := replica.consecutiveFailures.Load(); got != 0 {
			t.Errorf("expected 0 consecutive failures, got %d", got)
		}

		// First failure - still healthy
		replica.RecordFailure()
		if !replica.isUsable() {
			t.Error("expected replica to remain healthy after 1 failure")
		}
		if got := replica.consecutiveFailures.Load(); got != 1 {
			t.Errorf("expected 1 consecutive failure, got %d", got)
		}

		// Second failure - now unhealthy
		replica.RecordFailure()
		if replica.isUsable() {
			t.Error("expected replica to be unhealthy after 2 failures")
		}
		if got := replica.consecutiveFailures.Load(); got != 2 {
			t.Errorf("expected 2 consecutive failures, got %d", got)
		}

		// Third failure - remains unhealthy
		replica.RecordFailure()
		if replica.isUsable() {
			t.Error("expected replica to remain unhealthy after 3 failures")
		}
		if got := replica.consecutiveFailures.Load(); got != 3 {
			t.Errorf("expected 3 consecutive failures, got %d", got)
		}
	})

	t.Run("records failure timestamp", func(t *testing.T) {
		mock := &mockDB{}
		replica := newReplica("test-replica", mock, 3)

		before := time.Now().UnixNano()
		replica.RecordFailure()
		after := time.Now().UnixNano()

		timestamp := replica.lastFailureTime.Load()
		if timestamp < before || timestamp > after {
			t.Errorf("timestamp %d outside expected range %d < x < %d", timestamp, before, after)
		}
	})
}

func TestReplica_RecordSuccess(t *testing.T) {
	t.Run("marks unhealthy replica as healthy", func(t *testing.T) {
		mock := &mockDB{}
		replica := newReplica("test-replica", mock, 2)

		// Make it unhealthy
		replica.RecordFailure()
		replica.RecordFailure()
		if replica.isUsable() {
			t.Error("expected replica to be unhealthy after 2 failures")
		}

		// Record success should mark it healthy and reset counter
		replica.RecordSuccess()
		if !replica.isUsable() {
			t.Error("expected replica to be healthy after RecordSuccess")
		}
		if got := replica.consecutiveFailures.Load(); got != 0 {
			t.Errorf("expected consecutive failures to be reset to 0, got %d", got)
		}
	})

	t.Run("keeps healthy replica healthy and resets counter", func(t *testing.T) {
		mock := &mockDB{}
		replica := newReplica("test-replica", mock, 3)

		// Record one failure
		replica.RecordFailure()
		if got := replica.consecutiveFailures.Load(); got != 1 {
			t.Errorf("expected 1 consecutive failure, got %d", got)
		}

		// Record success should keep it healthy and reset counter
		replica.RecordSuccess()
		if !replica.isUsable() {
			t.Error("expected replica to remain healthy after RecordSuccess")
		}
		if got := replica.consecutiveFailures.Load(); got != 0 {
			t.Errorf("expected consecutive failures to be reset to 0, got %d", got)
		}
	})
}

func TestReplica_DB(t *testing.T) {
	t.Run("returns the wrapped db instance", func(t *testing.T) {
		mock := &mockDB{}
		replica := newReplica("test-replica", mock, 3)

		db := replica.DB()
		if db == nil {
			t.Fatal("expected DB() to return non-nil")
		}
		if err := db.PingContext(context.Background()); err != nil {
			t.Errorf("expected PingContext to have no err, got %v", err)
		}
	})
}

func TestNewReplica(t *testing.T) {
	t.Run("creates replica with correct initial state", func(t *testing.T) {
		mock := &mockDB{}
		replica := newReplica("test-id", mock, 5)

		if replica.id != "test-id" {
			t.Errorf("expected id 'test-id', got '%s'", replica.id)
		}
		if replica.db == nil {
			t.Error("expected db to be non-nil")
		}
		if !replica.isUsable() {
			t.Error("expected replica to be initially healthy")
		}
		if got := replica.consecutiveFailures.Load(); got != 0 {
			t.Errorf("expected 0 consecutive failures, got %d", got)
		}
		if replica.unhealthyThreshold != 5 {
			t.Errorf("expected unhealthyThreshold to be 5, got %d", replica.unhealthyThreshold)
		}
	})
}
