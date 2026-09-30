package replicaset

import (
	"testing"
	"time"
)

func TestReplicaSet_AddReplica(t *testing.T) {
	t.Run("adds replica to replicas list", func(t *testing.T) {
		rs := NewReplicaSet[*mockDB](Config{
			UnhealthyThreshold: 3,
		})

		if got := len(rs.replicas); got != 0 {
			t.Errorf("expected 0 replicas initially, got %d", got)
		}

		mock1 := &mockDB{}
		rs.AddReplica("replica-1", mock1)
		if got := len(rs.replicas); got != 1 {
			t.Errorf("expected 1 replica after AddReplica, got %d", got)
		}
		if got := rs.replicas[0].id; got != "replica-1" {
			t.Errorf("expected replica id 'replica-1', got '%s'", got)
		}

		mock2 := &mockDB{}
		rs.AddReplica("replica-2", mock2)
		if got := len(rs.replicas); got != 2 {
			t.Errorf("expected 2 replicas after second AddReplica, got %d", got)
		}
		if got := rs.replicas[1].id; got != "replica-2" {
			t.Errorf("expected replica id 'replica-2', got '%s'", got)
		}
	})
}

func TestReplicaSet_RoundRobin(t *testing.T) {
	t.Run("selects from replicas first", func(t *testing.T) {
		rs := NewReplicaSet[*mockDB](Config{
			UnhealthyThreshold: 3,
		})

		mock := &mockDB{}
		rs.AddReplica("replica-1", mock)

		selected := rs.RoundRobin()
		if selected == nil {
			t.Fatal("expected selected replica to be non-nil")
		}
		if selected.id != "replica-1" {
			t.Errorf("expected replica id 'replica-1', got '%s'", selected.id)
		}
	})

	t.Run("uses random selection when all are unhealthy", func(t *testing.T) {
		rs := NewReplicaSet[*mockDB](Config{
			UnhealthyThreshold: 1,
		})

		mock := &mockDB{}
		rs.AddReplica("replica-1", mock)

		// Mark as unhealthy
		rs.replicas[0].RecordFailure()

		selected := rs.RoundRobin()
		if selected == nil {
			t.Fatal("expected selected replica to be non-nil")
		}
		if selected.id != "replica-1" {
			t.Errorf("expected replica id 'replica-1', got '%s'", selected.id)
		}
	})
}

func TestReplicaSet_Random(t *testing.T) {
	t.Run("selects a replica from replicas list", func(t *testing.T) {
		rs := NewReplicaSet[*mockDB](Config{
			UnhealthyThreshold: 3,
		})

		mock1 := &mockDB{}
		mock2 := &mockDB{}
		mock3 := &mockDB{}

		rs.AddReplica("replica-1", mock1)
		rs.AddReplica("replica-2", mock2)
		rs.AddReplica("replica-3", mock3)

		// Call Random several times to ensure it doesn't panic
		for range 10 {
			selected := rs.Random()
			if selected == nil {
				t.Fatal("expected selected replica to be non-nil")
			}
			// Should be one of our replicas
			found := false
			for _, r := range rs.replicas {
				if r.id == selected.id {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Random() returned replica with id '%s' which is not in replicas list", selected.id)
			}
		}
	})
}

func TestNewBalancedReplicaSet(t *testing.T) {
	t.Run("creates replica set with correct config", func(t *testing.T) {
		cfg := Config{
			UnhealthyThreshold:  5,
			RecoveryTimeout:     10 * time.Second,
			HealthCheckInterval: 30 * time.Second,
		}

		rs := NewReplicaSet[*mockDB](cfg)

		if rs.unhealthyThreshold != 5 {
			t.Errorf("expected unhealthyThreshold to be 5, got %d", rs.unhealthyThreshold)
		}
		if rs.recoveryTimeout != 10*time.Second {
			t.Errorf("expected recoveryTimeout to be 10s, got %v", rs.recoveryTimeout)
		}
		if rs.healthCheckInterval != 30*time.Second {
			t.Errorf("expected healthCheckInterval to be 30s, got %v", rs.healthCheckInterval)
		}
		if len(rs.replicas) != 0 {
			t.Errorf("expected 0 replicas initially, got %d", len(rs.replicas))
		}
	})
}
