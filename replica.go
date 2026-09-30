package replicaset

import (
	"context"
	"sync/atomic"
	"time"
)

// DBMonitor is a constraint for database types with minimal monitoring capabilities.
type DBMonitor interface {
	PingContext(ctx context.Context) error
}

// Replica wraps a sql.DB and tracks its health status.
type Replica[DB DBMonitor] struct {
	id                  string       // unique identifier for the Replica
	db                  DB           // the database instance
	consecutiveFailures atomic.Int64 // number of consecutive failures since last success
	lastFailureTime     atomic.Int64 // Unix nano timestamp of last failure
	unhealthyThreshold  int64        // number of consecutive failures before marking a Replica unhealthy
}

func newReplica[DB DBMonitor](id string, db DB, unhealthyThreshold int64) *Replica[DB] {
	return &Replica[DB]{
		id:                 id,
		db:                 db,
		unhealthyThreshold: unhealthyThreshold,
	}
}

func (r *Replica[DB]) DB() DB { return r.db }

// isUsable returns true if Replica is in a healthy state.
func (r *Replica[DB]) isUsable() bool {
	return r.consecutiveFailures.Load() < r.unhealthyThreshold
}

// RecordFailure increments the failure counter, which will make the
// Replica unusable (circuit break) if the number of consecutive failures
// exceeds the unhealthy threshold.
func (r *Replica[DB]) RecordFailure() {
	// Store timestamp first so concurrent readers always see a fresh value when failures >= threshold.
	r.lastFailureTime.Store(time.Now().UnixNano())
	r.consecutiveFailures.Add(1)
}

// RecordSuccess resets the failure counter, making the Replica usable again.
func (r *Replica[DB]) RecordSuccess() {
	r.consecutiveFailures.Store(0)
}

// checkHealth checks the health of a given Replica.
// If the Replica is unhealthy and the recovery timeout has elapsed,
// it attempts to ping the Replica to see if it has recovered.
// Should run in a single goroutine.
func (r *Replica[DB]) checkHealth(ctx context.Context, recoveryTimeout time.Duration) {
	if r.isUsable() {
		return
	}
	// only retry if recoveryTimeout has elapsed (circuit breaker behavior)
	if time.Now().UnixNano()-r.lastFailureTime.Load() < recoveryTimeout.Nanoseconds() {
		return
	}
	// Attempt ping to see if replica is healthy again.
	// Note: as a TOCTOU race condition, the replica may have become healthy
	// between the time we checked IsUsable() and now, but executing the ping
	// is harmless in that case.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second) // TODO timeout could be configurable
	err := r.db.PingContext(ctx)
	cancel()

	if err != nil {
		r.RecordFailure() // increment failure counter again
	} else {
		r.RecordSuccess() // mark usable again
	}
}
