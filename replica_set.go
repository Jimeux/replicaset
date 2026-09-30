package replicaset

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync/atomic"
	"time"
)

// ReplicaSet implements a load-balanced replica set with health monitoring,
// and circuit breaker functionality for multiple database replicas.
type ReplicaSet[DB DBMonitor] struct {
	replicas            []*Replica[DB]
	unhealthyThreshold  int64
	recoveryTimeout     time.Duration
	healthCheckInterval time.Duration
	counter             atomic.Uint64
}

// Config provides settings for a ReplicaSet instance.
type Config struct {
	UnhealthyThreshold  int64         // number of consecutive failures before marking a replica unhealthy
	RecoveryTimeout     time.Duration // recovery timeout for an unhealthy replica (replica is not used until this timeout elapses)
	HealthCheckInterval time.Duration // interval between health checks
}

func NewReplicaSet[DB DBMonitor](cfg Config) *ReplicaSet[DB] {
	return &ReplicaSet[DB]{
		unhealthyThreshold:  cfg.UnhealthyThreshold,
		recoveryTimeout:     cfg.RecoveryTimeout,
		healthCheckInterval: cfg.HealthCheckInterval,
	}
}

// AddReplica adds a new replica to the replica set.
// Note: not thread-safe; should be called only during initialization.
func (rs *ReplicaSet[DB]) AddReplica(id string, db DB) {
	rs.replicas = append(rs.replicas, newReplica(id, db, rs.unhealthyThreshold))
}

var ErrNoReplicasAvailable = errors.New("ReplicaSet failed to acquire a replica")

func Do[DB DBMonitor, Res any](rs *ReplicaSet[DB], fn func(DB) (Res, error)) (Res, error) {
	replica := rs.RoundRobin()
	if replica == nil {
		var zero Res
		return zero, ErrNoReplicasAvailable
	}
	result, err := fn(replica.db)
	if err != nil && isConnectionError(err) {
		replica.RecordFailure()
	} else {
		replica.RecordSuccess()
	}
	return result, err
}

// isConnectionError returns true for errors that indicate the Replica
// itself is unhealthy, as opposed to query-level errors (syntax, constraints, etc.).
// TODO could be configurable
func isConnectionError(err error) bool {
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	_, isMatch := errors.AsType[net.Error](err)
	return isMatch
}

// RoundRobin selects the next replica in a round-robin
func (rs *ReplicaSet[DB]) RoundRobin() *Replica[DB] {
	for range len(rs.replicas) {
		// increment and get the next index
		n := rs.counter.Add(1)
		idx := n % uint64(len(rs.replicas))
		// skip unusable replicas
		if rs.replicas[idx].isUsable() {
			return rs.replicas[idx]
		}
	}
	// TODO least failures?
	return rs.Random() // all unhealthy, so return any one
}

// Random selects a replica at Random from the replica set.
func (rs *ReplicaSet[DB]) Random() *Replica[DB] {
	if len(rs.replicas) == 0 {
		return nil
	}
	return rs.replicas[rand.IntN(len(rs.replicas))]
}

// RunHealthCheck periodically checks the health of all replicas.
// Attempts to recover unhealthy replicas after the recovery timeout has elapsed.
// It should be run asynchronously in a dedicated goroutine.
func (rs *ReplicaSet[DB]) RunHealthCheck(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return // ctx already expired
	}

	ticker := time.NewTicker(rs.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, rep := range rs.replicas {
				rep.checkHealth(ctx, rs.recoveryTimeout)
			}
		}
	}
}
