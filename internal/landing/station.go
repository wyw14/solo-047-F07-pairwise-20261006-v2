package landing

import (
	"errors"
	"sync"
	"time"
)

type StationState string

const (
	StationOnline   StationState = "online"
	StationDegraded StationState = "degraded"
	StationOffline  StationState = "offline"
)

// Transition reasons recorded in the station's state-change history.
const (
	ReasonInitialHeartbeat     = "initial_heartbeat"
	ReasonHeartbeatResumed     = "heartbeat_resumed"
	ReasonHeartbeatExpired     = "heartbeat_expired"
	ReasonMaintenanceStarted   = "maintenance_started"
	ReasonMaintenanceCompleted = "maintenance_completed"
)

type StateTransition struct {
	From   StationState `json:"from"`
	To     StationState `json:"to"`
	At     time.Time    `json:"at"`
	Reason string       `json:"reason"`
}

type Recovery struct {
	At     time.Time `json:"at"`
	Reason string    `json:"reason"`
}

type MaintenanceWindow struct {
	Reason string    `json:"reason"`
	Since  time.Time `json:"since"`
}

type StationSnapshot struct {
	ID         string       `json:"id"`
	Name       string       `json:"name"`
	State      StationState `json:"state"`
	LastSeenAt time.Time    `json:"last_seen_at"`
	Sequence   uint64       `json:"sequence"`
	UpdatedAt  time.Time    `json:"updated_at"`
}

type Station struct {
	mu           sync.RWMutex
	snapshot     StationSnapshot
	transitions  []StateTransition
	maintenance  *MaintenanceWindow
	lastRecovery *Recovery
	everOnline   bool
	rejected     uint64
}

// NewStation starts offline with no heartbeat seen; the first in-order
// heartbeat brings the station online.
func NewStation(id, name string, at time.Time) (*Station, error) {
	if id == "" || name == "" {
		return nil, errors.New("station identity is required")
	}
	return &Station{snapshot: StationSnapshot{
		ID: id, Name: name, State: StationOffline, UpdatedAt: at.UTC(),
	}}, nil
}

// Heartbeat applies a heartbeat. Out-of-order sequences (anything not greater
// than the last accepted sequence) are counted as rejected and never change
// the station state, so a stale heartbeat cannot mark a station online again.
func (s *Station) Heartbeat(sequence uint64, at time.Time) (StationSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sequence <= s.snapshot.Sequence {
		s.rejected++
		return s.snapshot, false
	}
	s.applyLocked(sequence, at)
	return s.snapshot, true
}

// Restore replays a persisted checkpoint without counting it as a live
// heartbeat; equal or older checkpoints are ignored silently.
func (s *Station) Restore(sequence uint64, seenAt time.Time) StationSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sequence > s.snapshot.Sequence {
		s.applyLocked(sequence, seenAt)
	}
	return s.snapshot
}

func (s *Station) applyLocked(sequence uint64, at time.Time) {
	s.snapshot.Sequence = sequence
	s.snapshot.LastSeenAt = at.UTC()
	s.snapshot.UpdatedAt = at.UTC()
	if s.maintenance != nil || s.snapshot.State == StationOnline {
		return
	}
	reason := ReasonHeartbeatResumed
	if !s.everOnline {
		reason = ReasonInitialHeartbeat
	}
	s.recoverLocked(reason, at)
}

// Expire marks an online station offline because its heartbeat aged out.
// Stations under maintenance or already offline are left untouched.
func (s *Station) Expire(at time.Time) (StationSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maintenance != nil || s.snapshot.State != StationOnline {
		return s.snapshot, false
	}
	s.transitionLocked(StationOffline, ReasonHeartbeatExpired, at)
	return s.snapshot, true
}

// MarkOnline recovers an offline station with an explicit reason (for example
// maintenance completion). It is a no-op while maintenance is active.
func (s *Station) MarkOnline(reason string, at time.Time) StationSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maintenance == nil && s.snapshot.State != StationOnline {
		s.recoverLocked(reason, at)
	}
	return s.snapshot
}

func (s *Station) recoverLocked(reason string, at time.Time) {
	s.transitionLocked(StationOnline, reason, at)
	s.everOnline = true
	if reason != ReasonInitialHeartbeat {
		s.lastRecovery = &Recovery{At: at.UTC(), Reason: reason}
	}
}

func (s *Station) BeginMaintenance(reason string, at time.Time) StationSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maintenance = &MaintenanceWindow{Reason: reason, Since: at.UTC()}
	if s.snapshot.State != StationOffline {
		s.transitionLocked(StationOffline, ReasonMaintenanceStarted, at)
	}
	return s.snapshot
}

// EndMaintenance clears the maintenance window and reports whether one was
// active. The caller decides whether the station may recover.
func (s *Station) EndMaintenance(at time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maintenance == nil {
		return false
	}
	s.maintenance = nil
	s.snapshot.UpdatedAt = at.UTC()
	return true
}

func (s *Station) transitionLocked(to StationState, reason string, at time.Time) {
	from := s.snapshot.State
	if from == to {
		return
	}
	s.snapshot.State = to
	s.snapshot.UpdatedAt = at.UTC()
	s.transitions = append(s.transitions, StateTransition{From: from, To: to, At: at.UTC(), Reason: reason})
}

func (s *Station) Maintenance() (MaintenanceWindow, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.maintenance == nil {
		return MaintenanceWindow{}, false
	}
	return *s.maintenance, true
}

func (s *Station) LastRecovery() (Recovery, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lastRecovery == nil {
		return Recovery{}, false
	}
	return *s.lastRecovery, true
}

func (s *Station) Transitions() []StateTransition {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]StateTransition(nil), s.transitions...)
}

// Rejected counts heartbeats ignored because their sequence did not advance.
func (s *Station) Rejected() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rejected
}

func (s *Station) Snapshot() StationSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}
