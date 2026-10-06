package landing

import (
	"errors"
	"sync"
	"time"
)

type StationState string

const (
	// StationNeverOnline has never accepted an in-order heartbeat.
	StationNeverOnline StationState = "never_online"
	// StationOnline is accepting heartbeats within the freshness timeout.
	StationOnline StationState = "online"
	// StationStale has a last heartbeat older than the freshness timeout.
	StationStale StationState = "stale"
	// StationMaintenance is an operator-declared maintenance window; heartbeats are ignored.
	StationMaintenance StationState = "maintenance"
	// StationRecovering saw a fresh heartbeat after going stale and awaits confirmation.
	StationRecovering StationState = "recovering"
)

// Reasons recorded when the station state changes.
const (
	ReasonHeartbeatAccepted = "heartbeat_accepted"
	ReasonFirstContact      = "first_contact"
	ReasonHeartbeatStale    = "heartbeat_stale"
	ReasonNeverOnline       = "never_online"
	ReasonMaintenanceBegin  = "maintenance_begin"
	ReasonMaintenanceEnd    = "maintenance_end"
	ReasonRecoveryHeartbeat = "recovery_heartbeat"
	ReasonRecoveryConfirmed = "recovery_confirmed"
	ReasonRecovered         = "recovered"
)

type StationSnapshot struct {
	ID                 string       `json:"id"`
	Name               string       `json:"name"`
	State              StationState `json:"state"`
	LastSeenAt         time.Time    `json:"last_seen_at"`
	Sequence           uint64       `json:"sequence"`
	UpdatedAt          time.Time    `json:"updated_at"`
	StateChangedAt     time.Time    `json:"state_changed_at"`
	StateReason        string       `json:"state_reason,omitempty"`
	LastRecoveryReason string       `json:"last_recovery_reason,omitempty"`
	LastRecoveredAt    time.Time    `json:"last_recovered_at,omitempty"`
	LastAcceptedBeatAt time.Time    `json:"last_accepted_beat_at"`
}

type Station struct {
	mu       sync.RWMutex
	snapshot StationSnapshot
}

func NewStation(id, name string, at time.Time) (*Station, error) {
	if id == "" || name == "" {
		return nil, errors.New("station identity is required")
	}
	return &Station{snapshot: StationSnapshot{
		ID: id, Name: name,
		State: StationNeverOnline, StateReason: ReasonNeverOnline,
		UpdatedAt: at.UTC(),
	}}, nil
}

// Heartbeat accepts an in-order heartbeat. Stale (older or equal sequence)
// heartbeats never move the station back to online. While a maintenance
// window is active heartbeats are recorded but do not change the state.
func (s *Station) Heartbeat(sequence uint64, at time.Time) StationSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	beatAt := at.UTC()
	if sequence <= s.snapshot.Sequence {
		return s.snapshot
	}
	s.snapshot.Sequence = sequence
	s.snapshot.LastSeenAt = beatAt
	s.snapshot.LastAcceptedBeatAt = beatAt
	s.snapshot.UpdatedAt = beatAt
	switch s.snapshot.State {
	case StationMaintenance, StationRecovering:
		// Heartbeats stay tracked (sequence and freshness) but the station
		// only leaves these states through an explicit operator action.
	case StationStale:
		s.transitionLocked(StationRecovering, ReasonRecoveryHeartbeat, beatAt)
	case StationNeverOnline:
		s.transitionLocked(StationOnline, ReasonFirstContact, beatAt)
	default:
		s.transitionLocked(StationOnline, ReasonHeartbeatAccepted, beatAt)
	}
	return s.snapshot
}

// MarkStale flags the station when its newest heartbeat is past the freshness timeout.
// A station that never came online is left untouched.
func (s *Station) MarkStale(at time.Time) StationSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := at.UTC()
	if s.snapshot.State == StationOnline || s.snapshot.State == StationRecovering {
		s.transitionLocked(StationStale, ReasonHeartbeatStale, now)
		return s.snapshot
	}
	s.snapshot.UpdatedAt = now
	return s.snapshot
}

// BeginMaintenance opens an operator-declared maintenance window.
func (s *Station) BeginMaintenance(at time.Time) StationSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := at.UTC()
	if s.snapshot.State != StationMaintenance {
		s.transitionLocked(StationMaintenance, ReasonMaintenanceBegin, now)
		return s.snapshot
	}
	s.snapshot.UpdatedAt = now
	return s.snapshot
}

// EndMaintenance closes a maintenance window. A station with a fresh
// heartbeat enters recovering pending confirmation; one without any
// heartbeat has still never been online.
func (s *Station) EndMaintenance(reason string, at time.Time) StationSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := at.UTC()
	if s.snapshot.State != StationMaintenance {
		s.snapshot.UpdatedAt = now
		return s.snapshot
	}
	if reason == "" {
		reason = ReasonMaintenanceEnd
	}
	s.snapshot.LastRecoveryReason = reason
	s.snapshot.LastRecoveredAt = now
	if s.snapshot.LastSeenAt.IsZero() {
		s.transitionLocked(StationNeverOnline, ReasonMaintenanceEnd, now)
		return s.snapshot
	}
	s.transitionLocked(StationRecovering, ReasonMaintenanceEnd, now)
	return s.snapshot
}

// ConfirmRecovery completes a pending recovery and puts the station online.
func (s *Station) ConfirmRecovery(reason string, at time.Time) (StationSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := at.UTC()
	if s.snapshot.State != StationRecovering {
		return s.snapshot, errors.New("station is not awaiting recovery confirmation")
	}
	if reason == "" {
		reason = ReasonRecoveryConfirmed
	}
	s.snapshot.LastRecoveryReason = reason
	s.snapshot.LastRecoveredAt = now
	s.transitionLocked(StationOnline, ReasonRecovered, now)
	return s.snapshot, nil
}

func (s *Station) transitionLocked(state StationState, reason string, at time.Time) {
	if s.snapshot.State != state {
		s.snapshot.State = state
		s.snapshot.StateChangedAt = at
		s.snapshot.StateReason = reason
	}
	s.snapshot.UpdatedAt = at
}

func (s *Station) Snapshot() StationSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}
