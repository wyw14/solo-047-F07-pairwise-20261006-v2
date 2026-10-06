package landing

import (
	"fmt"
	"time"

	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/telemetry"
)

// HeartbeatDetail is the per-station heartbeat view: sequence, age,
// last state transition and the reason of the most recent recovery.
type HeartbeatDetail struct {
	ID                 string       `json:"id"`
	Name               string       `json:"name"`
	State              StationState `json:"state"`
	Overall            string       `json:"overall"`
	Sequence           uint64       `json:"sequence"`
	LastSeenAt         time.Time    `json:"last_seen_at"`
	AgeSeconds         *float64     `json:"age_seconds"`
	TimeoutSeconds     float64      `json:"timeout_seconds"`
	StateChangedAt     time.Time    `json:"state_changed_at"`
	StateAgeSeconds    *float64     `json:"state_age_seconds"`
	StateReason        string       `json:"state_reason,omitempty"`
	LastRecoveryReason string       `json:"last_recovery_reason,omitempty"`
	LastRecoveredAt    time.Time    `json:"last_recovered_at,omitempty"`
	EvaluatedAt        time.Time    `json:"evaluated_at"`
}

type HealthService struct {
	evaluator  *telemetry.AgeEvaluator
	heartbeats *store.HeartbeatStore
}

func NewHealthService(evaluator *telemetry.AgeEvaluator, heartbeats *store.HeartbeatStore) *HealthService {
	return &HealthService{evaluator: evaluator, heartbeats: heartbeats}
}

// Restore rebuilds station heartbeat state from a persisted checkpoint.
func (s *Station) Restore(stored store.HeartbeatSnapshot) StationSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Sequence = stored.Sequence
	s.snapshot.LastSeenAt = stored.SeenAt.UTC()
	s.snapshot.LastAcceptedBeatAt = stored.LastAcceptedBeatAt.UTC()
	s.snapshot.LastRecoveryReason = stored.LastRecoveryReason
	s.snapshot.LastRecoveredAt = stored.LastRecoveredAt.UTC()
	switch StationState(stored.State) {
	case StationOnline, StationStale, StationMaintenance, StationRecovering, StationNeverOnline:
		s.snapshot.State = StationState(stored.State)
	default:
		// Checkpoints written before explicit states existed were always online
		// once a heartbeat had been seen.
		if stored.SeenAt.IsZero() {
			s.snapshot.State = StationNeverOnline
		} else {
			s.snapshot.State = StationOnline
		}
	}
	s.snapshot.StateReason = stored.StateReason
	if s.snapshot.StateReason == "" {
		s.snapshot.StateReason = defaultStateReason(s.snapshot.State)
	}
	if !stored.StateChangedAt.IsZero() {
		s.snapshot.StateChangedAt = stored.StateChangedAt.UTC()
	} else {
		s.snapshot.StateChangedAt = stored.SeenAt.UTC()
	}
	s.snapshot.UpdatedAt = s.snapshot.StateChangedAt
	return s.snapshot
}

func defaultStateReason(state StationState) string {
	switch state {
	case StationNeverOnline:
		return ReasonNeverOnline
	case StationStale:
		return ReasonHeartbeatStale
	case StationMaintenance:
		return ReasonMaintenanceBegin
	case StationRecovering:
		return ReasonRecoveryHeartbeat
	default:
		return ReasonHeartbeatAccepted
	}
}

func heartbeatCheckpoint(snapshot StationSnapshot) store.HeartbeatSnapshot {
	return store.HeartbeatSnapshot{
		StationID:          snapshot.ID,
		SeenAt:             snapshot.LastSeenAt,
		Sequence:           snapshot.Sequence,
		State:              string(snapshot.State),
		StateChangedAt:     snapshot.StateChangedAt,
		StateReason:        snapshot.StateReason,
		LastRecoveryReason: snapshot.LastRecoveryReason,
		LastRecoveredAt:    snapshot.LastRecoveredAt,
		LastAcceptedBeatAt: snapshot.LastAcceptedBeatAt,
	}
}

func (s *HealthService) persist(snapshot StationSnapshot) error {
	if err := s.heartbeats.Record(heartbeatCheckpoint(snapshot)); err != nil {
		return fmt.Errorf("persist landing heartbeat: %w", err)
	}
	return nil
}

// Record accepts an in-order heartbeat. Out-of-order heartbeats are ignored
// and can never bring a station back online.
func (s *HealthService) Record(station *Station, sequence uint64, at time.Time) (StationSnapshot, error) {
	if sequence <= station.Snapshot().Sequence {
		return station.Snapshot(), nil
	}
	snapshot := station.Heartbeat(sequence, at)
	if err := s.persist(snapshot); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

func (s *HealthService) Recover(station *Station) (StationSnapshot, error) {
	stored, found, err := s.heartbeats.Load(station.Snapshot().ID)
	if err != nil {
		return station.Snapshot(), err
	}
	if !found {
		return station.Snapshot(), nil
	}
	return station.Restore(stored), nil
}

// Evaluate applies heartbeat freshness: an online or recovering station
// whose newest heartbeat is past the timeout goes stale. Maintenance and
// never-online stations are left untouched.
func (s *HealthService) Evaluate(station *Station, at time.Time) (HeartbeatDetail, error) {
	snapshot := station.Snapshot()
	if (snapshot.State == StationOnline || snapshot.State == StationRecovering) &&
		!s.evaluator.Healthy(snapshot.LastSeenAt) {
		snapshot = station.MarkStale(at)
		if err := s.persist(snapshot); err != nil {
			return HeartbeatDetail{}, err
		}
	}
	return s.Detail(station, at), nil
}

func (s *HealthService) BeginMaintenance(station *Station, at time.Time) (StationSnapshot, error) {
	snapshot := station.BeginMaintenance(at)
	if err := s.persist(snapshot); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

func (s *HealthService) EndMaintenance(station *Station, reason string, at time.Time) (StationSnapshot, error) {
	snapshot := station.EndMaintenance(reason, at)
	if snapshot.State == StationRecovering && !s.evaluator.Healthy(snapshot.LastSeenAt) {
		snapshot = station.MarkStale(at)
	}
	if err := s.persist(snapshot); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

func (s *HealthService) ConfirmRecovery(station *Station, reason string, at time.Time) (StationSnapshot, error) {
	snapshot, err := station.ConfirmRecovery(reason, at)
	if err != nil {
		return StationSnapshot{}, err
	}
	if err := s.persist(snapshot); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

// Detail renders the heartbeat breakdown without mutating station state.
func (s *HealthService) Detail(station *Station, at time.Time) HeartbeatDetail {
	snapshot := station.Snapshot()
	detail := HeartbeatDetail{
		ID:                 snapshot.ID,
		Name:               snapshot.Name,
		State:              snapshot.State,
		Overall:            overallState(snapshot.State),
		Sequence:           snapshot.Sequence,
		LastSeenAt:         snapshot.LastSeenAt,
		TimeoutSeconds:     s.evaluator.Timeout().Seconds(),
		StateChangedAt:     snapshot.StateChangedAt,
		StateReason:        snapshot.StateReason,
		LastRecoveryReason: snapshot.LastRecoveryReason,
		LastRecoveredAt:    snapshot.LastRecoveredAt,
		EvaluatedAt:        at.UTC(),
	}
	if !snapshot.LastSeenAt.IsZero() {
		age := s.evaluator.Age(snapshot.LastSeenAt).Seconds()
		detail.AgeSeconds = &age
	}
	if !snapshot.StateChangedAt.IsZero() {
		stateAge := at.UTC().Sub(snapshot.StateChangedAt.UTC()).Seconds()
		if stateAge < 0 {
			stateAge = 0
		}
		detail.StateAgeSeconds = &stateAge
	}
	return detail
}

func overallState(state StationState) string {
	switch state {
	case StationOnline:
		return "healthy"
	case StationStale:
		return "unhealthy"
	default:
		return string(state)
	}
}
