package landing

import (
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/telemetry"
)

// Condition explains the station's current health beyond the coarse state.
type Condition string

const (
	ConditionHealthy          Condition = "healthy"
	ConditionNeverOnline      Condition = "never_online"
	ConditionHeartbeatExpired Condition = "heartbeat_expired"
	ConditionMaintenance      Condition = "maintenance"
	ConditionRecovering       Condition = "recovering"
)

// HeartbeatDetail describes the most recent accepted heartbeat.
type HeartbeatDetail struct {
	Sequence   uint64    `json:"sequence"`
	LastSeenAt time.Time `json:"last_seen_at,omitempty"`
	AgeMS      *int64    `json:"age_ms,omitempty"`
	TimeoutMS  int64     `json:"timeout_ms"`
	Expired    bool      `json:"expired"`
	Rejected   uint64    `json:"rejected"`
}

// HealthReport is the full heartbeat breakdown for a landing station.
type HealthReport struct {
	Station      StationSnapshot    `json:"station"`
	Condition    Condition          `json:"condition"`
	Heartbeat    HeartbeatDetail    `json:"heartbeat"`
	Maintenance  *MaintenanceWindow `json:"maintenance,omitempty"`
	LastRecovery *Recovery          `json:"last_recovery,omitempty"`
	Transitions  []StateTransition  `json:"transitions"`
}

type HealthService struct {
	evaluator  *telemetry.AgeEvaluator
	heartbeats *store.HeartbeatStore
}

func NewHealthService(evaluator *telemetry.AgeEvaluator, heartbeats *store.HeartbeatStore) *HealthService {
	return &HealthService{evaluator: evaluator, heartbeats: heartbeats}
}

// Record applies a heartbeat and persists the resulting checkpoint. Stale
// sequences are rejected by the station and skip persistence entirely.
func (s *HealthService) Record(station *Station, sequence uint64, at time.Time) (HealthReport, error) {
	snapshot, accepted := station.Heartbeat(sequence, at)
	if !accepted {
		return s.Report(station), nil
	}
	if err := s.heartbeats.Record(snapshot.ID, snapshot.Sequence, snapshot.LastSeenAt); err != nil {
		return HealthReport{}, fmt.Errorf("persist landing heartbeat: %w", err)
	}
	return s.Report(station), nil
}

// Recover reloads the persisted heartbeat checkpoint into the station.
func (s *HealthService) Recover(station *Station) (StationSnapshot, error) {
	stored, found, err := s.heartbeats.Load(station.Snapshot().ID)
	if err != nil {
		return station.Snapshot(), err
	}
	if !found {
		return station.Snapshot(), nil
	}
	return station.Restore(stored.Sequence, stored.SeenAt), nil
}

// BeginMaintenance parks the station for deliberate maintenance; heartbeats
// keep being tracked but no longer flip the station online.
func (s *HealthService) BeginMaintenance(station *Station, reason string, at time.Time) HealthReport {
	station.BeginMaintenance(reason, at)
	return s.Report(station)
}

// EndMaintenance lifts the maintenance window. The station recovers only if
// its last heartbeat is still fresh; otherwise it stays offline until an
// in-order heartbeat arrives.
func (s *HealthService) EndMaintenance(station *Station, at time.Time) (HealthReport, error) {
	if !station.EndMaintenance(at) {
		return HealthReport{}, errors.New("landing station is not under maintenance")
	}
	if s.evaluator.Healthy(station.Snapshot().LastSeenAt) {
		station.MarkOnline(ReasonMaintenanceCompleted, at)
	}
	return s.Report(station), nil
}

// Evaluate expires a stale online station and returns the current report.
func (s *HealthService) Evaluate(station *Station, at time.Time) HealthReport {
	if _, maintained := station.Maintenance(); !maintained {
		snapshot := station.Snapshot()
		if snapshot.State == StationOnline && !s.evaluator.Healthy(snapshot.LastSeenAt) {
			station.Expire(at)
		}
	}
	return s.Report(station)
}

// Report assembles the heartbeat detail view without mutating the station.
func (s *HealthService) Report(station *Station) HealthReport {
	snapshot := station.Snapshot()
	report := HealthReport{
		Station:     snapshot,
		Condition:   s.condition(station, snapshot),
		Transitions: station.Transitions(),
		Heartbeat: HeartbeatDetail{
			Sequence:   snapshot.Sequence,
			LastSeenAt: snapshot.LastSeenAt,
			TimeoutMS:  s.evaluator.Timeout().Milliseconds(),
			Expired:    !snapshot.LastSeenAt.IsZero() && !s.evaluator.Healthy(snapshot.LastSeenAt),
			Rejected:   station.Rejected(),
		},
	}
	if !snapshot.LastSeenAt.IsZero() {
		age := s.evaluator.Age(snapshot.LastSeenAt).Milliseconds()
		report.Heartbeat.AgeMS = &age
	}
	if window, ok := station.Maintenance(); ok {
		report.Maintenance = &window
	}
	if recovery, ok := station.LastRecovery(); ok {
		report.LastRecovery = &recovery
	}
	return report
}

func (s *HealthService) condition(station *Station, snapshot StationSnapshot) Condition {
	if _, maintained := station.Maintenance(); maintained {
		return ConditionMaintenance
	}
	if snapshot.LastSeenAt.IsZero() {
		return ConditionNeverOnline
	}
	if !s.evaluator.Healthy(snapshot.LastSeenAt) {
		return ConditionHeartbeatExpired
	}
	if recovery, ok := station.LastRecovery(); ok && s.evaluator.Age(recovery.At) <= s.evaluator.Timeout() {
		return ConditionRecovering
	}
	return ConditionHealthy
}
