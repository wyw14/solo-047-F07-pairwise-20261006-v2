package landing

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/telemetry"
)

type testClock struct {
	now time.Time
}

func (c *testClock) Now() time.Time {
	return c.now
}

func (c *testClock) Advance(by time.Duration) {
	c.now = c.now.Add(by)
}

func newTestHealth(t *testing.T, timeout time.Duration) (*HealthService, *Station, *testClock) {
	t.Helper()
	clock := &testClock{now: time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)}
	evaluator, err := telemetry.NewAgeEvaluator(clock.Now, timeout)
	if err != nil {
		t.Fatal(err)
	}
	checkpoints := store.NewCheckpoints(store.NewSnapshotStore(filepath.Join(t.TempDir(), "snapshots")))
	service := NewHealthService(evaluator, store.NewHeartbeatStore(checkpoints))
	station, err := NewStation("east", "East Landing", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	return service, station, clock
}

func TestStationNeverOnlineUntilFirstHeartbeat(t *testing.T) {
	service, station, clock := newTestHealth(t, 10*time.Second)

	report := service.Evaluate(station, clock.Now())
	if report.Condition != ConditionNeverOnline {
		t.Fatalf("condition=%s, want %s", report.Condition, ConditionNeverOnline)
	}
	if report.Station.State != StationOffline {
		t.Fatalf("state=%s, want %s", report.Station.State, StationOffline)
	}
	if report.Heartbeat.AgeMS != nil {
		t.Fatalf("age should be omitted before the first heartbeat: %+v", report.Heartbeat)
	}

	clock.Advance(time.Minute)
	report = service.Evaluate(station, clock.Now())
	if report.Condition != ConditionNeverOnline {
		t.Fatalf("station without any heartbeat must stay never_online, got %s", report.Condition)
	}
}

func TestHeartbeatLifecycleTransitionsAndRecovery(t *testing.T) {
	service, station, clock := newTestHealth(t, 10*time.Second)

	if _, err := service.Record(station, 1, clock.Now()); err != nil {
		t.Fatal(err)
	}
	report := service.Evaluate(station, clock.Now())
	if report.Condition != ConditionHealthy || report.Station.State != StationOnline {
		t.Fatalf("after first heartbeat: condition=%s state=%s", report.Condition, report.Station.State)
	}
	if report.Heartbeat.AgeMS == nil || *report.Heartbeat.AgeMS != 0 {
		t.Fatalf("age should be zero right after the heartbeat: %+v", report.Heartbeat)
	}

	clock.Advance(11 * time.Second)
	report = service.Evaluate(station, clock.Now())
	if report.Condition != ConditionHeartbeatExpired || report.Station.State != StationOffline {
		t.Fatalf("after timeout: condition=%s state=%s", report.Condition, report.Station.State)
	}
	if !report.Heartbeat.Expired || report.Heartbeat.AgeMS == nil || *report.Heartbeat.AgeMS != 11000 {
		t.Fatalf("heartbeat detail should report expiry: %+v", report.Heartbeat)
	}

	if _, err := service.Record(station, 2, clock.Now()); err != nil {
		t.Fatal(err)
	}
	report = service.Evaluate(station, clock.Now())
	if report.Condition != ConditionRecovering || report.Station.State != StationOnline {
		t.Fatalf("after resume: condition=%s state=%s", report.Condition, report.Station.State)
	}
	if report.LastRecovery == nil || report.LastRecovery.Reason != ReasonHeartbeatResumed {
		t.Fatalf("last recovery should record %q: %+v", ReasonHeartbeatResumed, report.LastRecovery)
	}

	// Once the recovery is older than the timeout and heartbeats keep flowing,
	// the station is fully healthy again.
	clock.Advance(11 * time.Second)
	if _, err := service.Record(station, 3, clock.Now()); err != nil {
		t.Fatal(err)
	}
	report = service.Evaluate(station, clock.Now())
	if report.Condition != ConditionHealthy {
		t.Fatalf("condition=%s, want %s", report.Condition, ConditionHealthy)
	}

	transitions := report.Transitions
	if len(transitions) != 3 {
		t.Fatalf("expected 3 transitions, got %+v", transitions)
	}
	wantReasons := []string{ReasonInitialHeartbeat, ReasonHeartbeatExpired, ReasonHeartbeatResumed}
	for i, want := range wantReasons {
		if transitions[i].Reason != want {
			t.Fatalf("transition %d reason=%s, want %s (all=%+v)", i, transitions[i].Reason, want, transitions)
		}
	}
}

func TestOutOfOrderHeartbeatNeverMarksStationOnline(t *testing.T) {
	service, station, clock := newTestHealth(t, 10*time.Second)

	if _, err := service.Record(station, 5, clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(11 * time.Second)
	report := service.Evaluate(station, clock.Now())
	if report.Station.State != StationOffline {
		t.Fatalf("state=%s, want offline after expiry", report.Station.State)
	}

	// A stale sequence arrives while the station is down.
	if _, err := service.Record(station, 3, clock.Now()); err != nil {
		t.Fatal(err)
	}
	report = service.Evaluate(station, clock.Now())
	if report.Station.State != StationOffline {
		t.Fatalf("out-of-order heartbeat marked station %s", report.Station.State)
	}
	if report.Condition != ConditionHeartbeatExpired {
		t.Fatalf("condition=%s, want %s", report.Condition, ConditionHeartbeatExpired)
	}
	if report.Heartbeat.Sequence != 5 {
		t.Fatalf("sequence moved to %d on stale heartbeat", report.Heartbeat.Sequence)
	}
	if report.Heartbeat.Rejected != 1 {
		t.Fatalf("rejected=%d, want 1", report.Heartbeat.Rejected)
	}
	if len(report.Transitions) != 2 {
		t.Fatalf("stale heartbeat must not add transitions: %+v", report.Transitions)
	}

	// Replaying the last accepted sequence is rejected too.
	if _, err := service.Record(station, 5, clock.Now()); err != nil {
		t.Fatal(err)
	}
	if report := service.Evaluate(station, clock.Now()); report.Heartbeat.Rejected != 2 {
		t.Fatalf("rejected=%d, want 2", report.Heartbeat.Rejected)
	}

	// Only an advancing sequence revives the station.
	if _, err := service.Record(station, 6, clock.Now()); err != nil {
		t.Fatal(err)
	}
	report = service.Evaluate(station, clock.Now())
	if report.Station.State != StationOnline || report.Condition != ConditionRecovering {
		t.Fatalf("state=%s condition=%s after in-order heartbeat", report.Station.State, report.Condition)
	}
}

func TestMaintenanceKeepsStationOfflineUntilCompleted(t *testing.T) {
	service, station, clock := newTestHealth(t, 10*time.Second)

	if _, err := service.Record(station, 1, clock.Now()); err != nil {
		t.Fatal(err)
	}
	report := service.BeginMaintenance(station, "fiber swap", clock.Now())
	if report.Condition != ConditionMaintenance || report.Station.State != StationOffline {
		t.Fatalf("condition=%s state=%s", report.Condition, report.Station.State)
	}
	if report.Maintenance == nil || report.Maintenance.Reason != "fiber swap" {
		t.Fatalf("maintenance window missing: %+v", report.Maintenance)
	}

	// Heartbeats during maintenance advance the sequence but stay offline.
	clock.Advance(time.Second)
	if _, err := service.Record(station, 2, clock.Now()); err != nil {
		t.Fatal(err)
	}
	report = service.Evaluate(station, clock.Now())
	if report.Station.State != StationOffline || report.Condition != ConditionMaintenance {
		t.Fatalf("maintenance heartbeat changed state=%s condition=%s", report.Station.State, report.Condition)
	}
	if report.Heartbeat.Sequence != 2 {
		t.Fatalf("sequence=%d, want 2", report.Heartbeat.Sequence)
	}

	// Expiry must not fire while maintenance is active.
	clock.Advance(time.Minute)
	report = service.Evaluate(station, clock.Now())
	if report.Condition != ConditionMaintenance {
		t.Fatalf("condition=%s, want %s during maintenance", report.Condition, ConditionMaintenance)
	}

	// Completing maintenance with a stale heartbeat stays offline.
	if _, err := service.EndMaintenance(station, clock.Now()); err != nil {
		t.Fatal(err)
	}
	report = service.Evaluate(station, clock.Now())
	if report.Condition != ConditionHeartbeatExpired || report.Station.State != StationOffline {
		t.Fatalf("after stale maintenance end: condition=%s state=%s", report.Condition, report.Station.State)
	}

	// Completing maintenance with a fresh heartbeat recovers with a reason.
	if _, err := service.Record(station, 3, clock.Now()); err != nil {
		t.Fatal(err)
	}
	service.BeginMaintenance(station, "power check", clock.Now())
	report, err := service.EndMaintenance(station, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if report.Condition != ConditionRecovering || report.Station.State != StationOnline {
		t.Fatalf("after fresh maintenance end: condition=%s state=%s", report.Condition, report.Station.State)
	}
	if report.LastRecovery == nil || report.LastRecovery.Reason != ReasonMaintenanceCompleted {
		t.Fatalf("last recovery should be %q: %+v", ReasonMaintenanceCompleted, report.LastRecovery)
	}

	if _, err := service.EndMaintenance(station, clock.Now()); err == nil {
		t.Fatal("ending maintenance twice should fail")
	}
}

func TestRecoverRestoresCheckpointOnRestart(t *testing.T) {
	service, station, clock := newTestHealth(t, 10*time.Second)
	if _, err := service.Record(station, 7, clock.Now()); err != nil {
		t.Fatal(err)
	}

	// Simulate a process restart: a fresh station over the same store.
	restarted, err := NewStation("east", "East Landing", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Recover(restarted)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Sequence != 7 || snapshot.State != StationOnline {
		t.Fatalf("restored snapshot=%+v", snapshot)
	}
	report := service.Evaluate(restarted, clock.Now())
	if report.Condition != ConditionHealthy {
		t.Fatalf("condition=%s after restore, want %s", report.Condition, ConditionHealthy)
	}
}
