package landing

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/telemetry"
)

type mutableClock struct{ now time.Time }

func (c *mutableClock) Now() time.Time { return c.now }

func (c *mutableClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newTestHealthService(t *testing.T) (*HealthService, *Station, *mutableClock) {
	t.Helper()
	clock := &mutableClock{now: time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC)}
	snapshots := store.NewSnapshotStore(filepath.Join(t.TempDir(), "snapshots"))
	heartbeats := store.NewHeartbeatStore(store.NewCheckpoints(snapshots))
	evaluator, err := telemetry.NewAgeEvaluator(clock.Now, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	station, err := NewStation("east", "East Landing", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	return NewHealthService(evaluator, heartbeats), station, clock
}

func TestNewStationIsNeverOnline(t *testing.T) {
	service, station, clock := newTestHealthService(t)
	detail := service.Detail(station, clock.Now())
	if detail.State != StationNeverOnline {
		t.Fatalf("new station state = %s, want never_online", detail.State)
	}
	if detail.Overall != "never_online" || detail.Sequence != 0 {
		t.Fatalf("unexpected detail: %+v", detail)
	}
	if detail.AgeSeconds != nil {
		t.Fatalf("never-online station must not report a heartbeat age, got %v", *detail.AgeSeconds)
	}
	if detail.StateReason != ReasonNeverOnline {
		t.Fatalf("reason = %q, want %q", detail.StateReason, ReasonNeverOnline)
	}
}

func TestFirstHeartbeatComesOnline(t *testing.T) {
	service, station, clock := newTestHealthService(t)
	snapshot, err := service.Record(station, 1, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != StationOnline || snapshot.StateReason != ReasonFirstContact {
		t.Fatalf("snapshot = %+v, want online/first_contact", snapshot)
	}
	detail := service.Detail(station, clock.Now())
	if detail.Overall != "healthy" || detail.AgeSeconds == nil || *detail.AgeSeconds != 0 {
		t.Fatalf("unexpected detail: %+v", detail)
	}
}

func TestStaleThenOutdatedHeartbeatStaysOffline(t *testing.T) {
	service, station, clock := newTestHealthService(t)
	if _, err := service.Record(station, 5, clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(11 * time.Second)
	detail, err := service.Evaluate(station, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if detail.State != StationStale || detail.Overall != "unhealthy" {
		t.Fatalf("state = %s/%s, want stale/unhealthy", detail.State, detail.Overall)
	}

	// Equal and lower sequence numbers must never resurrect the station.
	for _, sequence := range []uint64{5, 4} {
		if _, err := service.Record(station, sequence, clock.Now()); err != nil {
			t.Fatal(err)
		}
		detail = service.Detail(station, clock.Now())
		if detail.State != StationStale {
			t.Fatalf("out-of-order heartbeat seq=%d produced state %s, want stale", sequence, detail.State)
		}
		if detail.Sequence != 5 {
			t.Fatalf("sequence regressed to %d", detail.Sequence)
		}
	}
}

func TestRecoveryRequiresConfirmation(t *testing.T) {
	service, station, clock := newTestHealthService(t)
	if _, err := service.Record(station, 1, clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(11 * time.Second)
	if _, err := service.Evaluate(station, clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if _, err := service.Record(station, 2, clock.Now()); err != nil {
		t.Fatal(err)
	}
	detail := service.Detail(station, clock.Now())
	if detail.State != StationRecovering || detail.StateReason != ReasonRecoveryHeartbeat {
		t.Fatalf("fresh post-stale heartbeat state = %s, want recovering", detail.State)
	}

	if _, err := service.ConfirmRecovery(station, "fibre-spliced", clock.Now()); err != nil {
		t.Fatal(err)
	}
	detail = service.Detail(station, clock.Now())
	if detail.State != StationOnline || detail.LastRecoveryReason != "fibre-spliced" {
		t.Fatalf("confirmed detail = %+v, want online with recovery reason", detail)
	}

	// Confirming again is rejected: the station is no longer awaiting recovery.
	if _, err := service.ConfirmRecovery(station, "again", clock.Now()); err == nil {
		t.Fatal("expected error confirming recovery while online")
	}
}

func TestMaintenanceSuppressesHeartbeats(t *testing.T) {
	service, station, clock := newTestHealthService(t)
	if _, err := service.Record(station, 1, clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.BeginMaintenance(station, clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(30 * time.Second)
	if _, err := service.Record(station, 2, clock.Now()); err != nil {
		t.Fatal(err)
	}
	detail, err := service.Evaluate(station, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if detail.State != StationMaintenance || detail.Sequence != 2 {
		t.Fatalf("heartbeat during maintenance changed state: %+v", detail)
	}

	if _, err := service.EndMaintenance(station, "window-closed", clock.Now()); err != nil {
		t.Fatal(err)
	}
	detail = service.Detail(station, clock.Now())
	if detail.State != StationRecovering || detail.LastRecoveryReason != "window-closed" {
		t.Fatalf("after maintenance detail = %+v, want recovering with reason", detail)
	}
}

func TestEndMaintenanceBeforeAnyHeartbeatStaysNeverOnline(t *testing.T) {
	service, station, clock := newTestHealthService(t)
	if _, err := service.BeginMaintenance(station, clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EndMaintenance(station, "", clock.Now()); err != nil {
		t.Fatal(err)
	}
	detail := service.Detail(station, clock.Now())
	if detail.State != StationNeverOnline {
		t.Fatalf("state = %s, want never_online", detail.State)
	}
}

func TestEndMaintenanceAfterFreshHeartbeatRequiresConfirmation(t *testing.T) {
	service, station, clock := newTestHealthService(t)
	if _, err := service.BeginMaintenance(station, clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Second)
	if _, err := service.Record(station, 3, clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EndMaintenance(station, "window-closed", clock.Now()); err != nil {
		t.Fatal(err)
	}
	detail := service.Detail(station, clock.Now())
	if detail.State != StationRecovering || detail.LastRecoveryReason != "window-closed" {
		t.Fatalf("fresh post-maintenance detail = %+v, want recovering", detail)
	}
}

func TestEndMaintenanceWithStaleHeartbeatGoesStale(t *testing.T) {
	service, station, clock := newTestHealthService(t)
	if _, err := service.BeginMaintenance(station, clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(20 * time.Second)
	if _, err := service.Record(station, 3, clock.Now().Add(-15*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EndMaintenance(station, "window-closed", clock.Now()); err != nil {
		t.Fatal(err)
	}
	detail := service.Detail(station, clock.Now())
	if detail.State != StationStale {
		t.Fatalf("stale post-maintenance detail = %+v, want stale", detail)
	}
}

func TestRecoveringHeartbeatsDoNotBypassConfirmation(t *testing.T) {
	service, station, clock := newTestHealthService(t)
	if _, err := service.Record(station, 1, clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(11 * time.Second)
	if _, err := service.Evaluate(station, clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Record(station, 2, clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if _, err := service.Record(station, 3, clock.Now()); err != nil {
		t.Fatal(err)
	}
	detail := service.Detail(station, clock.Now())
	if detail.State != StationRecovering || detail.Sequence != 3 {
		t.Fatalf("heartbeat while recovering state = %s seq = %d, want recovering/3", detail.State, detail.Sequence)
	}
}

func TestHeartbeatCheckpointRoundTrip(t *testing.T) {
	service, station, clock := newTestHealthService(t)
	if _, err := service.Record(station, 1, clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(11 * time.Second)
	if _, err := service.Evaluate(station, clock.Now()); err != nil {
		t.Fatal(err)
	}

	rebuilt, err := NewStation("east", "East Landing", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Recover(rebuilt)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != StationStale || snapshot.Sequence != 1 || snapshot.StateReason != ReasonHeartbeatStale {
		t.Fatalf("recovered snapshot = %+v, want stale seq 1", snapshot)
	}
	if !snapshot.StateChangedAt.Equal(clock.Now()) {
		t.Fatalf("state changed at = %s, want %s", snapshot.StateChangedAt, clock.Now())
	}
}
