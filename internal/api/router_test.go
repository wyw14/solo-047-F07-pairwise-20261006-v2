package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	return testServerWithClock(t, fixedClock{now: time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)})
}

func testServerWithClock(t *testing.T, clock platform.Clock) *Server {
	t.Helper()
	ids := platform.UUIDGenerator{}
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, clock.Now()); err != nil {
		t.Fatal(err)
	}
	target, err := span.New("S3", "S3", 126, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	path, err := route.New("R-S3", "S3", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	drainer := route.NewDrainer(0)
	activator := route.NewActivator(registry)
	receipts := landing.NewReceiptRegistry()
	permitService := repair.NewPermitService(feed.NewIsolationService(), receipts, ids, clock.Now)
	circuit, err := feed.NewCircuit("B", "east", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	permitService.RegisterCircuit(circuit)
	repeaterAsset, err := repeater.New("R07", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := incident.NewRepeaterLifecycle()
	projector := repeater.NewHealthProjector(lifecycle, 85)
	projector.Register(repeaterAsset)
	repository := telemetry.NewRepository()
	acceptance := repair.NewAcceptanceService(switchover.NewRestorer(drainer, activator, registry, ids, clock.Now), clock.Now)
	acceptance.Register("S3", target, path)
	station, err := landing.NewStation("east", "East Landing", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	checkpoints := store.NewCheckpoints(store.NewSnapshotStore(filepath.Join(t.TempDir(), "snapshots")))
	evaluator, err := telemetry.NewAgeEvaluator(clock.Now, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	services := &Services{
		Clock: clock, IDs: ids, Journal: store.NewJournal(filepath.Join(t.TempDir(), "events.jsonl")),
		Spans: map[string]*span.Span{"S3": target}, Routes: map[string]*route.Route{"R-S3": path},
		Repeaters: map[string]*repeater.Repeater{"R07": repeaterAsset}, Topology: registry,
		OTDR:        otdr.NewSessionService(registry, landing.NewScanner(registry, 0), ids, clock.Now),
		Switchovers: switchover.NewSessionService(drainer, activator, ids, clock.Now, time.Second),
		Permits:     permitService, Receipts: receipts, Acceptance: acceptance,
		Telemetry: telemetry.NewIngestor(repository, clock.Now, projector), TelemetryRepository: repository,
		Admission: incident.NewAdmissionService(telemetry.NewMuteManager(clock.Now)),
		Health:    landing.NewHealthService(evaluator, store.NewHeartbeatStore(checkpoints)),
		Station:   station,
	}
	return NewServer(services)
}

type mutableClock struct {
	now time.Time
}

func (c *mutableClock) Now() time.Time {
	return c.now
}

func (c *mutableClock) Advance(by time.Duration) {
	c.now = c.now.Add(by)
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

func TestHealthAndScanHTTPFlow(t *testing.T) {
	server := testServer(t)
	health := httptest.NewRecorder()
	server.Handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}

	body, _ := json.Marshal(ScanRequest{StationID: "east", LengthKM: 126, Chunks: 3})
	scan := httptest.NewRecorder()
	server.Handler().ServeHTTP(scan, httptest.NewRequest(http.MethodPost, "/api/spans/S3/otdr-scans", bytes.NewReader(body)))
	if scan.Code != http.StatusCreated {
		t.Fatalf("scan status=%d body=%s", scan.Code, scan.Body.String())
	}
	var trace otdr.Trace
	if err := json.Unmarshal(scan.Body.Bytes(), &trace); err != nil {
		t.Fatal(err)
	}
	if trace.SpanID != "S3" || len(trace.Chunks) != 3 {
		t.Fatalf("unexpected trace: %+v", trace)
	}

	status := httptest.NewRecorder()
	server.Handler().ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/system/status", nil))
	if status.Code != http.StatusOK {
		t.Fatalf("status code=%d body=%s", status.Code, status.Body.String())
	}
}

func TestLandingHealthHeartbeatDetailFlow(t *testing.T) {
	clock := &mutableClock{now: time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC)}
	server := testServerWithClock(t, clock)

	getHealth := func() landing.HealthReport {
		t.Helper()
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/landings/east/health", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("health status=%d body=%s", response.Code, response.Body.String())
		}
		var report landing.HealthReport
		if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
			t.Fatal(err)
		}
		return report
	}
	post := func(path string, payload any, want int) landing.HealthReport {
		t.Helper()
		body, _ := json.Marshal(payload)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)))
		if response.Code != want {
			t.Fatalf("POST %s status=%d body=%s", path, response.Code, response.Body.String())
		}
		var report landing.HealthReport
		if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
			t.Fatal(err)
		}
		return report
	}

	// Unknown station is rejected.
	missing := httptest.NewRecorder()
	server.Handler().ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/api/landings/west/health", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("unknown station status=%d", missing.Code)
	}

	// No heartbeat yet: never online, no age.
	report := getHealth()
	if report.Condition != landing.ConditionNeverOnline || report.Station.State != landing.StationOffline {
		t.Fatalf("condition=%s state=%s", report.Condition, report.Station.State)
	}
	if report.Heartbeat.AgeMS != nil {
		t.Fatalf("age must be omitted before the first heartbeat: %+v", report.Heartbeat)
	}

	// First heartbeat brings the station online.
	report = post("/api/landings/east/heartbeats", HeartbeatRequest{Sequence: 5}, http.StatusAccepted)
	if report.Condition != landing.ConditionHealthy || report.Heartbeat.Sequence != 5 {
		t.Fatalf("after first heartbeat: %+v", report)
	}

	// Let the heartbeat expire.
	clock.Advance(11 * time.Second)
	report = getHealth()
	if report.Condition != landing.ConditionHeartbeatExpired || report.Station.State != landing.StationOffline {
		t.Fatalf("after expiry: condition=%s state=%s", report.Condition, report.Station.State)
	}
	if report.Heartbeat.AgeMS == nil || *report.Heartbeat.AgeMS != 11000 || !report.Heartbeat.Expired {
		t.Fatalf("heartbeat detail: %+v", report.Heartbeat)
	}

	// An out-of-order heartbeat must not mark the station online again.
	report = post("/api/landings/east/heartbeats", HeartbeatRequest{Sequence: 3}, http.StatusAccepted)
	if report.Station.State != landing.StationOffline || report.Condition != landing.ConditionHeartbeatExpired {
		t.Fatalf("stale heartbeat revived station: condition=%s state=%s", report.Condition, report.Station.State)
	}
	if report.Heartbeat.Sequence != 5 || report.Heartbeat.Rejected != 1 {
		t.Fatalf("stale heartbeat mutated detail: %+v", report.Heartbeat)
	}

	// An advancing heartbeat recovers the station with a reason.
	report = post("/api/landings/east/heartbeats", HeartbeatRequest{Sequence: 6}, http.StatusAccepted)
	if report.Condition != landing.ConditionRecovering || report.Station.State != landing.StationOnline {
		t.Fatalf("after resume: condition=%s state=%s", report.Condition, report.Station.State)
	}
	if report.LastRecovery == nil || report.LastRecovery.Reason != landing.ReasonHeartbeatResumed {
		t.Fatalf("last recovery: %+v", report.LastRecovery)
	}
	if len(report.Transitions) != 3 {
		t.Fatalf("transitions: %+v", report.Transitions)
	}

	// Deliberate maintenance parks the station even with fresh heartbeats.
	report = post("/api/landings/east/maintenance", MaintenanceRequest{Action: "begin", Reason: "fiber swap"}, http.StatusOK)
	if report.Condition != landing.ConditionMaintenance || report.Maintenance == nil || report.Maintenance.Reason != "fiber swap" {
		t.Fatalf("maintenance begin: %+v", report)
	}
	report = post("/api/landings/east/maintenance", MaintenanceRequest{Action: "complete"}, http.StatusOK)
	if report.Condition != landing.ConditionRecovering {
		t.Fatalf("maintenance complete: condition=%s", report.Condition)
	}
	if report.LastRecovery == nil || report.LastRecovery.Reason != landing.ReasonMaintenanceCompleted {
		t.Fatalf("last recovery after maintenance: %+v", report.LastRecovery)
	}

	// Invalid input is rejected.
	post("/api/landings/east/heartbeats", HeartbeatRequest{Sequence: 0}, http.StatusBadRequest)
}
