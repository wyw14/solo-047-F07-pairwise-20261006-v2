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
	clock := fixedClock{now: time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)}
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
	services := &Services{
		Clock: clock, IDs: ids, Journal: store.NewJournal(filepath.Join(t.TempDir(), "events.jsonl")),
		Spans: map[string]*span.Span{"S3": target}, Routes: map[string]*route.Route{"R-S3": path},
		Repeaters: map[string]*repeater.Repeater{"R07": repeaterAsset}, Topology: registry,
		OTDR:        otdr.NewSessionService(registry, landing.NewScanner(registry, 0), ids, clock.Now),
		Switchovers: switchover.NewSessionService(drainer, activator, ids, clock.Now, time.Second),
		Permits:     permitService, Receipts: receipts, Acceptance: acceptance,
		Telemetry: telemetry.NewIngestor(repository, clock.Now, projector), TelemetryRepository: repository,
		Admission: incident.NewAdmissionService(telemetry.NewMuteManager(clock.Now)),
	}
	return NewServer(services)
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

type mutableClock struct{ now time.Time }

func (c *mutableClock) Now() time.Time { return c.now }

func (c *mutableClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func landingHealthServer(t *testing.T) (*Server, *mutableClock) {
	t.Helper()
	clock := &mutableClock{now: time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC)}
	snapshots := store.NewSnapshotStore(filepath.Join(t.TempDir(), "snapshots"))
	heartbeats := store.NewHeartbeatStore(store.NewCheckpoints(snapshots))
	evaluator, err := telemetry.NewAgeEvaluator(clock.Now, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	station, err := landing.NewStation("east", "East Landing", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	services := &Services{
		Clock:   clock,
		Station: station,
		Health:  landing.NewHealthService(evaluator, heartbeats),
	}
	return NewServer(services), clock
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

func TestLandingHeartbeatLifecycleHTTP(t *testing.T) {
	server, clock := landingHealthServer(t)
	serve := func(method, path string, body any) *httptest.ResponseRecorder {
		var reader *bytes.Reader
		if body != nil {
			raw, _ := json.Marshal(body)
			reader = bytes.NewReader(raw)
		} else {
			reader = bytes.NewReader(nil)
		}
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(method, path, reader))
		return response
	}

	health := serve(http.MethodGet, "/api/landings/east/health", nil)
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}
	var detail landing.HeartbeatDetail
	if err := json.Unmarshal(health.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.State != landing.StationNeverOnline || detail.AgeSeconds != nil {
		t.Fatalf("unexpected initial detail: %+v", detail)
	}

	beat := serve(http.MethodPost, "/api/landings/east/heartbeats", HeartbeatRequest{Sequence: 7})
	if beat.Code != http.StatusOK {
		t.Fatalf("heartbeat status=%d body=%s", beat.Code, beat.Body.String())
	}
	if err := json.Unmarshal(beat.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.State != landing.StationOnline || detail.Sequence != 7 {
		t.Fatalf("unexpected detail after heartbeat: %+v", detail)
	}
	if detail.StateChangedAt.IsZero() || detail.StateReason != landing.ReasonFirstContact {
		t.Fatalf("missing state transition metadata: %+v", detail)
	}

	clock.Advance(11 * time.Second)
	stale := serve(http.MethodGet, "/api/landings/east/health", nil)
	if stale.Code != http.StatusOK || json.Unmarshal(stale.Body.Bytes(), &detail) != nil || detail.State != landing.StationStale {
		t.Fatalf("expected stale, got %d %s", stale.Code, stale.Body.String())
	}
	if detail.AgeSeconds == nil || *detail.AgeSeconds < 11 {
		t.Fatalf("expected age >= 11s, got %+v", detail.AgeSeconds)
	}

	// Out-of-order heartbeat must not mark the station online.
	replay := serve(http.MethodPost, "/api/landings/east/heartbeats", HeartbeatRequest{Sequence: 7})
	if replay.Code != http.StatusOK || json.Unmarshal(replay.Body.Bytes(), &detail) != nil || detail.State != landing.StationStale {
		t.Fatalf("replayed heartbeat resurrected station: %d %s", replay.Code, replay.Body.String())
	}

	// Fresh in-order heartbeat enters recovering, not online.
	next := serve(http.MethodPost, "/api/landings/east/heartbeats", HeartbeatRequest{Sequence: 8})
	if next.Code != http.StatusOK || json.Unmarshal(next.Body.Bytes(), &detail) != nil || detail.State != landing.StationRecovering {
		t.Fatalf("expected recovering, got %d %s", next.Code, next.Body.String())
	}

	confirm := serve(http.MethodPost, "/api/landings/east/recovery/confirm", RecoveryRequest{Reason: "splice-verified"})
	if confirm.Code != http.StatusOK || json.Unmarshal(confirm.Body.Bytes(), &detail) != nil {
		t.Fatalf("confirm failed: %d %s", confirm.Code, confirm.Body.String())
	}
	if detail.State != landing.StationOnline || detail.LastRecoveryReason != "splice-verified" {
		t.Fatalf("unexpected confirmed detail: %+v", detail)
	}
}

func TestLandingMaintenanceWindowHTTP(t *testing.T) {
	server, _ := landingHealthServer(t)
	post := func(path string, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw)))
		return response
	}

	begin := post("/api/landings/east/maintenance", MaintenanceRequest{Action: "begin"})
	if begin.Code != http.StatusOK {
		t.Fatalf("begin status=%d body=%s", begin.Code, begin.Body.String())
	}
	var detail landing.HeartbeatDetail
	if err := json.Unmarshal(begin.Body.Bytes(), &detail); err != nil || detail.State != landing.StationMaintenance {
		t.Fatalf("unexpected maintenance detail: %s", begin.Body.String())
	}

	invalid := post("/api/landings/east/maintenance", MaintenanceRequest{Action: "pause"})
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("bad action status=%d, want 400", invalid.Code)
	}

	// A recovering state is required before recovery confirmation.
	confirm := post("/api/landings/east/recovery/confirm", RecoveryRequest{})
	if confirm.Code != http.StatusConflict {
		t.Fatalf("confirm under maintenance status=%d, want 409", confirm.Code)
	}

	end := post("/api/landings/east/maintenance", MaintenanceRequest{Action: "end", Reason: "window-done"})
	if end.Code != http.StatusOK || json.Unmarshal(end.Body.Bytes(), &detail) != nil {
		t.Fatalf("end failed: %d %s", end.Code, end.Body.String())
	}
	if detail.State != landing.StationNeverOnline || detail.LastRecoveryReason != "window-done" {
		t.Fatalf("after maintenance detail = %+v, want never_online with recovery reason", detail)
	}

	if response := post("/api/landings/west/maintenance", MaintenanceRequest{Action: "begin"}); response.Code != http.StatusNotFound {
		t.Fatalf("unknown station status=%d, want 404", response.Code)
	}
}
