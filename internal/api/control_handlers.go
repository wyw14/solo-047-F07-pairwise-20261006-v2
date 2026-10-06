package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/repeater"
)

func (s *Server) requireLandingStation(writer http.ResponseWriter, request *http.Request) (*landing.Station, bool) {
	if s.services.Station == nil || chi.URLParam(request, "id") != s.services.Station.Snapshot().ID {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "landing station not found"})
		return nil, false
	}
	return s.services.Station, true
}

func (s *Server) getLandingHealth(writer http.ResponseWriter, request *http.Request) {
	station, ok := s.requireLandingStation(writer, request)
	if !ok {
		return
	}
	detail, err := s.services.Health.Evaluate(station, s.services.Clock.Now())
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, detail)
}

func (s *Server) recordHeartbeat(writer http.ResponseWriter, request *http.Request) {
	station, ok := s.requireLandingStation(writer, request)
	if !ok {
		return
	}
	var input HeartbeatRequest
	if err := decodeJSON(request, &input); err != nil {
		writeJSON(writer, http.StatusBadRequest, err)
		return
	}
	if _, err := s.services.Health.Record(station, input.Sequence, s.services.Clock.Now()); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	detail, err := s.services.Health.Evaluate(station, s.services.Clock.Now())
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, detail)
}

func (s *Server) controlMaintenance(writer http.ResponseWriter, request *http.Request) {
	station, ok := s.requireLandingStation(writer, request)
	if !ok {
		return
	}
	var input MaintenanceRequest
	if err := decodeJSON(request, &input); err != nil {
		writeJSON(writer, http.StatusBadRequest, err)
		return
	}
	var err error
	switch input.Action {
	case "begin":
		_, err = s.services.Health.BeginMaintenance(station, s.services.Clock.Now())
	case "end":
		_, err = s.services.Health.EndMaintenance(station, input.Reason, s.services.Clock.Now())
	default:
		writeJSON(writer, http.StatusBadRequest, ErrorResponse{Error: "maintenance action must be begin or end"})
		return
	}
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, s.services.Health.Detail(station, s.services.Clock.Now()))
}

func (s *Server) confirmRecovery(writer http.ResponseWriter, request *http.Request) {
	station, ok := s.requireLandingStation(writer, request)
	if !ok {
		return
	}
	var input RecoveryRequest
	if err := decodeJSON(request, &input); err != nil {
		writeJSON(writer, http.StatusBadRequest, err)
		return
	}
	if _, err := s.services.Health.ConfirmRecovery(station, input.Reason, s.services.Clock.Now()); err != nil {
		writeJSON(writer, http.StatusConflict, err)
		return
	}
	detail, err := s.services.Health.Evaluate(station, s.services.Clock.Now())
	if err != nil {
		writeJSON(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, detail)
}

func (s *Server) rampRepeater(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Repeater(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input RampRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	result, err := s.services.Ramps.ApplyStep(request.Context(), target, repeater.RampStep{Number: input.Step, Voltage: input.Voltage}, input.SamplesAfter)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"repeater": result, "commands": s.services.Actuator.Commands()})
}

func (s *Server) resetRepeater(writer http.ResponseWriter, request *http.Request) {
	var input ResetRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	if err := s.services.RepeaterHealth.Reset(chi.URLParam(request, "id"), input.Generation); err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	target, _ := s.services.Repeater(chi.URLParam(request, "id"))
	writeJSON(writer, http.StatusOK, target.Snapshot())
}

func (s *Server) admitAlarm(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	var input AlarmRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	alarm := incident.Alarm{ID: s.services.IDs.New("alarm"), SpanID: spanID, Kind: input.Kind, ObservedAt: s.services.Clock.Now()}
	if !s.services.Admission.Accept(alarm) {
		writeJSON(writer, http.StatusAccepted, map[string]any{"admitted": false, "alarm": alarm})
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"admitted": true, "alarm": alarm})
}

func (s *Server) listAlarms(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.services.Admission.Accepted(chi.URLParam(request, "id"))})
}

func (s *Server) startWetPlantTest(writer http.ResponseWriter, request *http.Request) {
	snapshot := s.services.StartWetPlantTest(request.Context(), chi.URLParam(request, "id"))
	writeJSON(writer, http.StatusCreated, snapshot)
}

func (s *Server) finishWetPlantTest(writer http.ResponseWriter, request *http.Request) {
	var input TestRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	snapshot, err := s.services.FinishWetPlantTest(chi.URLParam(request, "id"), input.Action)
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, snapshot)
}

func (s *Server) updateIncident(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	incidentID := chi.URLParam(request, "incidentID")
	var input IncidentRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	current := s.services.Incident(incidentID, spanID)
	switch input.Action {
	case "local-complete":
		s.services.Classifier.LocalComplete(current, input.RequiresPeer, s.services.Clock.Now())
	case "peer-evidence":
		pair := s.services.Correlator.Add(otdr.Evidence{
			IncidentID: incidentID, StationID: input.StationID, TraceID: input.TraceID,
			DistanceKM: input.DistanceKM, ObservedAt: s.services.Clock.Now(),
		})
		_ = s.services.Correlator.Pair(incidentID)
		if pair.Complete || input.StationID != "" {
			s.services.Classifier.PeerEvidence(current, s.services.Clock.Now())
		}
	case "expire":
		s.services.Classifier.Expire(current, s.services.Clock.Now().Add(11*time.Second))
	default:
		writeJSON(writer, http.StatusBadRequest, ErrorResponse{Error: "unsupported incident action"})
		return
	}
	writeJSON(writer, http.StatusOK, current.Snapshot())
}
