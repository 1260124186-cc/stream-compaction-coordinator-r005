package httpapi

import (
	"net/http"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
)

type createPlanRequest struct {
	SourceSegmentIDs       []string `json:"source_segment_ids"`
	ExpectedStreamRevision uint64   `json:"expected_stream_revision"`
}

func (a *API) handleCreateCompactionPlan(writer http.ResponseWriter, request *http.Request) {
	var body createPlanRequest
	if err := decodeJSON(writer, request, a.maxRequestBytes, &body); err != nil {
		writeError(writer, err)
		return
	}
	result, err := a.service.CreateCompactionPlan(
		request.Context(),
		application.CreateCompactionPlanCommand{
			StreamID:               request.PathValue("stream_id"),
			SourceSegmentIDs:       body.SourceSegmentIDs,
			ExpectedStreamRevision: body.ExpectedStreamRevision,
		},
	)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, result)
}

func (a *API) handleStartCompactionPlan(writer http.ResponseWriter, request *http.Request) {
	var body struct{}
	if err := decodeJSON(writer, request, a.maxRequestBytes, &body); err != nil {
		writeError(writer, err)
		return
	}
	result, err := a.service.StartCompactionPlan(
		request.Context(),
		application.StartCompactionPlanCommand{
			PlanID: request.PathValue("plan_id"),
		},
	)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

type finishPlanRequest struct {
	ResultChecksum  string `json:"result_checksum"`
	ResultSizeBytes int64  `json:"result_size_bytes"`
}

func (a *API) handleFinishCompactionPlan(writer http.ResponseWriter, request *http.Request) {
	var body finishPlanRequest
	if err := decodeJSON(writer, request, a.maxRequestBytes, &body); err != nil {
		writeError(writer, err)
		return
	}
	result, err := a.service.FinishCompactionPlan(
		request.Context(),
		application.FinishCompactionPlanCommand{
			PlanID:          request.PathValue("plan_id"),
			ResultChecksum:  body.ResultChecksum,
			ResultSizeBytes: body.ResultSizeBytes,
		},
	)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) handleGetCompactionPlan(writer http.ResponseWriter, request *http.Request) {
	plan, err := a.service.GetCompactionPlan(request.Context(), request.PathValue("plan_id"))
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, plan)
}
