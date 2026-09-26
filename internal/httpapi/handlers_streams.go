package httpapi

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

type createStreamRequest struct {
	Namespace      string `json:"namespace"`
	Name           string `json:"name"`
	ThresholdBytes int64  `json:"threshold_bytes"`
}

func (a *API) handleCreateStream(writer http.ResponseWriter, request *http.Request) {
	var body createStreamRequest
	if err := decodeJSON(writer, request, a.maxRequestBytes, &body); err != nil {
		writeError(writer, err)
		return
	}
	result, err := a.service.CreateStream(request.Context(), application.CreateStreamCommand{
		Namespace:      body.Namespace,
		Name:           body.Name,
		ThresholdBytes: body.ThresholdBytes,
	})
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, result)
}

func (a *API) handleGetStream(writer http.ResponseWriter, request *http.Request) {
	stream, err := a.service.GetStream(request.Context(), request.PathValue("stream_id"))
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, stream)
}

type appendRecordsRequest struct {
	ExpectedRevision uint64 `json:"expected_revision"`
	RecordCount      uint64 `json:"record_count"`
	PayloadBytes     int64  `json:"payload_bytes"`
}

func (a *API) handleAppendRecords(writer http.ResponseWriter, request *http.Request) {
	var body appendRecordsRequest
	if err := decodeJSON(writer, request, a.maxRequestBytes, &body); err != nil {
		writeError(writer, err)
		return
	}
	result, err := a.service.AppendRecords(request.Context(), application.AppendRecordsCommand{
		StreamID:         request.PathValue("stream_id"),
		ExpectedRevision: body.ExpectedRevision,
		RecordCount:      body.RecordCount,
		PayloadBytes:     body.PayloadBytes,
	})
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

type segmentListResponse struct {
	Items []*domain.Segment `json:"items"`
}

func (a *API) handleListSegments(writer http.ResponseWriter, request *http.Request) {
	states := map[domain.SegmentState]bool{}
	for _, raw := range strings.Split(request.URL.Query().Get("states"), ",") {
		value := domain.SegmentState(strings.TrimSpace(raw))
		if value == "" {
			continue
		}
		switch value {
		case domain.SegmentStateOpen, domain.SegmentStateSealed,
			domain.SegmentStateCompacting, domain.SegmentStateRetired:
			states[value] = true
		default:
			writeError(writer, domain.NewError(domain.CodeInvalidInput, "unknown segment state").
				WithDetail("state", value))
			return
		}
	}
	limit, err := parseLimit(request.URL.Query().Get("limit"))
	if err != nil {
		writeError(writer, err)
		return
	}
	segments, err := a.service.ListSegments(request.Context(), storage.SegmentFilter{
		StreamID: request.PathValue("stream_id"),
		States:   states,
		AfterID:  strings.TrimSpace(request.URL.Query().Get("after")),
		Limit:    limit,
	})
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, segmentListResponse{Items: segments})
}

func (a *API) handleWatermark(writer http.ResponseWriter, request *http.Request) {
	result, err := a.service.GetWatermark(request.Context(), request.PathValue("stream_id"))
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func parseLimit(raw string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return 100, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > 1000 {
		return 0, domain.NewError(domain.CodeInvalidInput, "limit must be between 1 and 1000")
	}
	return value, nil
}
