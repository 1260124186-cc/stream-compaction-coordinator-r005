package httpapi

import (
	"net/http"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
)

type replicaAckRequest struct {
	NodeID     string `json:"node_id"`
	Generation uint64 `json:"generation"`
	Offset     int64  `json:"offset"`
}

func (a *API) handleAcknowledgeReplica(writer http.ResponseWriter, request *http.Request) {
	var body replicaAckRequest
	if err := decodeJSON(writer, request, a.maxRequestBytes, &body); err != nil {
		writeError(writer, err)
		return
	}
	result, err := a.service.AcknowledgeReplica(
		request.Context(),
		application.AcknowledgeReplicaCommand{
			SegmentID:  request.PathValue("segment_id"),
			NodeID:     body.NodeID,
			Generation: body.Generation,
			Offset:     body.Offset,
		},
	)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) handleGetSegment(writer http.ResponseWriter, request *http.Request) {
	segment, err := a.service.GetSegment(request.Context(), request.PathValue("segment_id"))
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, segment)
}

func (a *API) handleRecover(writer http.ResponseWriter, request *http.Request) {
	var body struct{}
	if err := decodeJSON(writer, request, a.maxRequestBytes, &body); err != nil {
		writeError(writer, err)
		return
	}
	report, err := a.service.Recover(request.Context())
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, report)
}

func (a *API) handleHealth(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{
		"status": "ok",
	})
}
