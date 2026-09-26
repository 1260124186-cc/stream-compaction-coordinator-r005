package runtimecheck

import (
	"context"
	"net/http"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

func runReplicaWatermark(
	ctx context.Context,
	assertions *counterAssertions,
) error {
	h, err := newHarness()
	if err != nil {
		return err
	}
	defer h.close()

	var created application.CreateStreamResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams",
		map[string]any{
			"namespace":       "field-lab",
			"name":            "replica-flow",
			"threshold_bytes": 30,
		},
		&created,
	); err != nil {
		return err
	}
	for index, payload := range []int64{10, 20, 10} {
		if err := h.call(
			ctx,
			http.MethodPost,
			"/v1/streams/"+created.Stream.ID+"/records",
			map[string]any{
				"expected_revision": index + 1,
				"record_count":      1,
				"payload_bytes":     payload,
			},
			&application.AppendRecordsResult{},
		); err != nil {
			return err
		}
	}
	segments, err := listSegments(ctx, h, created.Stream.ID)
	if err != nil {
		return err
	}
	if err := assertions.require(
		len(segments) == 2 &&
			segments[0].State == string(domain.SegmentStateSealed),
		"replica setup did not seal the first segment: %+v",
		segments,
	); err != nil {
		return err
	}
	sealed := segments[0]
	active := segments[1]

	var acknowledged application.AcknowledgeReplicaResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/segments/"+sealed.ID+"/replica-acks",
		map[string]any{
			"node_id":    "edge-a",
			"generation": sealed.Generation,
			"offset":     sealed.EndOffset,
		},
		&acknowledged,
	); err != nil {
		return err
	}
	if err := assertions.require(
		acknowledged.Changed &&
			acknowledged.Watermark == 30,
		"first acknowledgement did not advance the watermark: %+v",
		acknowledged,
	); err != nil {
		return err
	}

	var repeated application.AcknowledgeReplicaResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/segments/"+sealed.ID+"/replica-acks",
		map[string]any{
			"node_id":    "edge-a",
			"generation": sealed.Generation,
			"offset":     sealed.EndOffset,
		},
		&repeated,
	); err != nil {
		return err
	}
	if err := assertions.require(
		!repeated.Changed,
		"same acknowledgement was not idempotent",
	); err != nil {
		return err
	}

	var conflict errorBodyEnvelope
	if err := h.callExpect(
		ctx,
		http.MethodPost,
		"/v1/segments/"+sealed.ID+"/replica-acks",
		map[string]any{
			"node_id":    "edge-a",
			"generation": sealed.Generation,
			"offset":     10,
		},
		http.StatusConflict,
		&conflict,
	); err != nil {
		return err
	}
	if err := assertions.require(
		conflict.Error.Code == string(domain.CodeConflict),
		"decreasing acknowledgement error code is %q",
		conflict.Error.Code,
	); err != nil {
		return err
	}

	var activeAck application.AcknowledgeReplicaResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/segments/"+active.ID+"/replica-acks",
		map[string]any{
			"node_id":    "edge-b",
			"generation": active.Generation,
			"offset":     active.EndOffset,
		},
		&activeAck,
	); err != nil {
		return err
	}
	if err := assertions.require(
		activeAck.Watermark == 30,
		"open segment acknowledgement changed the sealed watermark: %+v",
		activeAck,
	); err != nil {
		return err
	}

	if err := h.restart(); err != nil {
		return err
	}
	var watermark application.WatermarkResult
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/streams/"+created.Stream.ID+"/watermark",
		nil,
		&watermark,
	); err != nil {
		return err
	}
	return assertions.require(
		watermark.Watermark == 30,
		"restart changed the watermark to %d",
		watermark.Watermark,
	)
}

type errorBodyEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}
