package runtimecheck

import (
	"context"
	"net/http"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

func runCompactionRecovery(
	ctx context.Context,
	assertions *counterAssertions,
) error {
	h, err := newHarness()
	if err != nil {
		return err
	}
	defer h.close()

	streamID, sourceIDs, err := prepareTwoSealedSegments(ctx, h, "compaction-flow")
	if err != nil {
		return err
	}

	var planResult application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams/"+streamID+"/compaction-plans",
		map[string]any{
			"source_segment_ids":       sourceIDs,
			"expected_stream_revision": 5,
		},
		&planResult,
	); err != nil {
		return err
	}
	if err := assertions.require(
		planResult.Plan != nil && planResult.Plan.State == domain.PlanStateProposed,
		"created plan is not proposed: %+v",
		planResult.Plan,
	); err != nil {
		return err
	}
	planID := planResult.Plan.ID

	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+planID+"/start",
		map[string]any{},
		&planResult,
	); err != nil {
		return err
	}
	if err := assertions.require(
		planResult.Plan.State == domain.PlanStateRunning,
		"started plan is not running: %+v",
		planResult.Plan,
	); err != nil {
		return err
	}

	var finished application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+planID+"/finish",
		map[string]any{
			"result_checksum":   "sha256:compacted",
			"result_size_bytes": 160,
		},
		&finished,
	); err != nil {
		return err
	}
	if err := assertions.require(
		finished.Plan.State == domain.PlanStateCompleted &&
			finished.ResultSegment != nil,
		"finished plan is incomplete: %+v",
		finished,
	); err != nil {
		return err
	}
	if err := assertions.require(
		finished.ResultSegment.Range.BaseOffset == 0 &&
			finished.ResultSegment.Range.EndOffset == 80,
		"compacted result range is wrong: %+v",
		finished.ResultSegment.Range,
	); err != nil {
		return err
	}

	var repeated application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+planID+"/finish",
		map[string]any{
			"result_checksum":   "sha256:compacted",
			"result_size_bytes": 160,
		},
		&repeated,
	); err != nil {
		return err
	}
	if err := assertions.require(
		repeated.AlreadyFinished &&
			repeated.ResultSegment.ID == finished.ResultSegment.ID,
		"repeated finish was not idempotent: %+v",
		repeated,
	); err != nil {
		return err
	}

	var secondStream application.CreateStreamResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams",
		map[string]any{
			"namespace":       "field-lab",
			"name":            "recovery-flow",
			"threshold_bytes": 16,
		},
		&secondStream,
	); err != nil {
		return err
	}
	for index, payload := range []int64{8, 8, 8, 8} {
		if err := h.call(
			ctx,
			http.MethodPost,
			"/v1/streams/"+secondStream.Stream.ID+"/records",
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
	recoverySegments, err := listSegments(ctx, h, secondStream.Stream.ID)
	if err != nil {
		return err
	}
	if err := assertions.require(
		len(recoverySegments) == 3,
		"recovery stream expected three segments, got %d",
		len(recoverySegments),
	); err != nil {
		return err
	}
	recoverySources := []string{recoverySegments[0].ID, recoverySegments[1].ID}
	var recoveryPlan application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams/"+secondStream.Stream.ID+"/compaction-plans",
		map[string]any{
			"source_segment_ids":       recoverySources,
			"expected_stream_revision": 5,
		},
		&recoveryPlan,
	); err != nil {
		return err
	}
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+recoveryPlan.Plan.ID+"/start",
		map[string]any{},
		&recoveryPlan,
	); err != nil {
		return err
	}

	if err := h.restart(); err != nil {
		return err
	}
	var recoveredPlan domain.CompactionPlan
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/compaction-plans/"+recoveryPlan.Plan.ID,
		nil,
		&recoveredPlan,
	); err != nil {
		return err
	}
	if err := assertions.require(
		recoveredPlan.State == domain.PlanStateAborted,
		"interrupted plan state is %s, want aborted",
		recoveredPlan.State,
	); err != nil {
		return err
	}
	for _, segmentID := range recoverySources {
		var segment domain.Segment
		if err := h.call(
			ctx,
			http.MethodGet,
			"/v1/segments/"+segmentID,
			nil,
			&segment,
		); err != nil {
			return err
		}
		if err := assertions.require(
			segment.State == domain.SegmentStateSealed,
			"recovered source %s state is %s, want sealed",
			segment.ID,
			segment.State,
		); err != nil {
			return err
		}
	}
	return nil
}

func prepareTwoSealedSegments(
	ctx context.Context,
	h *harness,
	name string,
) (string, []string, error) {
	var created application.CreateStreamResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams",
		map[string]any{
			"namespace":       "field-lab",
			"name":            name,
			"threshold_bytes": 32,
		},
		&created,
	); err != nil {
		return "", nil, err
	}
	for index, payload := range []int64{20, 20, 20, 20} {
		if err := h.call(
			ctx,
			http.MethodPost,
			"/v1/streams/"+created.Stream.ID+"/records",
			map[string]any{
				"expected_revision": index + 1,
				"record_count":      2,
				"payload_bytes":     payload,
			},
			&application.AppendRecordsResult{},
		); err != nil {
			return "", nil, err
		}
	}
	segments, err := listSegments(ctx, h, created.Stream.ID)
	if err != nil {
		return "", nil, err
	}
	if len(segments) < 3 ||
		segments[0].State != string(domain.SegmentStateSealed) ||
		segments[1].State != string(domain.SegmentStateSealed) {
		return "", nil, domain.NewError(
			domain.CodeInconsistentState,
			"preparation did not produce two sealed source segments",
		)
	}
	return created.Stream.ID, []string{segments[0].ID, segments[1].ID}, nil
}
