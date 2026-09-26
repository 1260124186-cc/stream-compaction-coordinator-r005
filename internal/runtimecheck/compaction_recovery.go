package runtimecheck

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

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

	// The result segment keeps the first batch of sources as its lineage.
	resultSegmentID := finished.ResultSegment.ID
	var resultSegment domain.Segment
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/segments/"+resultSegmentID,
		nil,
		&resultSegment,
	); err != nil {
		return err
	}
	if err := assertions.require(
		resultSegment.State == domain.SegmentStateSealed &&
			len(resultSegment.SourceSegmentIDs) == len(sourceIDs) &&
			resultSegment.SourceSegmentIDs[0] == sourceIDs[0] &&
			resultSegment.SourceSegmentIDs[1] == sourceIDs[1],
		"result segment lost its source lineage: %+v",
		resultSegment,
	); err != nil {
		return err
	}

	// Grow the stream so two more original sealed segments sit next to the
	// compacted result: [80,120) and [120,160).
	for index, payload := range []int64{20, 20, 20, 20} {
		if err := h.call(
			ctx,
			http.MethodPost,
			"/v1/streams/"+streamID+"/records",
			map[string]any{
				"expected_revision": index + 6,
				"record_count":      2,
				"payload_bytes":     payload,
			},
			&application.AppendRecordsResult{},
		); err != nil {
			return err
		}
	}
	grown, err := listSegments(ctx, h, streamID)
	if err != nil {
		return err
	}
	originals := make([]segmentView, 0, 2)
	for _, segment := range grown {
		if segment.State == string(domain.SegmentStateSealed) && segment.ID != resultSegmentID {
			originals = append(originals, segment)
		}
	}
	if err := assertions.require(
		len(originals) == 2 &&
			originals[0].BaseOffset == 80 && originals[0].EndOffset == 120 &&
			originals[1].BaseOffset == 120 && originals[1].EndOffset == 160,
		"expected two adjacent original sealed segments beyond the result: %+v",
		grown,
	); err != nil {
		return err
	}

	// A plan that reuses the compacted result as a source must be rejected,
	// and the rejection must be stable across identical requests.
	secondAttempt := map[string]any{
		"source_segment_ids":       []string{resultSegmentID, originals[0].ID},
		"expected_stream_revision": 10,
	}
	for attempt := 0; attempt < 2; attempt++ {
		var rejection planErrorEnvelope
		if err := h.callExpect(
			ctx,
			http.MethodPost,
			"/v1/streams/"+streamID+"/compaction-plans",
			secondAttempt,
			http.StatusConflict,
			&rejection,
		); err != nil {
			return err
		}
		if err := assertions.require(
			rejection.Error.Code == string(domain.CodeInvalidState),
			"result-segment plan rejection has code %q, want %q",
			rejection.Error.Code,
			domain.CodeInvalidState,
		); err != nil {
			return err
		}
	}
	var streamAfterRejection domain.Stream
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/streams/"+streamID,
		nil,
		&streamAfterRejection,
	); err != nil {
		return err
	}
	if err := assertions.require(
		streamAfterRejection.Revision == 10,
		"rejected plan attempt mutated the stream: revision %d, want 10",
		streamAfterRejection.Revision,
	); err != nil {
		return err
	}

	// Adjacent original sealed segments still compact in order.
	var followUp application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams/"+streamID+"/compaction-plans",
		map[string]any{
			"source_segment_ids":       []string{originals[0].ID, originals[1].ID},
			"expected_stream_revision": 10,
		},
		&followUp,
	); err != nil {
		return err
	}
	if err := assertions.require(
		followUp.Plan != nil && followUp.Plan.State == domain.PlanStateProposed,
		"follow-up plan over original segments is not proposed: %+v",
		followUp.Plan,
	); err != nil {
		return err
	}
	// The harness identifier generator is a deterministic counter, so the two
	// rejected attempts consumed exactly the two plan identifiers before the
	// follow-up plan. Those identifiers must not resolve to any stored plan.
	rejectedPlanIDs, err := precedingPlanIDs(followUp.Plan.ID, 2)
	if err != nil {
		return err
	}
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+followUp.Plan.ID+"/start",
		map[string]any{},
		&followUp,
	); err != nil {
		return err
	}
	var followUpFinished application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+followUp.Plan.ID+"/finish",
		map[string]any{
			"result_checksum":   "sha256:follow-up",
			"result_size_bytes": 150,
		},
		&followUpFinished,
	); err != nil {
		return err
	}
	if err := assertions.require(
		followUpFinished.Plan.State == domain.PlanStateCompleted &&
			followUpFinished.ResultSegment != nil &&
			followUpFinished.ResultSegment.Range.BaseOffset == 80 &&
			followUpFinished.ResultSegment.Range.EndOffset == 160,
		"follow-up compaction over original segments failed: %+v",
		followUpFinished,
	); err != nil {
		return err
	}
	for _, rejectedID := range rejectedPlanIDs {
		if err := h.callExpect(
			ctx,
			http.MethodGet,
			"/v1/compaction-plans/"+rejectedID,
			nil,
			http.StatusNotFound,
			nil,
		); err != nil {
			return err
		}
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

	// After the restart, the first plan and its result segment keep their
	// state, the rejected attempts left no plans behind, and the repeated
	// completion stays idempotent.
	var firstPlan domain.CompactionPlan
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/compaction-plans/"+planID,
		nil,
		&firstPlan,
	); err != nil {
		return err
	}
	if err := assertions.require(
		firstPlan.State == domain.PlanStateCompleted &&
			firstPlan.ResultSegmentID == resultSegmentID,
		"first plan changed after restart: %+v",
		firstPlan,
	); err != nil {
		return err
	}
	var followUpPlan domain.CompactionPlan
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/compaction-plans/"+followUp.Plan.ID,
		nil,
		&followUpPlan,
	); err != nil {
		return err
	}
	if err := assertions.require(
		followUpPlan.State == domain.PlanStateCompleted,
		"follow-up plan state after restart is %s, want completed",
		followUpPlan.State,
	); err != nil {
		return err
	}
	var resultAfterRestart domain.Segment
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/segments/"+resultSegmentID,
		nil,
		&resultAfterRestart,
	); err != nil {
		return err
	}
	if err := assertions.require(
		resultAfterRestart.State == domain.SegmentStateSealed &&
			len(resultAfterRestart.SourceSegmentIDs) == len(sourceIDs) &&
			resultAfterRestart.SourceSegmentIDs[0] == sourceIDs[0] &&
			resultAfterRestart.SourceSegmentIDs[1] == sourceIDs[1],
		"result segment changed after restart: %+v",
		resultAfterRestart,
	); err != nil {
		return err
	}
	for _, rejectedID := range rejectedPlanIDs {
		if err := h.callExpect(
			ctx,
			http.MethodGet,
			"/v1/compaction-plans/"+rejectedID,
			nil,
			http.StatusNotFound,
			nil,
		); err != nil {
			return err
		}
	}
	var repeatedAfterRestart application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+planID+"/finish",
		map[string]any{
			"result_checksum":   "sha256:compacted",
			"result_size_bytes": 160,
		},
		&repeatedAfterRestart,
	); err != nil {
		return err
	}
	if err := assertions.require(
		repeatedAfterRestart.AlreadyFinished &&
			repeatedAfterRestart.ResultSegment.ID == resultSegmentID,
		"finish after restart was not idempotent: %+v",
		repeatedAfterRestart,
	); err != nil {
		return err
	}
	return nil
}

type planErrorEnvelope struct {
	Error struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details,omitempty"`
	} `json:"error"`
}

func precedingPlanIDs(id string, count int) ([]string, error) {
	separator := strings.LastIndex(id, "_")
	if separator < 0 {
		return nil, fmt.Errorf("plan identifier %q has no numeric suffix", id)
	}
	value, err := strconv.Atoi(id[separator+1:])
	if err != nil {
		return nil, fmt.Errorf("plan identifier %q has no numeric suffix: %w", id, err)
	}
	if value-count < 0 {
		return nil, fmt.Errorf("plan identifier %q has no %d predecessors", id, count)
	}
	result := make([]string, 0, count)
	for offset := count; offset >= 1; offset-- {
		result = append(result, fmt.Sprintf("%s%06d", id[:separator+1], value-offset))
	}
	return result, nil
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
