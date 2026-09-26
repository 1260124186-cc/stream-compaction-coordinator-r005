package runtimecheck

import (
	"context"
	"net/http"
	"sort"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

type planErrorView struct {
	Error struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details,omitempty"`
	} `json:"error"`
}

func runCompactionLineage(
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
			"name":            "lineage-flow",
			"threshold_bytes": 32,
		},
		&created,
	); err != nil {
		return err
	}
	streamID := created.Stream.ID
	for index := 0; index < 8; index++ {
		if err := h.call(
			ctx,
			http.MethodPost,
			"/v1/streams/"+streamID+"/records",
			map[string]any{
				"expected_revision": index + 1,
				"record_count":      2,
				"payload_bytes":     20,
			},
			&application.AppendRecordsResult{},
		); err != nil {
			return err
		}
	}
	segments, err := listSegments(ctx, h, streamID)
	if err != nil {
		return err
	}
	if err := assertions.require(
		len(segments) == 5 &&
			segments[0].State == string(domain.SegmentStateSealed) &&
			segments[1].State == string(domain.SegmentStateSealed) &&
			segments[2].State == string(domain.SegmentStateSealed) &&
			segments[3].State == string(domain.SegmentStateSealed),
		"preparation did not produce four sealed source segments: %+v",
		segments,
	); err != nil {
		return err
	}
	firstSources := []string{segments[0].ID, segments[1].ID}
	secondSources := []string{segments[2].ID, segments[3].ID}

	var firstPlan application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams/"+streamID+"/compaction-plans",
		map[string]any{
			"source_segment_ids":       firstSources,
			"expected_stream_revision": 9,
		},
		&firstPlan,
	); err != nil {
		return err
	}
	if err := assertions.require(
		firstPlan.Plan != nil && firstPlan.Plan.State == domain.PlanStateProposed,
		"first plan is not proposed: %+v",
		firstPlan.Plan,
	); err != nil {
		return err
	}
	firstPlanID := firstPlan.Plan.ID
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+firstPlanID+"/start",
		map[string]any{},
		&firstPlan,
	); err != nil {
		return err
	}
	var firstFinish application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+firstPlanID+"/finish",
		map[string]any{
			"result_checksum":   "sha256:lineage-one",
			"result_size_bytes": 160,
		},
		&firstFinish,
	); err != nil {
		return err
	}
	if err := assertions.require(
		firstFinish.Plan.State == domain.PlanStateCompleted &&
			firstFinish.ResultSegment != nil,
		"first plan did not complete: %+v",
		firstFinish,
	); err != nil {
		return err
	}
	resultID := firstFinish.ResultSegment.ID
	if err := assertions.require(
		len(firstFinish.ResultSegment.SourceSegmentIDs) == 2 &&
			firstFinish.ResultSegment.SourceSegmentIDs[0] == firstSources[0] &&
			firstFinish.ResultSegment.SourceSegmentIDs[1] == firstSources[1],
		"result segment lost its source lineage: %+v",
		firstFinish.ResultSegment.SourceSegmentIDs,
	); err != nil {
		return err
	}

	var repeatedFinish application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+firstPlanID+"/finish",
		map[string]any{
			"result_checksum":   "sha256:lineage-one",
			"result_size_bytes": 160,
		},
		&repeatedFinish,
	); err != nil {
		return err
	}
	if err := assertions.require(
		repeatedFinish.AlreadyFinished &&
			repeatedFinish.ResultSegment.ID == resultID,
		"repeated finish was not idempotent: %+v",
		repeatedFinish,
	); err != nil {
		return err
	}

	rejectedBody := map[string]any{
		"source_segment_ids":       []string{resultID, segments[2].ID},
		"expected_stream_revision": 10,
	}
	var firstRejection planErrorView
	if err := h.callExpect(
		ctx,
		http.MethodPost,
		"/v1/streams/"+streamID+"/compaction-plans",
		rejectedBody,
		http.StatusConflict,
		&firstRejection,
	); err != nil {
		return err
	}
	if err := assertions.require(
		firstRejection.Error.Code == string(domain.CodeInvalidState),
		"compacted result source rejection code is %q, want %q",
		firstRejection.Error.Code,
		domain.CodeInvalidState,
	); err != nil {
		return err
	}
	var secondRejection planErrorView
	if err := h.callExpect(
		ctx,
		http.MethodPost,
		"/v1/streams/"+streamID+"/compaction-plans",
		rejectedBody,
		http.StatusConflict,
		&secondRejection,
	); err != nil {
		return err
	}
	if err := assertions.require(
		secondRejection.Error.Code == firstRejection.Error.Code &&
			secondRejection.Error.Message == firstRejection.Error.Message,
		"repeated rejection is not stable: %+v vs %+v",
		secondRejection.Error,
		firstRejection.Error,
	); err != nil {
		return err
	}

	snapshot, err := h.repo.Load()
	if err != nil {
		return err
	}
	if err := assertions.require(
		len(snapshot.Plans) == 1 && snapshot.Plans[firstPlanID] != nil,
		"rejected attempt left plans behind: %+v",
		sortedPlanIDs(snapshot.Plans),
	); err != nil {
		return err
	}

	var secondPlan application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams/"+streamID+"/compaction-plans",
		map[string]any{
			"source_segment_ids":       secondSources,
			"expected_stream_revision": 10,
		},
		&secondPlan,
	); err != nil {
		return err
	}
	if err := assertions.require(
		secondPlan.Plan != nil && secondPlan.Plan.State == domain.PlanStateProposed,
		"adjacent original segments are not plannable: %+v",
		secondPlan.Plan,
	); err != nil {
		return err
	}
	secondPlanID := secondPlan.Plan.ID
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+secondPlanID+"/start",
		map[string]any{},
		&secondPlan,
	); err != nil {
		return err
	}
	var secondFinish application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+secondPlanID+"/finish",
		map[string]any{
			"result_checksum":   "sha256:lineage-two",
			"result_size_bytes": 150,
		},
		&secondFinish,
	); err != nil {
		return err
	}
	if err := assertions.require(
		secondFinish.Plan.State == domain.PlanStateCompleted &&
			secondFinish.ResultSegment != nil &&
			secondFinish.ResultSegment.Range.BaseOffset == 80 &&
			secondFinish.ResultSegment.Range.EndOffset == 160,
		"second original-range plan did not complete: %+v",
		secondFinish,
	); err != nil {
		return err
	}
	secondResultID := secondFinish.ResultSegment.ID

	if err := h.restart(); err != nil {
		return err
	}

	var recoveredFirst domain.CompactionPlan
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/compaction-plans/"+firstPlanID,
		nil,
		&recoveredFirst,
	); err != nil {
		return err
	}
	if err := assertions.require(
		recoveredFirst.State == domain.PlanStateCompleted &&
			recoveredFirst.ResultSegmentID == resultID,
		"first plan changed after restart: %+v",
		recoveredFirst,
	); err != nil {
		return err
	}
	var recoveredResult domain.Segment
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/segments/"+resultID,
		nil,
		&recoveredResult,
	); err != nil {
		return err
	}
	if err := assertions.require(
		recoveredResult.State == domain.SegmentStateSealed &&
			len(recoveredResult.SourceSegmentIDs) == 2 &&
			recoveredResult.SourceSegmentIDs[0] == firstSources[0] &&
			recoveredResult.SourceSegmentIDs[1] == firstSources[1],
		"result segment changed after restart: %+v",
		recoveredResult,
	); err != nil {
		return err
	}
	var recoveredSecond domain.CompactionPlan
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/compaction-plans/"+secondPlanID,
		nil,
		&recoveredSecond,
	); err != nil {
		return err
	}
	if err := assertions.require(
		recoveredSecond.State == domain.PlanStateCompleted &&
			recoveredSecond.ResultSegmentID == secondResultID,
		"second plan changed after restart: %+v",
		recoveredSecond,
	); err != nil {
		return err
	}

	restored, err := h.repo.Load()
	if err != nil {
		return err
	}
	pending := 0
	for _, plan := range restored.Plans {
		if plan.State == domain.PlanStateProposed || plan.State == domain.PlanStateRunning {
			pending++
		}
	}
	if err := assertions.require(
		len(restored.Plans) == 2 && pending == 0,
		"restart exposed unexpected plans: %+v",
		sortedPlanIDs(restored.Plans),
	); err != nil {
		return err
	}
	return nil
}

func sortedPlanIDs(plans map[string]*domain.CompactionPlan) []string {
	ids := make([]string, 0, len(plans))
	for id := range plans {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
