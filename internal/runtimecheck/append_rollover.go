package runtimecheck

import (
	"context"
	"net/http"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

func runAppendRollover(
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
			"name":            "edge-events",
			"threshold_bytes": 64,
		},
		&created,
	); err != nil {
		return err
	}
	if err := assertions.require(created.Stream != nil, "stream result is missing"); err != nil {
		return err
	}
	if err := assertions.require(created.Segment != nil, "initial segment is missing"); err != nil {
		return err
	}
	if err := assertions.require(
		created.Segment.Range.BaseOffset == 0 && created.Segment.Range.EndOffset == 0,
		"initial range is not empty at zero: %+v",
		created.Segment.Range,
	); err != nil {
		return err
	}

	var first application.AppendRecordsResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams/"+created.Stream.ID+"/records",
		map[string]any{
			"expected_revision": 1,
			"record_count":      3,
			"payload_bytes":     30,
		},
		&first,
	); err != nil {
		return err
	}
	if err := assertions.require(!first.RolledOver, "first batch unexpectedly rolled over"); err != nil {
		return err
	}
	if err := assertions.require(
		first.Segment.Range.EndOffset == 30 && first.Stream.Revision == 2,
		"first append boundary or revision is wrong: end=%d revision=%d",
		first.Segment.Range.EndOffset,
		first.Stream.Revision,
	); err != nil {
		return err
	}

	var second application.AppendRecordsResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams/"+created.Stream.ID+"/records",
		map[string]any{
			"expected_revision": 2,
			"record_count":      4,
			"payload_bytes":     40,
		},
		&second,
	); err != nil {
		return err
	}
	if err := assertions.require(second.RolledOver, "threshold did not trigger rollover"); err != nil {
		return err
	}
	if err := assertions.require(
		second.Stream.Revision == 3,
		"rollover revision is %d, want 3",
		second.Stream.Revision,
	); err != nil {
		return err
	}
	if err := assertions.require(
		second.Segment.ID != created.Segment.ID,
		"rollover kept the same active identifier",
	); err != nil {
		return err
	}
	if err := assertions.require(
		second.Segment.Range.BaseOffset == 70 &&
			second.Segment.Range.EndOffset == 70,
		"new segment does not begin at the previous end: %+v",
		second.Segment.Range,
	); err != nil {
		return err
	}

	segments, err := listSegments(ctx, h, created.Stream.ID)
	if err != nil {
		return err
	}
	if err := assertions.require(len(segments) == 2, "expected two segments, got %d", len(segments)); err != nil {
		return err
	}
	if err := assertions.require(
		segments[0].State == string(domain.SegmentStateSealed) &&
			segments[0].EndOffset == 70,
		"sealed segment boundary or state is wrong: %+v",
		segments[0],
	); err != nil {
		return err
	}

	if err := h.restart(); err != nil {
		return err
	}
	var readBack domain.Stream
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/streams/"+created.Stream.ID,
		nil,
		&readBack,
	); err != nil {
		return err
	}
	return assertions.require(
		readBack.Revision == 3 && readBack.ActiveSegmentID == second.Segment.ID,
		"restart did not preserve revision and active segment: %+v",
		readBack,
	)
}
