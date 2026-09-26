package application

import (
	"context"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

func (s *Service) GetStream(ctx context.Context, streamID string) (*domain.Stream, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	state := s.readState()
	stream := storage.FindStream(state, normalizedIdentifier(streamID))
	if stream == nil {
		return nil, resourceNotFound("stream", streamID)
	}
	return stream, nil
}

func (s *Service) GetSegment(ctx context.Context, segmentID string) (*domain.Segment, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	state := s.readState()
	segment := storage.FindSegment(state, normalizedIdentifier(segmentID))
	if segment == nil {
		return nil, resourceNotFound("segment", segmentID)
	}
	return segment, nil
}

func (s *Service) GetCompactionPlan(
	ctx context.Context,
	planID string,
) (*domain.CompactionPlan, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	state := s.readState()
	plan := storage.FindPlan(state, normalizedIdentifier(planID))
	if plan == nil {
		return nil, resourceNotFound("compaction plan", planID)
	}
	return plan, nil
}

func (s *Service) GetWatermark(ctx context.Context, streamID string) (WatermarkResult, error) {
	if err := contextError(ctx); err != nil {
		return WatermarkResult{}, err
	}
	state := s.readState()
	stream := storage.FindStream(state, normalizedIdentifier(streamID))
	if stream == nil {
		return WatermarkResult{}, resourceNotFound("stream", streamID)
	}
	return WatermarkResult{
		StreamID:        stream.ID,
		Watermark:       stream.Watermark,
		CompactionFloor: stream.CompactionFloor,
		StreamRevision:  stream.Revision,
	}, nil
}

func (s *Service) ListSegments(
	ctx context.Context,
	filter storage.SegmentFilter,
) ([]*domain.Segment, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	state := s.readState()
	if filter.StreamID != "" && state.Streams[filter.StreamID] == nil {
		return nil, resourceNotFound("stream", filter.StreamID)
	}
	return storage.ListSegments(state, filter), nil
}
