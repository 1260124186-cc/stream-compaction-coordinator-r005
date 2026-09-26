package storage

import (
	"sort"
	"strings"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

func domainTimeZero() time.Time {
	return time.Time{}
}

type SegmentFilter struct {
	StreamID string
	States   map[domain.SegmentState]bool
	AfterID  string
	Limit    int
}

func ListSegments(state *domain.State, filter SegmentFilter) []*domain.Segment {
	if state == nil {
		return []*domain.Segment{}
	}
	result := make([]*domain.Segment, 0)
	for _, segment := range state.Segments {
		if segment == nil {
			continue
		}
		if filter.StreamID != "" && segment.StreamID != filter.StreamID {
			continue
		}
		if len(filter.States) > 0 && !filter.States[segment.State] {
			continue
		}
		result = append(result, domain.CloneState(&domain.State{
			SchemaVersion: domain.SnapshotSchemaVersion,
			Streams:       map[string]*domain.Stream{},
			Segments:      map[string]*domain.Segment{segment.ID: segment},
			Plans:         map[string]*domain.CompactionPlan{},
		}).Segments[segment.ID])
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Range.BaseOffset == result[right].Range.BaseOffset {
			return result[left].ID < result[right].ID
		}
		return result[left].Range.BaseOffset < result[right].Range.BaseOffset
	})
	if filter.AfterID != "" {
		start := 0
		for index, segment := range result {
			if segment.ID == filter.AfterID {
				start = index + 1
				break
			}
		}
		result = result[start:]
	}
	if filter.Limit > 0 && len(result) > filter.Limit {
		result = result[:filter.Limit]
	}
	return result
}

func FindStreamByName(state *domain.State, namespace, name string) *domain.Stream {
	if state == nil {
		return nil
	}
	namespace = strings.TrimSpace(namespace)
	name = strings.TrimSpace(name)
	for _, stream := range state.Streams {
		if stream != nil && stream.Namespace == namespace && stream.Name == name {
			return cloneStreamForRead(stream)
		}
	}
	return nil
}

func FindStream(state *domain.State, streamID string) *domain.Stream {
	if state == nil {
		return nil
	}
	return cloneStreamForRead(state.Streams[streamID])
}

func FindSegment(state *domain.State, segmentID string) *domain.Segment {
	if state == nil || state.Segments[segmentID] == nil {
		return nil
	}
	copyState := &domain.State{
		SchemaVersion: domain.SnapshotSchemaVersion,
		Streams:       map[string]*domain.Stream{},
		Segments: map[string]*domain.Segment{
			segmentID: state.Segments[segmentID],
		},
		Plans: map[string]*domain.CompactionPlan{},
	}
	return domain.CloneState(copyState).Segments[segmentID]
}

func FindPlan(state *domain.State, planID string) *domain.CompactionPlan {
	if state == nil || state.Plans[planID] == nil {
		return nil
	}
	copyState := &domain.State{
		SchemaVersion: domain.SnapshotSchemaVersion,
		Streams:       map[string]*domain.Stream{},
		Segments:      map[string]*domain.Segment{},
		Plans: map[string]*domain.CompactionPlan{
			planID: state.Plans[planID],
		},
	}
	return domain.CloneState(copyState).Plans[planID]
}

func cloneStreamForRead(source *domain.Stream) *domain.Stream {
	if source == nil {
		return nil
	}
	copyState := domain.CloneState(&domain.State{
		SchemaVersion: domain.SnapshotSchemaVersion,
		Streams: map[string]*domain.Stream{
			source.ID: source,
		},
		Segments: map[string]*domain.Segment{},
		Plans:    map[string]*domain.CompactionPlan{},
	})
	return copyState.Streams[source.ID]
}
