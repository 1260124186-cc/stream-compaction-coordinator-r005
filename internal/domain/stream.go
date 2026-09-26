package domain

import (
	"strings"
	"time"
	"unicode"
)

const (
	MaxNamespaceLength  = 64
	MaxStreamNameLength = 128
)

func NewStream(
	streamID string,
	segmentID string,
	namespace string,
	name string,
	thresholdBytes int64,
	now time.Time,
) (*Stream, *Segment, error) {
	namespace = strings.TrimSpace(namespace)
	name = strings.TrimSpace(name)
	if err := ValidateLabel("namespace", namespace, MaxNamespaceLength); err != nil {
		return nil, nil, err
	}
	if err := ValidateLabel("stream name", name, MaxStreamNameLength); err != nil {
		return nil, nil, err
	}
	if strings.TrimSpace(streamID) == "" {
		return nil, nil, NewError(CodeInvalidInput, "stream identifier is required")
	}
	if strings.TrimSpace(segmentID) == "" {
		return nil, nil, NewError(CodeInvalidInput, "segment identifier is required")
	}
	if thresholdBytes <= 0 {
		return nil, nil, NewError(CodeInvalidInput, "segment threshold must be positive").
			WithDetail("segment_threshold_bytes", thresholdBytes)
	}

	timestamp := now.UTC()
	stream := &Stream{
		ID:                    streamID,
		Namespace:             namespace,
		Name:                  name,
		ActiveSegmentID:       segmentID,
		SealedSegmentIDs:      []string{},
		CompactionFloor:       0,
		Watermark:             0,
		Revision:              1,
		SegmentThresholdBytes: thresholdBytes,
		CreatedAt:             timestamp,
		UpdatedAt:             timestamp,
	}
	segment := &Segment{
		ID:               segmentID,
		StreamID:         streamID,
		Generation:       1,
		Range:            Range{BaseOffset: 0, EndOffset: 0},
		State:            SegmentStateOpen,
		RecordCount:      0,
		SizeBytes:        0,
		Checksum:         "",
		SourceSegmentIDs: []string{},
		Durable:          false,
		ReplicaAcks:      map[string]ReplicaAck{},
		CreatedAt:        timestamp,
		UpdatedAt:        timestamp,
	}
	return stream, segment, nil
}

type AppendInput struct {
	ExpectedRevision uint64
	RecordCount      uint64
	PayloadBytes     int64
}

type AppendResult struct {
	Stream          *Stream
	Segment         *Segment
	PreviousSegment *Segment
	RolledOver      bool
}

func AppendRecords(
	stream *Stream,
	segment *Segment,
	input AppendInput,
	newSegmentID string,
	maxRecords uint64,
	maxBytes int64,
	now time.Time,
) (AppendResult, error) {
	if stream == nil || segment == nil {
		return AppendResult{}, NewError(CodeNotFound, "stream or active segment was not found")
	}
	if stream.ActiveSegmentID != segment.ID {
		return AppendResult{}, NewError(CodeInvalidState, "segment is not active for the stream")
	}
	if segment.State != SegmentStateOpen {
		return AppendResult{}, NewError(CodeInvalidState, "records may only be appended to an open segment")
	}
	if input.ExpectedRevision == 0 {
		return AppendResult{}, NewError(CodeInvalidInput, "expected revision is required")
	}
	if input.ExpectedRevision != stream.Revision {
		return AppendResult{}, NewError(CodeStaleRevision, "stream revision does not match").
			WithDetail("expected", input.ExpectedRevision).
			WithDetail("current", stream.Revision)
	}
	if input.RecordCount == 0 {
		return AppendResult{}, NewError(CodeInvalidInput, "record count must be positive")
	}
	if input.PayloadBytes <= 0 {
		return AppendResult{}, NewError(CodeInvalidInput, "payload byte count must be positive")
	}
	if maxRecords > 0 && input.RecordCount > maxRecords {
		return AppendResult{}, NewError(CodeLimitExceeded, "record batch exceeds the configured limit").
			WithDetail("limit", maxRecords)
	}
	if maxBytes > 0 && input.PayloadBytes > maxBytes {
		return AppendResult{}, NewError(CodeLimitExceeded, "byte batch exceeds the configured limit").
			WithDetail("limit", maxBytes)
	}

	newEnd, err := AddInt64Safely(segment.Range.EndOffset, input.PayloadBytes)
	if err != nil {
		return AppendResult{}, err
	}
	newRecordCount, err := AddUint64Safely(segment.RecordCount, input.RecordCount)
	if err != nil {
		return AppendResult{}, err
	}
	newSize, err := AddInt64Safely(segment.SizeBytes, input.PayloadBytes)
	if err != nil {
		return AppendResult{}, err
	}

	timestamp := now.UTC()
	previous := cloneSegment(segment)
	segment.Range.EndOffset = newEnd
	segment.RecordCount = newRecordCount
	segment.SizeBytes = newSize
	segment.UpdatedAt = timestamp

	stream.Revision++
	stream.UpdatedAt = timestamp

	result := AppendResult{
		Stream:          stream,
		Segment:         segment,
		PreviousSegment: previous,
		RolledOver:      false,
	}
	if newSize < stream.SegmentThresholdBytes {
		return result, nil
	}

	if strings.TrimSpace(newSegmentID) == "" {
		return AppendResult{}, NewError(CodeInvalidInput, "new segment identifier is required")
	}
	if newSegmentID == segment.ID {
		return AppendResult{}, NewError(CodeConflict, "new segment identifier is already in use")
	}

	sealedAt := timestamp
	segment.State = SegmentStateSealed
	segment.SealedAt = &sealedAt
	segment.UpdatedAt = timestamp

	next := &Segment{
		ID:               newSegmentID,
		StreamID:         stream.ID,
		Generation:       segment.Generation + 1,
		Range:            Range{BaseOffset: newEnd, EndOffset: newEnd},
		State:            SegmentStateOpen,
		RecordCount:      0,
		SizeBytes:        0,
		Checksum:         "",
		SourceSegmentIDs: []string{},
		Durable:          false,
		ReplicaAcks:      map[string]ReplicaAck{},
		CreatedAt:        timestamp,
		UpdatedAt:        timestamp,
	}
	stream.SealedSegmentIDs = append(stream.SealedSegmentIDs, segment.ID)
	stream.ActiveSegmentID = next.ID
	stream.UpdatedAt = timestamp

	result.Segment = next
	result.RolledOver = true
	return result, nil
}

func RecomputeStreamWatermark(stream *Stream, segments []*Segment) {
	if stream == nil {
		return
	}
	watermark := int64(0)
	initialized := false
	for _, segment := range segments {
		if segment == nil || segment.StreamID != stream.ID {
			continue
		}
		if segment.State == SegmentStateOpen || segment.State == SegmentStateRetired {
			continue
		}
		boundary := segment.Range.BaseOffset
		if segment.Durable {
			boundary = segment.Range.EndOffset
		}
		if !initialized || boundary < watermark {
			watermark = boundary
			initialized = true
		}
	}
	if !initialized {
		watermark = 0
	}
	stream.Watermark = watermark
}

func ValidateLabel(field string, value string, maximum int) error {
	if value == "" {
		return NewErrorf(CodeInvalidInput, "%s is required", field)
	}
	if len([]rune(value)) > maximum {
		return NewErrorf(CodeInvalidInput, "%s exceeds %d characters", field, maximum).
			WithDetail("field", field)
	}
	for _, character := range value {
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			continue
		}
		switch character {
		case '-', '_', '.', ' ', ':':
			continue
		default:
			return NewErrorf(CodeInvalidInput, "%s contains an unsupported character", field).
				WithDetail("field", field)
		}
	}
	return nil
}
