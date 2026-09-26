package domain

import (
	"strings"
	"time"
)

type ReplicaAckInput struct {
	NodeID     string
	Generation uint64
	Offset     int64
}

func ApplyReplicaAck(
	stream *Stream,
	segment *Segment,
	input ReplicaAckInput,
	now time.Time,
) (ReplicaAck, bool, error) {
	if stream == nil || segment == nil {
		return ReplicaAck{}, false, NewError(CodeNotFound, "segment was not found")
	}
	if segment.StreamID != stream.ID {
		return ReplicaAck{}, false, NewError(CodeConflict, "segment and stream do not match")
	}
	if segment.State == SegmentStateRetired {
		return ReplicaAck{}, false, NewError(CodeInvalidState, "retired segments do not accept acknowledgements")
	}
	if strings.TrimSpace(input.NodeID) == "" {
		return ReplicaAck{}, false, NewError(CodeInvalidInput, "node identifier is required")
	}
	if input.Generation == 0 {
		return ReplicaAck{}, false, NewError(CodeInvalidInput, "segment generation is required")
	}
	if input.Generation != segment.Generation {
		return ReplicaAck{}, false, NewError(CodeConflict, "segment generation does not match").
			WithDetail("expected", segment.Generation).
			WithDetail("actual", input.Generation)
	}
	if !segment.Range.ContainsOffset(input.Offset) {
		return ReplicaAck{}, false, NewError(CodeInvalidRange, "acknowledged offset is outside the segment range").
			WithDetail("base_offset", segment.Range.BaseOffset).
			WithDetail("end_offset", segment.Range.EndOffset).
			WithDetail("offset", input.Offset)
	}

	existing, exists := segment.ReplicaAcks[input.NodeID]
	if exists && existing.Generation == input.Generation && input.Offset < existing.Offset {
		return ReplicaAck{}, false, NewError(CodeConflict, "replica acknowledgement must not decrease").
			WithDetail("current", existing.Offset).
			WithDetail("offered", input.Offset)
	}
	if exists &&
		existing.Generation == input.Generation &&
		input.Offset == existing.Offset {
		return existing, false, nil
	}

	ack := ReplicaAck{
		NodeID:     input.NodeID,
		Generation: input.Generation,
		Offset:     input.Offset,
		AckedAt:    now.UTC(),
	}
	segment.ReplicaAcks[input.NodeID] = ack
	segment.UpdatedAt = ack.AckedAt
	if input.Offset == segment.Range.EndOffset {
		segment.Durable = true
	}
	return ack, true, nil
}
