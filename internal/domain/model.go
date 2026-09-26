package domain

import "time"

const (
	SnapshotSchemaVersion = 1

	SegmentStateOpen       SegmentState = "open"
	SegmentStateSealed     SegmentState = "sealed"
	SegmentStateCompacting SegmentState = "compacting"
	SegmentStateRetired    SegmentState = "retired"

	PlanStateProposed  PlanState = "proposed"
	PlanStateRunning   PlanState = "running"
	PlanStateCompleted PlanState = "completed"
	PlanStateAborted   PlanState = "aborted"
)

type SegmentState string

type PlanState string

type Stream struct {
	ID                    string    `json:"id"`
	Namespace             string    `json:"namespace"`
	Name                  string    `json:"name"`
	ActiveSegmentID       string    `json:"active_segment_id"`
	SealedSegmentIDs      []string  `json:"sealed_segment_ids"`
	CompactionFloor       int64     `json:"compaction_floor"`
	Watermark             int64     `json:"watermark"`
	Revision              uint64    `json:"revision"`
	SegmentThresholdBytes int64     `json:"segment_threshold_bytes"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
}

type Segment struct {
	ID               string                `json:"id"`
	StreamID         string                `json:"stream_id"`
	Generation       uint64                `json:"generation"`
	Range            Range                 `json:"range"`
	State            SegmentState          `json:"state"`
	RecordCount      uint64                `json:"record_count"`
	SizeBytes        int64                 `json:"size_bytes"`
	Checksum         string                `json:"checksum"`
	SourceSegmentIDs []string              `json:"source_segment_ids"`
	Durable          bool                  `json:"durable"`
	ReplicaAcks      map[string]ReplicaAck `json:"replica_acks"`
	CreatedAt        time.Time             `json:"created_at"`
	UpdatedAt        time.Time             `json:"updated_at"`
	SealedAt         *time.Time            `json:"sealed_at,omitempty"`
	RetiredAt        *time.Time            `json:"retired_at,omitempty"`
}

type ReplicaAck struct {
	NodeID     string    `json:"node_id"`
	Generation uint64    `json:"generation"`
	Offset     int64     `json:"offset"`
	AckedAt    time.Time `json:"acked_at"`
}

type CompactionPlan struct {
	ID                     string            `json:"id"`
	StreamID               string            `json:"stream_id"`
	SourceSegmentIDs       []string          `json:"source_segment_ids"`
	SourceGenerations      map[string]uint64 `json:"source_generations"`
	Range                  Range             `json:"range"`
	State                  PlanState         `json:"state"`
	ExpectedStreamRevision uint64            `json:"expected_stream_revision"`
	Revision               uint64            `json:"revision"`
	ResultSegmentID        string            `json:"result_segment_id,omitempty"`
	ResultChecksum         string            `json:"result_checksum,omitempty"`
	ResultSizeBytes        int64             `json:"result_size_bytes,omitempty"`
	CreatedAt              time.Time         `json:"created_at"`
	UpdatedAt              time.Time         `json:"updated_at"`
	StartedAt              *time.Time        `json:"started_at,omitempty"`
	CompletedAt            *time.Time        `json:"completed_at,omitempty"`
}

type AuditEvent struct {
	Sequence   uint64    `json:"sequence"`
	Action     string    `json:"action"`
	StreamID   string    `json:"stream_id,omitempty"`
	EntityID   string    `json:"entity_id,omitempty"`
	Revision   uint64    `json:"revision"`
	OccurredAt time.Time `json:"occurred_at"`
}

type State struct {
	SchemaVersion int                        `json:"schema_version"`
	Streams       map[string]*Stream         `json:"streams"`
	Segments      map[string]*Segment        `json:"segments"`
	Plans         map[string]*CompactionPlan `json:"plans"`
	Audit         []AuditEvent               `json:"audit"`
	AuditSequence uint64                     `json:"audit_sequence"`
	UpdatedAt     time.Time                  `json:"updated_at"`
}

func NewState(now time.Time) *State {
	return &State{
		SchemaVersion: SnapshotSchemaVersion,
		Streams:       map[string]*Stream{},
		Segments:      map[string]*Segment{},
		Plans:         map[string]*CompactionPlan{},
		Audit:         []AuditEvent{},
		UpdatedAt:     now.UTC(),
	}
}

func CloneState(source *State) *State {
	if source == nil {
		return NewState(time.Time{})
	}
	target := &State{
		SchemaVersion: source.SchemaVersion,
		Streams:       make(map[string]*Stream, len(source.Streams)),
		Segments:      make(map[string]*Segment, len(source.Segments)),
		Plans:         make(map[string]*CompactionPlan, len(source.Plans)),
		Audit:         make([]AuditEvent, len(source.Audit)),
		AuditSequence: source.AuditSequence,
		UpdatedAt:     source.UpdatedAt,
	}
	for id, stream := range source.Streams {
		target.Streams[id] = cloneStream(stream)
	}
	for id, segment := range source.Segments {
		target.Segments[id] = cloneSegment(segment)
	}
	for id, plan := range source.Plans {
		target.Plans[id] = clonePlan(plan)
	}
	copy(target.Audit, source.Audit)
	return target
}

func cloneStream(source *Stream) *Stream {
	if source == nil {
		return nil
	}
	target := *source
	target.SealedSegmentIDs = append([]string(nil), source.SealedSegmentIDs...)
	return &target
}

func cloneSegment(source *Segment) *Segment {
	if source == nil {
		return nil
	}
	target := *source
	target.SourceSegmentIDs = append([]string(nil), source.SourceSegmentIDs...)
	target.ReplicaAcks = make(map[string]ReplicaAck, len(source.ReplicaAcks))
	for nodeID, ack := range source.ReplicaAcks {
		target.ReplicaAcks[nodeID] = ack
	}
	if source.SealedAt != nil {
		value := *source.SealedAt
		target.SealedAt = &value
	}
	if source.RetiredAt != nil {
		value := *source.RetiredAt
		target.RetiredAt = &value
	}
	return &target
}

func clonePlan(source *CompactionPlan) *CompactionPlan {
	if source == nil {
		return nil
	}
	target := *source
	target.SourceSegmentIDs = append([]string(nil), source.SourceSegmentIDs...)
	target.SourceGenerations = make(map[string]uint64, len(source.SourceGenerations))
	for id, generation := range source.SourceGenerations {
		target.SourceGenerations[id] = generation
	}
	if source.StartedAt != nil {
		value := *source.StartedAt
		target.StartedAt = &value
	}
	if source.CompletedAt != nil {
		value := *source.CompletedAt
		target.CompletedAt = &value
	}
	return &target
}

func RecordAudit(
	state *State,
	action string,
	streamID string,
	entityID string,
	revision uint64,
	now time.Time,
) {
	state.AuditSequence++
	state.Audit = append(state.Audit, AuditEvent{
		Sequence:   state.AuditSequence,
		Action:     action,
		StreamID:   streamID,
		EntityID:   entityID,
		Revision:   revision,
		OccurredAt: now.UTC(),
	})
	state.UpdatedAt = now.UTC()
}
