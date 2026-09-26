package application

import "github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"

type CreateStreamCommand struct {
	Namespace      string
	Name           string
	ThresholdBytes int64
}

type CreateStreamResult struct {
	Stream  *domain.Stream  `json:"stream"`
	Segment *domain.Segment `json:"segment"`
}

type AppendRecordsCommand struct {
	StreamID         string
	ExpectedRevision uint64
	RecordCount      uint64
	PayloadBytes     int64
}

type AppendRecordsResult struct {
	Stream     *domain.Stream  `json:"stream"`
	Segment    *domain.Segment `json:"segment"`
	RolledOver bool            `json:"rolled_over"`
}

type CreateCompactionPlanCommand struct {
	StreamID               string
	SourceSegmentIDs       []string
	ExpectedStreamRevision uint64
}

type StartCompactionPlanCommand struct {
	PlanID string
}

type FinishCompactionPlanCommand struct {
	PlanID          string
	ResultChecksum  string
	ResultSizeBytes int64
}

type CompactionPlanResult struct {
	Plan            *domain.CompactionPlan `json:"plan"`
	ResultSegment   *domain.Segment        `json:"result_segment,omitempty"`
	AlreadyFinished bool                   `json:"already_finished"`
}

type AcknowledgeReplicaCommand struct {
	SegmentID  string
	NodeID     string
	Generation uint64
	Offset     int64
}

type AcknowledgeReplicaResult struct {
	Ack       domain.ReplicaAck `json:"ack"`
	Changed   bool              `json:"changed"`
	Watermark int64             `json:"watermark"`
	StreamID  string            `json:"stream_id"`
}

type WatermarkResult struct {
	StreamID        string `json:"stream_id"`
	Watermark       int64  `json:"watermark"`
	CompactionFloor int64  `json:"compaction_floor"`
	StreamRevision  uint64 `json:"stream_revision"`
}

type RecoveryReport struct {
	AbortedPlanIDs      []string `json:"aborted_plan_ids"`
	RecomputedStreamIDs []string `json:"recomputed_stream_ids"`
	Changed             bool     `json:"changed"`
}
