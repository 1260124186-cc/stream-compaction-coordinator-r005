package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

func MarshalState(state *domain.State) ([]byte, error) {
	if state == nil {
		return nil, fmt.Errorf("state is nil")
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func UnmarshalState(data []byte) (*domain.State, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state domain.State
	if err := decoder.Decode(&state); err != nil {
		return nil, err
	}
	if err := ensureEndOfDocument(decoder); err != nil {
		return nil, err
	}
	return &state, nil
}

func ensureEndOfDocument(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("state snapshot contains trailing JSON content")
}
