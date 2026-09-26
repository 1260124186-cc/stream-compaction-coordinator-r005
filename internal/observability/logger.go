package observability

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Logger captures structured operational records without coupling application
// behavior to a logging framework.
type Logger interface {
	Info(action string, fields map[string]any)
	Error(action string, err error, fields map[string]any)
}

type JSONLogger struct {
	mu      sync.Mutex
	out     io.Writer
	encoder *json.Encoder
}

func NewJSONLogger(out io.Writer) *JSONLogger {
	if out == nil {
		out = os.Stderr
	}
	encoder := json.NewEncoder(out)
	encoder.SetEscapeHTML(false)
	return &JSONLogger{
		out:     out,
		encoder: encoder,
	}
}

func (l *JSONLogger) Info(action string, fields map[string]any) {
	l.write("info", action, nil, fields)
}

func (l *JSONLogger) Error(action string, err error, fields map[string]any) {
	l.write("error", action, err, fields)
}

func (l *JSONLogger) write(kind string, action string, err error, fields map[string]any) {
	record := make(map[string]any, len(fields)+4)
	record["kind"] = kind
	record["action"] = action
	record["at"] = time.Now().UTC().Format(time.RFC3339Nano)
	if err != nil {
		record["error"] = err.Error()
	}
	for key, value := range fields {
		if key == "kind" || key == "action" || key == "at" || key == "error" {
			continue
		}
		record[key] = value
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.encoder.Encode(record); err != nil {
		fmt.Fprintf(l.out, `{"kind":"error","action":"logger.encode","error":%q}`+"\n", err.Error())
	}
}

// Discard drops all records and is used by short-lived checks.
type Discard struct{}

func (Discard) Info(string, map[string]any) {}

func (Discard) Error(string, error, map[string]any) {}
