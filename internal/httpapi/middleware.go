package httpapi

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

type contextKey string

const requestIDKey contextKey = "request_id"

var requestCounter atomic.Uint64

func (a *API) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		requestID := request.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = requestIdentifier()
		}
		writer.Header().Set("X-Request-ID", requestID)
		writer.Header().Set("X-Content-Type-Options", "nosniff")

		ctx, cancel := context.WithTimeout(request.Context(), a.requestTimeout)
		defer cancel()
		request = request.WithContext(context.WithValue(ctx, requestIDKey, requestID))
		status := &statusWriter{ResponseWriter: writer}

		defer func() {
			if recovered := recover(); recovered != nil {
				a.logger.Error("http.panic", nil, map[string]any{
					"request_id": requestID,
					"method":     request.Method,
					"path":       request.URL.Path,
					"panic":      recovered,
				})
				if !status.wroteHeader {
					writeError(status, context.DeadlineExceeded)
				}
			}
			a.logger.Info("http.request", map[string]any{
				"request_id": requestID,
				"method":     request.Method,
				"path":       request.URL.Path,
				"status":     status.statusCode(),
				"duration":   time.Since(started).String(),
			})
		}()

		next.ServeHTTP(status, request)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *statusWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func requestIdentifier() string {
	value := requestCounter.Add(1)
	return "req_" + time.Now().UTC().Format("20060102T150405.000000000") + "_" + formatUint(value)
}

func formatUint(value uint64) string {
	if value == 0 {
		return "0"
	}
	var buffer [20]byte
	index := len(buffer)
	for value > 0 {
		index--
		buffer[index] = byte('0' + value%10)
		value /= 10
	}
	return string(buffer[index:])
}
