package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultAddress          = "127.0.0.1:8080"
	DefaultStatePath        = ".segmentd/state.json"
	DefaultMaxRequestBytes  = int64(1 << 20)
	DefaultRequestTimeout   = 5 * time.Second
	DefaultShutdownTimeout  = 10 * time.Second
	DefaultMaxAppendRecords = int64(10_000)
	DefaultMaxAppendBytes   = int64(16 << 20)
)

// Settings contains all runtime knobs accepted by the service process.
type Settings struct {
	Address          string
	StatePath        string
	MaxRequestBytes  int64
	RequestTimeout   time.Duration
	ShutdownTimeout  time.Duration
	MaxAppendRecords int64
	MaxAppendBytes   int64
}

func Defaults() Settings {
	return Settings{
		Address:          DefaultAddress,
		StatePath:        DefaultStatePath,
		MaxRequestBytes:  DefaultMaxRequestBytes,
		RequestTimeout:   DefaultRequestTimeout,
		ShutdownTimeout:  DefaultShutdownTimeout,
		MaxAppendRecords: DefaultMaxAppendRecords,
		MaxAppendBytes:   DefaultMaxAppendBytes,
	}
}

func FromEnvironment() (Settings, error) {
	settings := Defaults()
	var err error

	if value, ok := os.LookupEnv("SEGMENTD_ADDR"); ok {
		settings.Address = strings.TrimSpace(value)
	}
	if value, ok := os.LookupEnv("SEGMENTD_STATE_PATH"); ok {
		settings.StatePath = strings.TrimSpace(value)
	}
	if settings.MaxRequestBytes, err = envInt64(
		"SEGMENTD_MAX_REQUEST_BYTES",
		settings.MaxRequestBytes,
	); err != nil {
		return Settings{}, err
	}
	if settings.RequestTimeout, err = envDuration(
		"SEGMENTD_REQUEST_TIMEOUT",
		settings.RequestTimeout,
	); err != nil {
		return Settings{}, err
	}
	if settings.ShutdownTimeout, err = envDuration(
		"SEGMENTD_SHUTDOWN_TIMEOUT",
		settings.ShutdownTimeout,
	); err != nil {
		return Settings{}, err
	}
	if settings.MaxAppendRecords, err = envInt64(
		"SEGMENTD_MAX_APPEND_RECORDS",
		settings.MaxAppendRecords,
	); err != nil {
		return Settings{}, err
	}
	if settings.MaxAppendBytes, err = envInt64(
		"SEGMENTD_MAX_APPEND_BYTES",
		settings.MaxAppendBytes,
	); err != nil {
		return Settings{}, err
	}
	if err := settings.Validate(); err != nil {
		return Settings{}, err
	}
	return settings, nil
}

func (s Settings) Validate() error {
	if strings.TrimSpace(s.Address) == "" {
		return fmt.Errorf("SEGMENTD_ADDR must not be empty")
	}
	if strings.TrimSpace(s.StatePath) == "" {
		return fmt.Errorf("SEGMENTD_STATE_PATH must not be empty")
	}
	if s.MaxRequestBytes < 1024 {
		return fmt.Errorf("SEGMENTD_MAX_REQUEST_BYTES must be at least 1024")
	}
	if s.RequestTimeout <= 0 {
		return fmt.Errorf("SEGMENTD_REQUEST_TIMEOUT must be positive")
	}
	if s.ShutdownTimeout <= 0 {
		return fmt.Errorf("SEGMENTD_SHUTDOWN_TIMEOUT must be positive")
	}
	if s.MaxAppendRecords <= 0 {
		return fmt.Errorf("SEGMENTD_MAX_APPEND_RECORDS must be positive")
	}
	if s.MaxAppendBytes <= 0 {
		return fmt.Errorf("SEGMENTD_MAX_APPEND_BYTES must be positive")
	}
	return nil
}

func envInt64(name string, fallback int64) (int64, error) {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("%s must not be empty", name)
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return value, nil
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	raw = strings.TrimSpace(raw)
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", name, err)
	}
	return value, nil
}
