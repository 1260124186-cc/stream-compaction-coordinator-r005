package application

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

type Limits struct {
	MaxAppendRecords uint64
	MaxAppendBytes   int64
}

type Service struct {
	mu     sync.RWMutex
	state  *domain.State
	repo   storage.Repository
	ids    idgen.Generator
	clock  clock.Clock
	logger observability.Logger
	limits Limits
}

func NewService(
	repository storage.Repository,
	identifiers idgen.Generator,
	timeSource clock.Clock,
	logger observability.Logger,
	limits Limits,
) (*Service, error) {
	if repository == nil {
		return nil, domain.NewError(domain.CodeInvalidInput, "repository is required")
	}
	if identifiers == nil {
		return nil, domain.NewError(domain.CodeInvalidInput, "identifier generator is required")
	}
	if timeSource == nil {
		return nil, domain.NewError(domain.CodeInvalidInput, "time source is required")
	}
	if logger == nil {
		logger = observability.Discard{}
	}
	if limits.MaxAppendRecords == 0 {
		limits.MaxAppendRecords = 10_000
	}
	if limits.MaxAppendBytes <= 0 {
		limits.MaxAppendBytes = 16 << 20
	}

	loaded, err := repository.Load()
	if err != nil {
		return nil, err
	}
	report, err := recoverState(loaded, timeSource.Now())
	if err != nil {
		return nil, err
	}
	if report.Changed {
		if err := repository.Save(loaded); err != nil {
			return nil, err
		}
		logger.Info("startup.recovered", map[string]any{
			"aborted_plans":      report.AbortedPlanIDs,
			"recomputed_streams": report.RecomputedStreamIDs,
		})
	}

	return &Service{
		state:  loaded,
		repo:   repository,
		ids:    identifiers,
		clock:  timeSource,
		logger: logger,
		limits: limits,
	}, nil
}

func (s *Service) RepositoryPath() string {
	if s == nil || s.repo == nil {
		return ""
	}
	return s.repo.Path()
}

func (s *Service) readState() *domain.State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return domain.CloneState(s.state)
}

func (s *Service) mutate(
	ctx context.Context,
	action string,
	change func(*domain.State, domainTimeline) error,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := contextError(ctx); err != nil {
		return err
	}
	next := domain.CloneState(s.state)
	timeline := domainTimeline{current: s.clock.Now().UTC()}
	if err := change(next, timeline); err != nil {
		return err
	}
	if err := domain.ValidateState(next); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := s.repo.Save(next); err != nil {
		s.logger.Error("state.commit.failed", err, map[string]any{
			"action": action,
			"path":   s.repo.Path(),
		})
		return err
	}
	s.state = next
	s.logger.Info("state.committed", map[string]any{
		"action": action,
		"path":   s.repo.Path(),
	})
	return nil
}

type domainTimeline struct {
	current time.Time
}

func (t domainTimeline) Now() time.Time {
	return t.current
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return domain.WrapError(domain.CodeInvalidInput, "operation was canceled", ctx.Err())
	default:
		return nil
	}
}

func streamSegments(state *domain.State, streamID string) []*domain.Segment {
	result := make([]*domain.Segment, 0)
	for _, segment := range state.Segments {
		if segment != nil && segment.StreamID == streamID {
			result = append(result, segment)
		}
	}
	return result
}

func normalizedIdentifier(value string) string {
	return strings.TrimSpace(value)
}

func resourceNotFound(kind string, id string) error {
	return domain.NewErrorf(domain.CodeNotFound, "%s was not found", kind).
		WithDetail("id", id)
}

func validateLimits(limits Limits) error {
	if limits.MaxAppendRecords == 0 {
		return fmt.Errorf("append record limit must be positive")
	}
	if limits.MaxAppendBytes <= 0 {
		return fmt.Errorf("append byte limit must be positive")
	}
	return nil
}
