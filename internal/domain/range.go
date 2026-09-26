package domain

import "math"

// Range is a half-open interval. A newly opened segment may temporarily have
// equal boundaries until its first record batch is accepted.
type Range struct {
	BaseOffset int64 `json:"base_offset"`
	EndOffset  int64 `json:"end_offset"`
}

func NewRange(baseOffset, endOffset int64) (Range, error) {
	result := Range{
		BaseOffset: baseOffset,
		EndOffset:  endOffset,
	}
	if err := result.Validate(); err != nil {
		return Range{}, err
	}
	return result, nil
}

func (r Range) Validate() error {
	if r.BaseOffset < 0 {
		return NewError(CodeInvalidRange, "base offset must not be negative").
			WithDetail("base_offset", r.BaseOffset)
	}
	if r.EndOffset < r.BaseOffset {
		return NewError(CodeInvalidRange, "end offset must not precede base offset").
			WithDetail("base_offset", r.BaseOffset).
			WithDetail("end_offset", r.EndOffset)
	}
	return nil
}

func (r Range) Length() (int64, error) {
	if err := r.Validate(); err != nil {
		return 0, err
	}
	return r.EndOffset - r.BaseOffset, nil
}

func (r Range) IsEmpty() bool {
	return r.BaseOffset == r.EndOffset
}

func (r Range) ContainsOffset(offset int64) bool {
	return offset >= r.BaseOffset && offset <= r.EndOffset
}

func (r Range) ContainsStrictOffset(offset int64) bool {
	return offset >= r.BaseOffset && offset < r.EndOffset
}

func (r Range) Overlaps(other Range) bool {
	return r.BaseOffset < other.EndOffset && other.BaseOffset < r.EndOffset
}

func RangeUnion(first, second Range) (Range, error) {
	if err := first.Validate(); err != nil {
		return Range{}, err
	}
	if err := second.Validate(); err != nil {
		return Range{}, err
	}
	return NewRange(
		minInt64(first.BaseOffset, second.BaseOffset),
		maxInt64(first.EndOffset, second.EndOffset),
	)
}

func ValidateContiguousRanges(ranges []Range) (Range, error) {
	if len(ranges) == 0 {
		return Range{}, NewError(CodeInvalidRange, "at least one range is required")
	}

	union := ranges[0]
	if err := union.Validate(); err != nil {
		return Range{}, err
	}
	for index := 1; index < len(ranges); index++ {
		current := ranges[index]
		if err := current.Validate(); err != nil {
			return Range{}, err
		}
		if current.BaseOffset != union.EndOffset {
			return Range{}, NewError(CodeInvalidRange, "ranges must be contiguous").
				WithDetail("expected_base", union.EndOffset).
				WithDetail("actual_base", current.BaseOffset)
		}
		var err error
		union, err = RangeUnion(union, current)
		if err != nil {
			return Range{}, err
		}
	}
	return union, nil
}

func AddInt64Safely(left, right int64) (int64, error) {
	if right > 0 && left > math.MaxInt64-right {
		return 0, NewError(CodeOverflow, "signed offset addition overflow")
	}
	if right < 0 && left < math.MinInt64-right {
		return 0, NewError(CodeOverflow, "signed offset addition underflow")
	}
	return left + right, nil
}

func AddUint64Safely(left, right uint64) (uint64, error) {
	if left > math.MaxUint64-right {
		return 0, NewError(CodeOverflow, "record count addition overflow")
	}
	return left + right, nil
}

func minInt64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
