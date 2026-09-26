package idgen

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// Generator returns opaque identifiers for newly created entities.
type Generator interface {
	New(prefix string) string
}

var encoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// Random combines a timestamp and cryptographic bytes to avoid collisions
// between processes without requiring global coordination.
type Random struct {
	fallback atomic.Uint64
}

func (r *Random) New(prefix string) string {
	cleanPrefix := strings.Trim(strings.ToLower(prefix), "_- ")
	if cleanPrefix == "" {
		cleanPrefix = "ent"
	}

	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		value := r.fallback.Add(1)
		return fmt.Sprintf(
			"%s_%x_%x",
			cleanPrefix,
			time.Now().UTC().UnixNano(),
			value,
		)
	}

	suffix := encoding.EncodeToString(randomBytes)
	if len(suffix) > 10 {
		suffix = suffix[:10]
	}
	return cleanPrefix + "_" + strings.ToLower(suffix)
}

// Counter produces predictable values when only uniqueness inside one process
// is required.
type Counter struct {
	next atomic.Uint64
}

func (c *Counter) New(prefix string) string {
	cleanPrefix := strings.Trim(strings.ToLower(prefix), "_- ")
	if cleanPrefix == "" {
		cleanPrefix = "ent"
	}
	return fmt.Sprintf("%s_%06d", cleanPrefix, c.next.Add(1))
}
