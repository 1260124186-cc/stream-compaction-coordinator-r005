package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteAtomically writes a complete sibling file and renames it into place.
// The file and parent directory are synchronized so a crash cannot commit a
// partially written snapshot on a local filesystem.
func WriteAtomically(path string, data []byte) error {
	if path == "" {
		return fmt.Errorf("path must not be empty")
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return fmt.Errorf("create parent directory: %w", err)
	}

	temporary, err := os.CreateTemp(parent, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary snapshot: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()

	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set snapshot file mode: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary snapshot: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("synchronize temporary snapshot: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary snapshot: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("rename temporary snapshot: %w", err)
	}
	committed = true

	directory, err := os.Open(parent)
	if err != nil {
		return fmt.Errorf("open parent directory for synchronization: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("synchronize parent directory: %w", err)
	}
	return nil
}
