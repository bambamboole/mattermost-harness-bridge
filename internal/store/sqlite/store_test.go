package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store/storetest"
)

func TestConformanceFile(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		s, err := Open(filepath.Join(t.TempDir(), "broker.db"))
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestConformanceMemory(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		s, err := Open(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestMigrateIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.db")
	for i := 0; i < 2; i++ {
		s, err := Open(path)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		s.Close()
	}
}
