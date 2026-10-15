package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// EnsureDeleted must be safely retryable: removing an already-missing object
// is success (account-deletion retirement relies on this).
func TestLocalStorageEnsureDeletedIsIdempotent(t *testing.T) {
	base := t.TempDir()
	store := &LocalStorage{BasePath: base}
	ctx := context.Background()
	path := "attachments/a/b.dat"
	full := filepath.Join(base, filepath.FromSlash(path))
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(t, os.WriteFile(full, []byte("x"), 0o644))

	require.NoError(t, store.EnsureDeleted(ctx, path))
	_, err := os.Stat(full)
	require.True(t, os.IsNotExist(err))

	require.NoError(t, store.EnsureDeleted(ctx, path), "missing object is not an error")
}
