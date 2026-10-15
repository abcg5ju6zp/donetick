package auth

import (
	"context"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"donetick.com/core/config"
	uModel "donetick.com/core/internal/user/model"
	uRepo "donetick.com/core/internal/user/repo"
)

// Token issuance must fail while the deletion fence is raised, even though the
// fence check runs before any JWT machinery is touched.
func TestGenerateTokens_RejectsFencedAccount(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&uModel.User{}))

	now := time.Now().UTC()
	require.NoError(t, db.Create(&uModel.User{
		ID: 1, Username: "fenced", CreatedAt: now, UpdatedAt: now, DeletingAt: &now,
	}).Error)

	repo := uRepo.NewUserRepository(db, &config.Config{})
	// jwtAuth is nil on purpose: a fenced account must be refused before the
	// JWT generator is used.
	svc := &TokenService{userRepo: repo}

	_, err = svc.GenerateTokens(context.Background(), &uModel.UserDetails{
		User: uModel.User{ID: 1},
	})
	require.ErrorIs(t, err, ErrAccountDeletionInProgress)
}
