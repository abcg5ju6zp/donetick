package user

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"donetick.com/core/config"
	cModel "donetick.com/core/internal/circle/model"
	storageModel "donetick.com/core/internal/storage/model"
	uModel "donetick.com/core/internal/user/model"
	uRepo "donetick.com/core/internal/user/repo"
)

const (
	stagedUserID   = 1
	stagedFilePath = "attachments/user-1/photo.jpg"
)

func newStagedDeletionService(db *gorm.DB, stor *mockStorage) *DeletionService {
	return NewDeletionService(db, stor)
}

func createUserWithAttachment(t *testing.T, db *gorm.DB) *mockStorage {
	t.Helper()
	now := time.Now().UTC()
	require.NoError(t, db.Create(&uModel.User{
		ID: stagedUserID, Username: "leaver", DisplayName: "Leaver",
		Email: "leaver@example.com", Password: "supersecret-pw-hash",
		MFASecret: "mfa-secret-value", CircleID: 7, CreatedAt: now, UpdatedAt: now,
	}).Error)
	require.NoError(t, db.Create(&storageModel.StorageFile{
		FilePath: stagedFilePath, FileName: "photo.jpg", SizeBytes: 1234, UserID: stagedUserID,
	}).Error)
	return &mockStorage{}
}

// Full staged flow for an account with an attachment: fence + manifests,
// database phase, then object retirement, ending in the completed state.
func TestDeleteOwnAccount_StagedWithAttachmentCompletes(t *testing.T) {
	db := setupTestDeletionDB(t)
	stor := createUserWithAttachment(t, db)
	service := newStagedDeletionService(db, stor)

	result, err := service.DeleteOwnAccount(context.Background(), stagedUserID, nil)
	require.NoError(t, err)
	require.True(t, result.Success, result.Message)
	require.False(t, result.StorageCleanupPending)
	require.Equal(t, 1, result.DeletedData["user"])
	require.Equal(t, 1, result.DeletedData["storage_files"])

	// User and file rows are gone; the external object was retired.
	var userCount, fileCount int64
	db.Model(&uModel.User{}).Where("id = ?", stagedUserID).Count(&userCount)
	require.Zero(t, userCount)
	db.Model(&storageModel.StorageFile{}).Where("user_id = ?", stagedUserID).Count(&fileCount)
	require.Zero(t, fileCount)
	require.Equal(t, []string{stagedFilePath}, stor.ensuredPaths)

	record := requireRecord(t, db, stagedUserID)
	require.Equal(t, uModel.DeletionStatusCompleted, record.Status)
	require.Equal(t, "[]", record.StorageFilesJSON)
	require.NotNil(t, record.CompletedAt)
	require.NotNil(t, record.DBCommittedAt)
	require.NotEmpty(t, record.UserSnapshotJSON)

	// Idempotent repeat (e.g. response lost and client retries): same terminal
	// state, no error, no second object deletion attempt.
	result2, err := service.DeleteOwnAccount(context.Background(), stagedUserID, nil)
	require.NoError(t, err)
	require.True(t, result2.Success)
	require.Len(t, stor.ensuredPaths, 1)
	require.Equal(t, int64(1), countRecords(t, db))
}

// Object storage unreachable after the database commit: the account is gone,
// a secret-free storage_pending record keeps the remaining file manifest, and
// retry/background recovery finishes the job.
func TestDeleteOwnAccount_StorageFailureLeavesResumableRecord(t *testing.T) {
	db := setupTestDeletionDB(t)
	stor := createUserWithAttachment(t, db)
	stor.ensureDeleteErr = errors.New("object store unreachable")
	service := newStagedDeletionService(db, stor)

	result, err := service.DeleteOwnAccount(context.Background(), stagedUserID, nil)
	require.NoError(t, err, "storage failure does not fail the API call after DB commit")
	require.True(t, result.Success)
	require.True(t, result.StorageCleanupPending)

	record := requireRecord(t, db, stagedUserID)
	require.Equal(t, uModel.DeletionStatusStoragePending, record.Status)
	require.Equal(t, 1, record.FileCount)
	var remaining []uModel.DeletionFileEntry
	require.NoError(t, json.Unmarshal([]byte(record.StorageFilesJSON), &remaining))
	require.Len(t, remaining, 1)
	require.Equal(t, stagedFilePath, remaining[0].Path)

	// User row and storage_files rows are already committed away.
	require.Zero(t, countUsers(t, db, stagedUserID))
	require.Zero(t, countStorageFiles(t, db, stagedUserID))

	// The persisted record must not contain credentials or secrets.
	raw := rawRecordText(t, db, stagedUserID)
	require.NotContains(t, raw, "supersecret-pw-hash")
	require.NotContains(t, raw, "mfa-secret-value")
	require.NotContains(t, raw, "sessionhash-secret")
	require.NotContains(t, raw, "apitokenhash-secret")

	// Process restart: a fresh service with healthy storage takes the record
	// over using only the stored manifest (storage_files rows are gone).
	recovery := NewDeletionService(db, &mockStorage{})
	recovered, err := recovery.DeleteOwnAccount(context.Background(), stagedUserID, nil)
	require.NoError(t, err)
	require.True(t, recovered.Success)
	require.False(t, recovered.StorageCleanupPending)

	record = requireRecord(t, db, stagedUserID)
	require.Equal(t, uModel.DeletionStatusCompleted, record.Status)
	require.Equal(t, "[]", record.StorageFilesJSON)
}

// Failure in the database phase rolls everything back: the account and its
// circle ownership stay usable, the fence is released, while credentials
// revoked by the fence stay revoked. A later attempt succeeds.
func TestDeleteOwnAccount_DatabaseFailureKeepsAccountAndAllowsRetry(t *testing.T) {
	db := setupTestDeletionDB(t)
	stor := createUserWithAttachment(t, db)
	service := newStagedDeletionService(db, stor)

	// Credentials that must be revoked by the fence.
	require.NoError(t, db.Create(&uModel.UserSession{
		ID: "s1", UserID: stagedUserID, TokenHash: "sessionhash-secret", FamilyID: "f1",
		CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}).Error)
	require.NoError(t, db.Create(&uModel.APIToken{
		ID: 9, UserID: stagedUserID, Name: "long", Token: "apitokenhash-secret",
		CreatedAt: time.Now(),
	}).Error)

	// Make the final user-row delete fail inside TX2.
	const callbackName = "test_fail_user_delete"
	require.NoError(t, db.Callback().Delete().Before("gorm:delete").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement != nil && tx.Statement.Table == "users" {
			tx.AddError(errors.New("forced user delete failure"))
		}
	}))

	_, err := service.DeleteOwnAccount(context.Background(), stagedUserID, nil)
	require.Error(t, err)
	require.NoError(t, db.Callback().Delete().Remove(callbackName))

	// Account and manifest source rows survive; fence released.
	require.Equal(t, int64(1), countUsers(t, db, stagedUserID))
	require.Equal(t, int64(1), countStorageFiles(t, db, stagedUserID))
	var user uModel.User
	require.NoError(t, db.First(&user, stagedUserID).Error)
	require.Nil(t, user.DeletingAt)

	record := requireRecord(t, db, stagedUserID)
	require.Equal(t, uModel.DeletionStatusFailed, record.Status)
	require.NotEmpty(t, record.LastError)

	// Revoked credentials are not restored.
	var sessions int64
	db.Model(&uModel.UserSession{}).Where("user_id = ? AND revoked_at IS NULL", stagedUserID).Count(&sessions)
	require.Zero(t, sessions)
	require.Equal(t, int64(0), countAPITokens(t, db, stagedUserID))

	// Retry re-arms the same row and completes.
	result, err := service.DeleteOwnAccount(context.Background(), stagedUserID, nil)
	require.NoError(t, err)
	require.True(t, result.Success)
	require.Equal(t, uModel.DeletionStatusCompleted, requireRecord(t, db, stagedUserID).Status)
	require.Equal(t, 2, requireRecord(t, db, stagedUserID).Attempts)
	require.Equal(t, int64(1), countRecords(t, db), "the same lifecycle row is reused")
}

// A fence record left by a crashed process is taken over on restart: with the
// user still present the database phase (including circle transfer) runs; a
// storage_pending record only needs file retirement.
func TestRecoverPendingDeletions_RestartTakeover(t *testing.T) {
	t.Run("fenced record with user present resumes database and storage", func(t *testing.T) {
		db := setupTestDeletionDB(t)
		stor := createUserWithAttachment(t, db)
		now := time.Now().UTC()

		// A circle the user owns and an admin member who inherits it.
		require.NoError(t, db.Create(&uModel.User{ID: 2, Username: "heir", CreatedAt: now, UpdatedAt: now}).Error)
		require.NoError(t, db.Create(&cModel.Circle{ID: 3, Name: "home", CreatedBy: stagedUserID, CreatedAt: now, InviteCode: "h"}).Error)
		require.NoError(t, db.Create(&cModel.UserCircle{UserID: stagedUserID, CircleID: 3, Role: cModel.UserRoleAdmin, IsActive: true}).Error)
		require.NoError(t, db.Create(&cModel.UserCircle{UserID: 2, CircleID: 3, Role: cModel.UserRoleAdmin, IsActive: true}).Error)

		plan := []CircleTransferOption{{CircleID: 3, NewOwnerID: 2, NewOwnerName: "heir"}}
		fenced := &uModel.AccountDeletion{
			UserID: stagedUserID, Username: "leaver", Status: uModel.DeletionStatusFenced,
			TransferPlanJSON:    jsonMust(plan),
			DBObjectCountsJSON:  `{"user":1}`,
			StorageFilesJSON:    jsonMust([]uModel.DeletionFileEntry{{Path: stagedFilePath, SizeBytes: 1234}}),
			FileCount:           1,
			CreatedAt:           now, FencedAt: now, Attempts: 1, LastAttemptAt: &now,
		}
		require.NoError(t, db.Create(fenced).Error)

		service := NewDeletionService(db, stor)
		service.RecoverPendingDeletions(context.Background())

		require.Equal(t, uModel.DeletionStatusCompleted, requireRecord(t, db, stagedUserID).Status)
		require.Zero(t, countUsers(t, db, stagedUserID))
		var circle cModel.Circle
		require.NoError(t, db.First(&circle, 3).Error)
		require.Equal(t, 2, circle.CreatedBy, "ownership transfer completed during recovery")
		require.Equal(t, []string{stagedFilePath}, stor.ensuredPaths)
	})

	t.Run("storage_pending record retires files without storage_files rows", func(t *testing.T) {
		db := setupTestDeletionDB(t)
		stor := createUserWithAttachment(t, db)
		// Database phase already committed: user and file rows removed.
		require.NoError(t, db.Delete(&uModel.User{}, stagedUserID).Error)
		require.NoError(t, db.Where("user_id = ?", stagedUserID).Delete(&storageModel.StorageFile{}).Error)
		committed := time.Now().UTC()
		require.NoError(t, db.Create(&uModel.AccountDeletion{
			UserID: stagedUserID, Username: "leaver", Status: uModel.DeletionStatusStoragePending,
			StorageFilesJSON: jsonMust([]uModel.DeletionFileEntry{{Path: stagedFilePath, SizeBytes: 1234}}),
			FileCount:        1, DBCommittedAt: &committed,
			CreatedAt: committed, FencedAt: committed, Attempts: 1, LastAttemptAt: &committed,
		}).Error)

		service := NewDeletionService(db, stor)
		service.RecoverPendingDeletions(context.Background())

		require.Equal(t, uModel.DeletionStatusCompleted, requireRecord(t, db, stagedUserID).Status)
		require.Equal(t, []string{stagedFilePath}, stor.ensuredPaths)
	})
}

// Duplicate/concurrent deletion requests converge to one completed record.
func TestDeleteOwnAccount_ConcurrentRequestsConverge(t *testing.T) {
	db := setupTestDeletionDB(t)
	stor := createUserWithAttachment(t, db)
	service := newStagedDeletionService(db, stor)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	oks := make([]bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			res, err := service.DeleteOwnAccount(context.Background(), stagedUserID, nil)
			errs[idx] = err
			oks[idx] = err == nil && res.Success
		}(i)
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.True(t, oks[0] && oks[1])
	require.Equal(t, int64(1), countRecords(t, db))
	require.Equal(t, uModel.DeletionStatusCompleted, requireRecord(t, db, stagedUserID).Status)
}

// Staged flow transfers owned circles using the saved plan.
func TestDeleteOwnAccount_StagedTransfersCircleOwnership(t *testing.T) {
	db := setupTestDeletionDB(t)
	stor := createUserWithAttachment(t, db)
	now := time.Now().UTC()
	require.NoError(t, db.Create(&uModel.User{ID: 2, Username: "heir", CreatedAt: now, UpdatedAt: now}).Error)
	require.NoError(t, db.Create(&cModel.Circle{ID: 3, Name: "home", CreatedBy: stagedUserID, CreatedAt: now, InviteCode: "h"}).Error)
	require.NoError(t, db.Create(&cModel.UserCircle{UserID: stagedUserID, CircleID: 3, Role: cModel.UserRoleAdmin, IsActive: true}).Error)
	require.NoError(t, db.Create(&cModel.UserCircle{UserID: 2, CircleID: 3, Role: cModel.UserRoleAdmin, IsActive: true}).Error)
	service := newStagedDeletionService(db, stor)

	// Without options the read-only pre-check asks for a transfer and arms
	// nothing.
	pre, err := service.DeleteOwnAccount(context.Background(), stagedUserID, nil)
	require.NoError(t, err)
	require.True(t, pre.RequiresTransfer)
	require.Nil(t, requireRecordOrNil(t, db, stagedUserID))
	require.Equal(t, int64(1), countUsers(t, db, stagedUserID))

	// An invalid heir (not an active admin) also arms nothing.
	bad, err := service.DeleteOwnAccount(context.Background(), stagedUserID,
		[]CircleTransferOption{{CircleID: 3, NewOwnerID: 999, NewOwnerName: "ghost"}})
	require.NoError(t, err)
	require.True(t, bad.RequiresTransfer)
	require.Nil(t, requireRecordOrNil(t, db, stagedUserID))

	// Valid option drives the full staged deletion.
	res, err := service.DeleteOwnAccount(context.Background(), stagedUserID,
		[]CircleTransferOption{{CircleID: 3, NewOwnerID: 2, NewOwnerName: "heir"}})
	require.NoError(t, err)
	require.True(t, res.Success, res.Message)

	var circle cModel.Circle
	require.NoError(t, db.First(&circle, 3).Error)
	require.Equal(t, 2, circle.CreatedBy)
	require.Equal(t, uModel.DeletionStatusCompleted, requireRecord(t, db, stagedUserID).Status)
}

// Accounts without attachments keep the legacy path: no lifecycle record is
// created (same for the dry-run check and admin-driven child deletion, which
// keep calling DeleteUserAccount directly).
func TestDeleteOwnAccount_NoAttachmentsUsesLegacyPath(t *testing.T) {
	db := setupTestDeletionDB(t)
	now := time.Now().UTC()
	require.NoError(t, db.Create(&uModel.User{
		ID: stagedUserID, Username: "plain", Email: "p@example.com",
		CreatedAt: now, UpdatedAt: now,
	}).Error)
	service := NewDeletionService(db, &mockStorage{})

	res, err := service.DeleteOwnAccount(context.Background(), stagedUserID, nil)
	require.NoError(t, err)
	require.True(t, res.Success)
	require.Zero(t, countRecords(t, db))
	require.Zero(t, countUsers(t, db, stagedUserID))
}

// The deletion fence is observable through the repository and blocks token
// issuance for accounts mid-deletion until it is released.
func TestAccountDeletionFence_RepositoryState(t *testing.T) {
	db := setupTestDeletionDB(t)
	repo := uRepo.NewUserRepository(db, &config.Config{})
	now := time.Now().UTC()
	require.NoError(t, db.Create(&uModel.User{ID: stagedUserID, Username: "fenced", CreatedAt: now, UpdatedAt: now}).Error)

	deleting, err := repo.IsAccountBeingDeleted(context.Background(), stagedUserID)
	require.NoError(t, err)
	require.False(t, deleting)

	require.NoError(t, db.Model(&uModel.User{}).Where("id = ?", stagedUserID).Update("deleting_at", now).Error)
	deleting, err = repo.IsAccountBeingDeleted(context.Background(), stagedUserID)
	require.NoError(t, err)
	require.True(t, deleting)

	require.NoError(t, db.Model(&uModel.User{}).Where("id = ?", stagedUserID).Updates(map[string]interface{}{"deleting_at": nil}).Error)
	deleting, err = repo.IsAccountBeingDeleted(context.Background(), stagedUserID)
	require.NoError(t, err)
	require.False(t, deleting)
}

// A failed record whose account is later deleted through the attachment-free
// legacy path converges to completed instead of lingering.
func TestDeleteOwnAccount_FailedRecordConvergesViaLegacyPath(t *testing.T) {
	db := setupTestDeletionDB(t)
	now := time.Now().UTC()
	require.NoError(t, db.Create(&uModel.User{
		ID: stagedUserID, Username: "plain", Email: "p@example.com",
		CreatedAt: now, UpdatedAt: now,
	}).Error)
	require.NoError(t, db.Create(&uModel.AccountDeletion{
		UserID: stagedUserID, Username: "plain", Status: uModel.DeletionStatusFailed,
		LastError: databaseErrorMessage, CreatedAt: now, FencedAt: now, Attempts: 1,
	}).Error)
	service := NewDeletionService(db, &mockStorage{})

	res, err := service.DeleteOwnAccount(context.Background(), stagedUserID, nil)
	require.NoError(t, err)
	require.True(t, res.Success)
	require.Zero(t, countUsers(t, db, stagedUserID))
	require.Equal(t, uModel.DeletionStatusCompleted, requireRecord(t, db, stagedUserID).Status)
}

// ---- helpers ----

func requireRecord(t *testing.T, db *gorm.DB, userID int) uModel.AccountDeletion {
	t.Helper()
	var record uModel.AccountDeletion
	require.NoError(t, db.Where("user_id = ?", userID).First(&record).Error)
	return record
}

func requireRecordOrNil(t *testing.T, db *gorm.DB, userID int) *uModel.AccountDeletion {
	t.Helper()
	var record uModel.AccountDeletion
	err := db.Where("user_id = ?", userID).First(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	require.NoError(t, err)
	return &record
}

func countRecords(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Model(&uModel.AccountDeletion{}).Count(&n).Error)
	return n
}

func countUsers(t *testing.T, db *gorm.DB, userID int) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Model(&uModel.User{}).Where("id = ?", userID).Count(&n).Error)
	return n
}

func countStorageFiles(t *testing.T, db *gorm.DB, userID int) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Model(&storageModel.StorageFile{}).Where("user_id = ?", userID).Count(&n).Error)
	return n
}

func countAPITokens(t *testing.T, db *gorm.DB, userID int) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Model(&uModel.APIToken{}).Where("user_id = ?", userID).Count(&n).Error)
	return n
}

func rawRecordText(t *testing.T, db *gorm.DB, userID int) string {
	t.Helper()
	var row map[string]interface{}
	require.NoError(t, db.Table("account_deletions").Where("user_id = ?", userID).Take(&row).Error)
	encoded, err := json.Marshal(row)
	require.NoError(t, err)
	return string(encoded)
}

func jsonMust(value interface{}) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("json marshal: %v", err))
	}
	return string(encoded)
}
