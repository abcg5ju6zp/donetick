package user

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	uModel "donetick.com/core/internal/user/model"
	"gorm.io/gorm"
)

// Staged, resumable account deletion.
//
// Flow for a self-service deletion where the account owns external files:
//
//  1. Read-only circle-transfer pre-check (no side effects).
//  2. armDeletion (TX1, committed independently): persist user snapshot,
//     transfer plan, database-object manifest and external-file manifest;
//     raise the deleting_at fence; revoke refresh sessions and delete
//     long-lived API tokens (irreversible).
//  3. runStagedDatabasePhase (TX2): transfer circle ownership, remove child
//     accounts, run the shared deletion steps, delete the user row. Failure
//     rolls back completely; the fence is then released so the account and
//     original ownership stay usable (revoked credentials stay revoked).
//  4. After TX2 commits, retire storage objects one by one from the manifest,
//     checkpointing the remainder. Failures leave a secret-free
//     storage_pending record for the background reaper / retries.
//
// Recovery reconciles the record with database facts (does the user row still
// exist?) rather than trusting the status column alone, so a crash between
// commit and status flip is handled correctly.

const deletionReapInterval = 10 * time.Minute

var (
	// errDeletionAlreadyFenced means the deleting_at compare-and-set lost to a
	// concurrent arm; the caller reloads the record and converges.
	errDeletionAlreadyFenced = errors.New("account deletion already in progress")

	// storageErrorMessage is the only error text persisted on the record. It
	// deliberately contains no driver error (which may embed SQL/values) or
	// credential material; the full error is logged server-side.
	storageErrorMessage = "external object storage was unavailable; remaining files are queued for background cleanup"
	databaseErrorMessage = "database deletion phase failed; account data and circle ownership were kept"
)

func (s *DeletionService) lockForUser(userID int) *sync.Mutex {
	v, _ := s.locks.LoadOrStore(userID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// DeleteOwnAccount is the self-service account-deletion entry point.
//
// Accounts without attachments keep using the legacy single-transaction path;
// accounts with files use the resumable staged flow. Repeated calls and lost
// responses converge on the single AccountDeletion row for the user.
func (s *DeletionService) DeleteOwnAccount(ctx context.Context, userID int, transferOptions []CircleTransferOption) (*DeletionResult, error) {
	mu := s.lockForUser(userID)
	mu.Lock()
	defer mu.Unlock()

	// Converge on an existing lifecycle first: a retried or duplicate request
	// must resume the in-flight deletion, never start a second one.
	rec, err := s.getDeletionRecord(userID)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		switch rec.Status {
		case uModel.DeletionStatusFenced, uModel.DeletionStatusStoragePending:
			return s.driveStagedDeletion(ctx, rec)
		case uModel.DeletionStatusCompleted:
			return s.deletionResultFromRecord(rec), nil
		case uModel.DeletionStatusFailed:
			// Account is usable again; this is a fresh request that re-arms.
		default:
			// Unknown status: treat as resumable to be safe.
			return s.driveStagedDeletion(ctx, rec)
		}
	}

	// Read-only transfer requirement pre-check (same behaviour as legacy).
	circlesOwned, err := s.getOwnedCircles(s.db, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to check owned circles: %w", err)
	}
	if len(circlesOwned) > 0 && len(transferOptions) == 0 {
		options, err := s.getTransferOptions(s.db, circlesOwned)
		if err != nil {
			return nil, fmt.Errorf("failed to get transfer options: %w", err)
		}
		return &DeletionResult{
			DeletedData:      map[string]int{},
			RequiresTransfer: true,
			TransferOptions:  options,
			CirclesLeft:      circlesOwned,
			Message:          "Account deletion requires circle ownership transfer",
		}, nil
	}

	// No external objects: nothing to orphan, keep the legacy transactional
	// path (also used by dry runs and admin-driven child-account deletion).
	var fileCount int64
	if err := s.db.Table("storage_files").Where("user_id = ?", userID).Count(&fileCount).Error; err != nil {
		return nil, fmt.Errorf("failed to check attachments: %w", err)
	}
	if fileCount == 0 {
		result, err := s.deleteUserAccount(ctx, userID, transferOptions, false)
		// Converge a stale row from an earlier failed attempt: if the account
		// is now gone, its lifecycle is complete.
		if err == nil && result != nil && result.Success && rec != nil &&
			rec.Status == uModel.DeletionStatusFailed {
			now := time.Now().UTC()
			if updateErr := s.db.Model(&uModel.AccountDeletion{}).Where("user_id = ?", userID).
				Updates(map[string]interface{}{
					"status":          uModel.DeletionStatusCompleted,
					"db_committed_at": now,
					"completed_at":    now,
					"last_attempt_at": now,
				}).Error; updateErr != nil {
				log.Printf("Warning: failed to converge deletion record for user %d: %v", userID, updateErr)
			}
		}
		return result, err
	}

	rec, preResult, err := s.armDeletion(ctx, userID, transferOptions)
	if errors.Is(err, errDeletionAlreadyFenced) {
		// Concurrent arm won the CAS; join its lifecycle.
		existing, getErr := s.getDeletionRecord(userID)
		if getErr != nil {
			return nil, getErr
		}
		if existing != nil {
			return s.driveStagedDeletion(ctx, existing)
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	if preResult != nil {
		return preResult, nil
	}
	return s.driveStagedDeletion(ctx, rec)
}

// driveStagedDeletion advances a record to completion based on its status AND
// database facts. It is idempotent and safe to invoke from the API path and
// the background reaper.
func (s *DeletionService) driveStagedDeletion(ctx context.Context, rec *uModel.AccountDeletion) (*DeletionResult, error) {
	if rec.Status == uModel.DeletionStatusCompleted {
		return s.deletionResultFromRecord(rec), nil
	}

	// Database phase is owed while the user row still exists. A fenced record
	// whose user is gone means TX2 committed but the process died before the
	// status flip: skip straight to file retirement.
	if rec.Status != uModel.DeletionStatusStoragePending {
		var user uModel.User
		findErr := s.db.Select("id").First(&user, rec.UserID).Error
		switch {
		case findErr == nil:
			deleted, phaseErr := s.runStagedDatabasePhase(ctx, rec)
			if phaseErr != nil {
				s.failStagedDeletion(rec, phaseErr)
				return nil, fmt.Errorf("failed to complete account deletion database phase: %w", phaseErr)
			}
			s.markStoragePending(rec, deleted)
		case errors.Is(findErr, gorm.ErrRecordNotFound):
			s.markStoragePending(rec, nil)
		default:
			return nil, fmt.Errorf("failed to determine account deletion state: %w", findErr)
		}
	}

	if rec.Status == uModel.DeletionStatusStoragePending {
		s.retireStagedFiles(ctx, rec)
	}

	return s.deletionResultFromRecord(rec), nil
}

// armDeletion runs TX1: fence + manifests + credential revocation.
//
// Return values:
//   - rec: the persisted record (on success),
//   - preResult: a non-destructive RequiresTransfer answer (tx rolled back),
//   - err: errDeletionAlreadyFenced on a lost CAS, or a real error.
func (s *DeletionService) armDeletion(ctx context.Context, userID int, transferOptions []CircleTransferOption) (recOut *uModel.AccountDeletion, preResult *DeletionResult, errOut error) {
	tx := s.db.Begin()
	if tx.Error != nil {
		return nil, nil, fmt.Errorf("failed to start transaction: %w", tx.Error)
	}
	defer tx.Rollback()

	now := time.Now().UTC()

	// Atomic fence claim. Works on both SQLite and Postgres (no row locking).
	cas := tx.Model(&uModel.User{}).
		Where("id = ? AND deleting_at IS NULL", userID).
		Update("deleting_at", now)
	if cas.Error != nil {
		return nil, nil, cas.Error
	}
	if cas.RowsAffected == 0 {
		return nil, nil, errDeletionAlreadyFenced
	}

	var user uModel.User
	if err := tx.First(&user, userID).Error; err != nil {
		return nil, nil, fmt.Errorf("failed to load user for deletion manifest: %w", err)
	}

	owned, err := s.getOwnedCircles(tx, userID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to check owned circles: %w", err)
	}
	plan, requiresResult, err := s.buildTransferPlan(tx, owned, transferOptions)
	if err != nil {
		return nil, nil, err
	}
	if requiresResult != nil {
		return nil, requiresResult, nil
	}

	// Database-object manifest (same numbers the dry-run impact report shows).
	objectCounts := make(map[string]int)
	for _, step := range s.allDeletionSteps() {
		count, countErr := step.count(tx, userID)
		if countErr != nil {
			return nil, nil, fmt.Errorf("failed to count %s: %w", step.name, countErr)
		}
		if count > 0 {
			objectCounts[step.name] = count
		}
	}
	objectCounts["user"] = 1

	// External-file manifest (object keys only, never credentials).
	var files []uModel.DeletionFileEntry
	if err := tx.Table("storage_files").
		Where("user_id = ?", userID).
		Order("file_path").
		Find(&files).Error; err != nil {
		return nil, nil, fmt.Errorf("failed to capture attachment manifest: %w", err)
	}

	// Revoke login sessions and long-lived credentials. This is irreversible
	// even if the database phase later rolls back: old sessions must not
	// outlive the deletion fence.
	if err := tx.Model(&uModel.UserSession{}).
		Where("user_id = ? AND revoked_at IS NULL", userID).
		Update("revoked_at", now).Error; err != nil {
		return nil, nil, fmt.Errorf("failed to revoke sessions: %w", err)
	}
	if _, err := s.safeDelete(tx, "DELETE FROM api_tokens WHERE user_id = ?", userID); err != nil {
		return nil, nil, fmt.Errorf("failed to revoke API tokens: %w", err)
	}
	if _, err := s.safeDelete(tx, "DELETE FROM mfa_sessions WHERE user_id = ?", userID); err != nil {
		return nil, nil, fmt.Errorf("failed to revoke MFA sessions: %w", err)
	}

	snapshot := uModel.DeletionUserSnapshot{
		ID:          user.ID,
		Username:    user.Username,
		DisplayName: user.DisplayName,
		CircleID:    user.CircleID,
		UserType:    int(user.UserType),
	}

	// Upsert the lifecycle row (a failed prior attempt reuses the same row).
	var record uModel.AccountDeletion
	findErr := tx.Where("user_id = ?", userID).First(&record).Error
	isNew := errors.Is(findErr, gorm.ErrRecordNotFound)
	if findErr != nil && !isNew {
		return nil, nil, fmt.Errorf("failed to load deletion record: %w", findErr)
	}

	record.UserID = userID
	record.Username = user.Username
	record.Status = uModel.DeletionStatusFenced
	record.UserSnapshotJSON = mustJSON(snapshot)
	record.TransferPlanJSON = mustJSON(plan)
	record.DBObjectCountsJSON = mustJSON(objectCounts)
	record.StorageFilesJSON = mustJSON(files)
	record.DeletedCountsJSON = ""
	record.FileCount = len(files)
	record.LastError = ""
	record.Attempts++
	record.FencedAt = now
	record.DBCommittedAt = nil
	record.CompletedAt = nil
	record.LastAttemptAt = &now
	if isNew {
		record.CreatedAt = now
		if err := tx.Create(&record).Error; err != nil {
			return nil, nil, fmt.Errorf("failed to persist deletion record: %w", err)
		}
	} else {
		if err := tx.Save(&record).Error; err != nil {
			return nil, nil, fmt.Errorf("failed to persist deletion record: %w", err)
		}
	}

	if err := tx.Commit().Error; err != nil {
		return nil, nil, fmt.Errorf("failed to commit deletion fence: %w", err)
	}
	return &record, nil, nil
}

// buildTransferPlan validates the requested transfers inside the fence tx.
// An invalid or missing choice yields the same RequiresTransfer answer the
// un-fenced pre-check gives, so the caller rolls back without side effects.
func (s *DeletionService) buildTransferPlan(tx *gorm.DB, owned []CircleTransferInfo, transferOptions []CircleTransferOption) ([]CircleTransferOption, *DeletionResult, error) {
	if len(owned) == 0 {
		return nil, nil, nil
	}

	byCircle := make(map[int]CircleTransferOption, len(transferOptions))
	for _, option := range transferOptions {
		byCircle[option.CircleID] = option
	}

	plan := make([]CircleTransferOption, 0, len(owned))
	for _, circle := range owned {
		option, ok := byCircle[circle.CircleID]
		if !ok {
			options, err := s.getTransferOptions(tx, owned)
			if err != nil {
				return nil, nil, err
			}
			return nil, transferRequiredResult(owned, options), nil
		}
		// The new owner must still be an active admin of the circle and not
		// itself in the middle of deletion.
		var valid int64
		if err := tx.Raw(`
			SELECT COUNT(1)
			FROM user_circles uc
			JOIN users u ON u.id = uc.user_id
			WHERE uc.circle_id = ? AND uc.user_id = ?
			  AND uc.role = 'admin' AND uc.is_active = true
			  AND u.deleting_at IS NULL`,
			circle.CircleID, option.NewOwnerID).Scan(&valid).Error; err != nil {
			return nil, nil, err
		}
		if valid == 0 {
			options, err := s.getTransferOptions(tx, owned)
			if err != nil {
				return nil, nil, err
			}
			return nil, transferRequiredResult(owned, options), nil
		}
		if option.NewOwnerName == "" {
			option.NewOwnerName = fmt.Sprintf("user %d", option.NewOwnerID)
		}
		plan = append(plan, option)
	}
	return plan, nil, nil
}

func transferRequiredResult(owned []CircleTransferInfo, options []CircleTransferOption) *DeletionResult {
	return &DeletionResult{
		DeletedData:      map[string]int{},
		RequiresTransfer: true,
		TransferOptions:  options,
		CirclesLeft:      owned,
		Message:          "Account deletion requires circle ownership transfer",
	}
}

// runStagedDatabasePhase is TX2: ownership transfer, child accounts, shared
// cleanup steps, user row. Everything is one transaction, so any failure
// leaves the account and original ownership intact.
func (s *DeletionService) runStagedDatabasePhase(ctx context.Context, rec *uModel.AccountDeletion) (map[string]int, error) {
	var plan []CircleTransferOption
	if rec.TransferPlanJSON != "" {
		if err := json.Unmarshal([]byte(rec.TransferPlanJSON), &plan); err != nil {
			return nil, fmt.Errorf("failed to read transfer plan: %w", err)
		}
	}

	tx := s.db.Begin()
	if tx.Error != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", tx.Error)
	}
	defer tx.Rollback()

	deleted := make(map[string]int)
	userID := rec.UserID

	// Transfer ownership using the saved plan with guards: do not overwrite a
	// circle this user no longer owns, and re-confirm the target is eligible.
	for _, option := range plan {
		var valid int64
		if err := tx.Raw(`
			SELECT COUNT(1)
			FROM user_circles uc
			JOIN users u ON u.id = uc.user_id
			WHERE uc.circle_id = ? AND uc.user_id = ?
			  AND uc.role = 'admin' AND uc.is_active = true
			  AND u.deleting_at IS NULL`,
			option.CircleID, option.NewOwnerID).Scan(&valid).Error; err != nil {
			return nil, fmt.Errorf("failed to validate circle %d transfer: %w", option.CircleID, err)
		}
		if valid == 0 {
			return nil, fmt.Errorf("chosen new owner %d for circle %d is no longer an active admin", option.NewOwnerID, option.CircleID)
		}
		result := tx.Exec("UPDATE circles SET created_by = ? WHERE id = ? AND created_by = ?", option.NewOwnerID, option.CircleID, userID)
		if result.Error != nil {
			return nil, fmt.Errorf("failed to transfer circle %d: %w", option.CircleID, result.Error)
		}
		if result.RowsAffected == 0 {
			// Resumed attempt after a partial state: already transferred to the
			// planned owner is fine; anything else is a conflict.
			var currentOwner int
			if err := tx.Table("circles").Where("id = ?", option.CircleID).Pluck("created_by", &currentOwner).Error; err != nil {
				return nil, err
			}
			if currentOwner != option.NewOwnerID {
				return nil, fmt.Errorf("circle %d is no longer owned by the deleted user", option.CircleID)
			}
		}
	}

	// Child accounts keep using the existing (separate transaction) path.
	childUsers, err := s.getChildUsers(tx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get child users: %w", err)
	}
	for _, childUserID := range childUsers {
		childResult, childErr := s.deleteUserAccount(ctx, childUserID, nil, false)
		if childErr != nil {
			return nil, fmt.Errorf("failed to delete child user %d: %w", childUserID, childErr)
		}
		for key, count := range childResult.DeletedData {
			deleted[key] += count
		}
	}

	for _, step := range s.allDeletionSteps() {
		count, stepErr := step.run(tx, userID)
		if stepErr != nil {
			return nil, fmt.Errorf("failed to delete %s: %w", step.name, stepErr)
		}
		if count > 0 {
			deleted[step.name] = count
		}
	}

	userResult := tx.Delete(&uModel.User{}, userID)
	if userResult.Error != nil {
		return nil, fmt.Errorf("failed to delete user record: %w", userResult.Error)
	}
	if userResult.RowsAffected == 0 {
		return nil, fmt.Errorf("user record %d was already removed", userID)
	}
	deleted["user"] = int(userResult.RowsAffected)

	if err := tx.Commit().Error; err != nil {
		return nil, fmt.Errorf("failed to commit account deletion: %w", err)
	}
	return deleted, nil
}

// markStoragePending records that the database phase committed. Safe to call
// when recovering a fenced record whose user row is already gone.
func (s *DeletionService) markStoragePending(rec *uModel.AccountDeletion, deleted map[string]int) {
	now := time.Now().UTC()
	updates := map[string]interface{}{
		"status":          uModel.DeletionStatusStoragePending,
		"db_committed_at": now,
		"last_attempt_at": now,
	}
	if deleted != nil {
		updates["deleted_counts"] = mustJSON(deleted)
	}
	if err := s.db.Model(&uModel.AccountDeletion{}).Where("user_id = ? AND status <> ?",
		rec.UserID, uModel.DeletionStatusCompleted).Updates(updates).Error; err != nil {
		log.Printf("Warning: failed to advance deletion record for user %d to storage_pending: %v", rec.UserID, err)
	}
	rec.Status = uModel.DeletionStatusStoragePending
	rec.DBCommittedAt = &now
	if deleted != nil {
		rec.DeletedCountsJSON = mustJSON(deleted)
	}
}

// failStagedDeletion releases the fence and records a secret-free failure. The
// account and circle ownership remain usable; revoked credentials stay
// revoked by design.
func (s *DeletionService) failStagedDeletion(rec *uModel.AccountDeletion, cause error) {
	log.Printf("Account deletion database phase failed for user %d (record %d): %v", rec.UserID, rec.ID, cause)
	now := time.Now().UTC()
	if err := s.db.Model(&uModel.AccountDeletion{}).Where("user_id = ?", rec.UserID).Updates(map[string]interface{}{
		"status":          uModel.DeletionStatusFailed,
		"last_error":      databaseErrorMessage,
		"last_attempt_at": now,
	}).Error; err != nil {
		log.Printf("Warning: failed to mark deletion record failed for user %d: %v", rec.UserID, err)
	}
	if err := s.db.Model(&uModel.User{}).Where("id = ?", rec.UserID).
		Updates(map[string]interface{}{"deleting_at": nil}).Error; err != nil {
		log.Printf("Warning: failed to release deletion fence for user %d: %v", rec.UserID, err)
	}
	rec.Status = uModel.DeletionStatusFailed
}

// retireStagedFiles removes external objects from the stored manifest after
// the database commit. The remaining list is checkpointed after every
// successful removal, and EnsureDeleted treats missing objects as success, so
// crashes and retries converge. The record never stores driver errors.
func (s *DeletionService) retireStagedFiles(ctx context.Context, rec *uModel.AccountDeletion) {
	var files []uModel.DeletionFileEntry
	if rec.StorageFilesJSON != "" {
		if err := json.Unmarshal([]byte(rec.StorageFilesJSON), &files); err != nil {
			log.Printf("Warning: storage manifest for deleted user %d is corrupt, leaving record pending: %v", rec.UserID, err)
			return
		}
	}

	now := time.Now().UTC()
	remaining := make([]uModel.DeletionFileEntry, 0)
	for i, file := range files {
		if s.storage == nil {
			// No storage backend configured: nothing to retire (legacy parity).
			continue
		}
		if err := s.storage.EnsureDeleted(ctx, file.Path); err != nil {
			log.Printf("Warning: failed to retire attachment %q for deleted user %d: %v", file.Path, rec.UserID, err)
			remaining = append(remaining, file)
			continue
		}
		// Checkpoint the survivors: earlier failures plus everything not tried
		// yet, so a crash never loses track of an object.
		pending := append(append([]uModel.DeletionFileEntry{}, remaining...), files[i+1:]...)
		rec.StorageFilesJSON = mustJSON(pending)
		rec.LastAttemptAt = &now
		if err := s.db.Model(&uModel.AccountDeletion{}).Where("user_id = ?", rec.UserID).
			Updates(map[string]interface{}{"storage_files": rec.StorageFilesJSON, "last_attempt_at": now}).Error; err != nil {
			log.Printf("Warning: failed to checkpoint storage retirement for user %d: %v", rec.UserID, err)
		}
	}

	if len(remaining) == 0 {
		completedAt := time.Now().UTC()
		rec.Status = uModel.DeletionStatusCompleted
		rec.StorageFilesJSON = "[]"
		rec.CompletedAt = &completedAt
		rec.LastError = ""
		if err := s.db.Model(&uModel.AccountDeletion{}).Where("user_id = ?", rec.UserID).Updates(map[string]interface{}{
			"status":        uModel.DeletionStatusCompleted,
			"storage_files": "[]",
			"completed_at":  completedAt,
			"last_error":    "",
		}).Error; err != nil {
			log.Printf("Warning: failed to mark deletion completed for user %d: %v", rec.UserID, err)
		}
		return
	}

	rec.Status = uModel.DeletionStatusStoragePending
	rec.StorageFilesJSON = mustJSON(remaining)
	rec.LastError = storageErrorMessage
	rec.LastAttemptAt = &now
	if err := s.db.Model(&uModel.AccountDeletion{}).Where("user_id = ?", rec.UserID).Updates(map[string]interface{}{
		"status":          uModel.DeletionStatusStoragePending,
		"storage_files":   rec.StorageFilesJSON,
		"last_error":      storageErrorMessage,
		"last_attempt_at": now,
	}).Error; err != nil {
		log.Printf("Warning: failed to persist pending storage cleanup for user %d: %v", rec.UserID, err)
	}
}

func (s *DeletionService) getDeletionRecord(userID int) (*uModel.AccountDeletion, error) {
	var record uModel.AccountDeletion
	err := s.db.Where("user_id = ?", userID).First(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load deletion record: %w", err)
	}
	return &record, nil
}

func (s *DeletionService) deletionResultFromRecord(rec *uModel.AccountDeletion) *DeletionResult {
	deleted := make(map[string]int)
	if rec.DeletedCountsJSON != "" {
		_ = json.Unmarshal([]byte(rec.DeletedCountsJSON), &deleted)
	}
	if len(deleted) == 0 && rec.DBObjectCountsJSON != "" {
		_ = json.Unmarshal([]byte(rec.DBObjectCountsJSON), &deleted)
	}

	result := &DeletionResult{
		DeletedData: deleted,
	}
	switch rec.Status {
	case uModel.DeletionStatusCompleted:
		result.Success = true
		result.Message = "Account deleted successfully"
	case uModel.DeletionStatusStoragePending:
		// Database is authoritative for account existence: the account is gone.
		result.Success = true
		result.StorageCleanupPending = true
		result.Message = "Account deleted; remaining attachments are being removed by background cleanup"
	default:
		result.Message = "Account deletion is in progress"
	}
	return result
}

func mustJSON(value interface{}) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "[]"
	}
	return string(encoded)
}

// Start launches the background recovery sweep: once immediately (to take over
// interrupted deletions after a process restart) and then on a fixed interval.
func (s *DeletionService) Start(ctx context.Context) {
	if s.reaperStop != nil {
		return
	}
	stop := make(chan struct{})
	s.reaperStop = stop

	go func() {
		// Startup takeover, off the lifecycle hook.
		s.RecoverPendingDeletions(ctx)

		ticker := time.NewTicker(deletionReapInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				s.RecoverPendingDeletions(ctx)
			}
		}
	}()
}

// Stop halts the background recovery sweep.
func (s *DeletionService) Stop() {
	if s.reaperStop != nil {
		close(s.reaperStop)
		s.reaperStop = nil
	}
}

// RecoverPendingDeletions finds non-terminal deletion records and drives each
// forward according to database facts and storage results.
func (s *DeletionService) RecoverPendingDeletions(ctx context.Context) {
	var records []uModel.AccountDeletion
	if err := s.db.Where("status IN ?", []string{
		uModel.DeletionStatusFenced,
		uModel.DeletionStatusStoragePending,
	}).Order("id ASC").Limit(100).Find(&records).Error; err != nil {
		log.Printf("Warning: failed to query resumable account deletions: %v", err)
		return
	}

	for i := range records {
		record := records[i]
		func() {
			mu := s.lockForUser(record.UserID)
			mu.Lock()
			defer mu.Unlock()

			// Re-read under the per-user lock so an API request can't race us.
			fresh, err := s.getDeletionRecord(record.UserID)
			if err != nil || fresh == nil {
				return
			}
			if fresh.Status != uModel.DeletionStatusFenced && fresh.Status != uModel.DeletionStatusStoragePending {
				return
			}
			if _, err := s.driveStagedDeletion(ctx, fresh); err != nil {
				log.Printf("Warning: background recovery for deleted user %d did not finish: %v", record.UserID, err)
			}
		}()
	}
}
