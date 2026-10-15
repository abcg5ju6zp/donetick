package chore

import (
	"context"
	"errors"
	"time"

	chModel "donetick.com/core/internal/chore/model"
	cModel "donetick.com/core/internal/circle/model"
	stModel "donetick.com/core/internal/subtask/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Completion requiring approval is modelled as a generation: one immutable
// pending ChoreHistory row. Approval and rejection both consume that exact
// generation exactly once; every settlement side effect (points, recurrence
// state, subtask reset, Thing actions, external delivery outbox) shares the
// same conditional, transactional consume. A retried, delayed or duplicate
// decision can never settle the generation a second time.

var (
	// ErrChoreAlreadyPending means a completion generation already exists for
	// the chore, so a duplicate completion submission is rejected deterministically.
	ErrChoreAlreadyPending = errors.New("chore is already pending approval")
	// ErrGenerationNotFound means no pending generation exists (and no previous
	// decision is available to replay).
	ErrGenerationNotFound = errors.New("pending approval generation not found")
)

// GenerationAlreadyDecided is returned when a generation has already been
// consumed. Status is the consuming decision (completed or rejected), so
// callers can return a deterministic result without re-settling anything.
type GenerationAlreadyDecided struct {
	Status chModel.ChoreHistoryStatus
}

func (e *GenerationAlreadyDecided) Error() string {
	switch e.Status {
	case chModel.ChoreHistoryStatusCompleted:
		return "completion generation was already approved"
	case chModel.ChoreHistoryStatusRejected:
		return "completion generation was already rejected"
	default:
		return "completion generation was already decided"
	}
}

// PendingApprovalInput carries everything a completion submission freezes
// into the new generation.
type PendingApprovalInput struct {
	Chore          *chModel.Chore
	Note           *string
	UserID         int        // who completed the chore
	CompletedDate  *time.Time // when it was completed
	NextDueDate    *time.Time // next due date computed at submission
	NextAssignedTo *int       // next executor chosen at submission
	ApplyPoints    bool       // whether points are enabled
}

// SetChorePendingApproval records a new pending completion generation. The
// generation locks the chore version, completer, completion time, next
// executor, next due date and points; the chore atomatically enters the
// pending-approval state. A duplicate submission while a generation is open
// returns ErrChoreAlreadyPending without writing anything.
func (r *ChoreRepository) SetChorePendingApproval(c context.Context, in PendingApprovalInput) (*chModel.ChoreHistory, error) {
	nextVersion, err := r.nextSyncVersion(c, in.Chore.CircleID)
	if err != nil {
		return nil, err
	}

	var history *chModel.ChoreHistory
	err = r.db.WithContext(c).Transaction(func(tx *gorm.DB) error {
		var locked chModel.Chore
		query := tx.WithContext(c)
		if r.dbType == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := query.Select("id", "status", "circle_id").First(&locked, in.Chore.ID).Error; err != nil {
			return err
		}
		if locked.Status == chModel.ChoreStatusPendingApproval {
			return ErrChoreAlreadyPending
		}

		// Reuse the "started" history row (an open timer session) like a direct
		// completion does; otherwise open a new generation row.
		var existing chModel.ChoreHistory
		findErr := tx.Where("chore_id = ? AND status = ?", in.Chore.ID, chModel.ChoreHistoryStatusStarted).
			First(&existing).Error

		var ch *chModel.ChoreHistory
		switch {
		case findErr == nil:
			existing.PerformedAt = in.CompletedDate
			existing.Note = in.Note
			existing.Status = chModel.ChoreHistoryStatusPendingApproval
			ch = &existing
		case errors.Is(findErr, gorm.ErrRecordNotFound):
			ch = &chModel.ChoreHistory{
				ChoreID:     in.Chore.ID,
				PerformedAt: in.CompletedDate,
				CompletedBy: in.UserID,
				AssignedTo:  in.Chore.AssignedTo,
				DueDate:     in.Chore.NextDueDate,
				Note:        in.Note,
				Status:      chModel.ChoreHistoryStatusPendingApproval,
			}
		default:
			return findErr
		}

		// Freeze the settlement plan onto the generation so that approval is a
		// deterministic replay of the submission, whenever the request arrives.
		ch.LockedChoreVersion = in.Chore.SyncVersion
		ch.LockedNextDueDate = in.NextDueDate
		ch.LockedNextAssignedTo = in.NextAssignedTo
		if in.ApplyPoints && in.Chore.Points != nil && *in.Chore.Points > 0 {
			points := *in.Chore.Points
			ch.LockedPoints = &points
		}
		ch.SyncVersion = nextVersion
		if err := tx.Save(ch).Error; err != nil {
			return err
		}

		// Guarded transition: a concurrent submission/decision loses.
		result := tx.Model(&chModel.Chore{}).
			Where("id = ? AND status <> ?", in.Chore.ID, chModel.ChoreStatusPendingApproval).
			Updates(map[string]interface{}{
				"status":       chModel.ChoreStatusPendingApproval,
				"sync_version": nextVersion,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrChoreAlreadyPending
		}

		// Any running timer session is finished as part of the submission, like
		// a direct completion.
		var timeSessions []*chModel.TimeSession
		tx.Model(&chModel.TimeSession{}).Where("chore_id = ? AND status < ?", in.Chore.ID, chModel.TimeSessionStatusCompleted).Find(&timeSessions)
		if len(timeSessions) != 0 {
			for _, session := range timeSessions {
				session.Finish(in.UserID)
			}
			if err := tx.Save(&timeSessions).Error; err != nil {
				return err
			}
		}

		history = ch
		return nil
	})
	if err != nil {
		return nil, err
	}
	return history, nil
}

// ApproveInput identifies the generation to approve and the settlement hooks.
type ApproveInput struct {
	Chore        *chModel.Chore
	GenerationID *int // optional explicit history id; defaults to the open generation
	AdminUserID  int
	ApplyPoints  bool
	// OnSettled runs inside the settlement transaction, only when this call is
	// the one consuming the generation. Use it to enqueue transactional side
	// effects (e.g. external delivery outbox rows).
	OnSettled func(tx *gorm.DB, outcome *ApprovalOutcome) error
}

// ApprovalOutcome is the single settlement result shared by points, timer
// sessions, subtask reset and recurrence state.
type ApprovalOutcome struct {
	History *chModel.ChoreHistory
	Chore   *chModel.Chore // scalar post-settlement chore state
}

// ApproveChore consumes one pending generation. Points, recurrence, subtask
// reset and Thing completion actions all commit together, at most once. A
// duplicate approval returns *GenerationAlreadyDecided (Status=Completed);
// approving a generation a rejection (or a later decision) consumed returns
// Status=Rejected.
func (r *ChoreRepository) ApproveChore(c context.Context, in ApproveInput) (*ApprovalOutcome, error) {
	var outcome *ApprovalOutcome
	err := r.db.WithContext(c).Transaction(func(tx *gorm.DB) error {
		locked, err := lockChore(tx, r.dbType, in.Chore.ID)
		if err != nil {
			return err
		}

		generation, err := resolveGeneration(tx, in.Chore.ID, in.GenerationID)
		if err != nil {
			return err
		}

		version, err := r.nextSyncVersionWithDB(c, tx, locked.CircleID)
		if err != nil {
			return err
		}
		now := time.Now().UTC()

		// The locked amount wins; fall back to the chore points for generations
		// recorded before locking existed.
		points := generation.LockedPoints
		if points == nil && in.ApplyPoints {
			points = locked.Points
		}

		historyUpdates := map[string]interface{}{
			"status":       chModel.ChoreHistoryStatusCompleted,
			"sync_version": version,
			"decided_by":   in.AdminUserID,
			"decided_at":   now,
			"updated_at":   now,
		}
		if in.ApplyPoints && points != nil && *points > 0 {
			historyUpdates["points"] = *points
		}
		consume := tx.Model(&chModel.ChoreHistory{}).
			Where("id = ? AND status = ?", generation.ID, chModel.ChoreHistoryStatusPendingApproval).
			Updates(historyUpdates)
		if consume.Error != nil {
			return consume.Error
		}
		if consume.RowsAffected == 0 {
			return reloadDecision(tx, generation.ID)
		}

		if in.ApplyPoints && points != nil && *points > 0 {
			if err := tx.Model(&cModel.UserCircle{}).
				Where("user_id = ? AND circle_id = ?", generation.CompletedBy, locked.CircleID).
				Update("points", gorm.Expr("points + ?", *points)).Error; err != nil {
				return err
			}
		}

		choreUpdates := map[string]interface{}{
			"next_due_date": generation.LockedNextDueDate,
			"status":        chModel.ChoreStatusNoStatus,
			"sync_version":  version,
		}
		settledChore := *locked
		settledChore.Status = chModel.ChoreStatusNoStatus
		settledChore.SyncVersion = version
		if generation.LockedNextDueDate != nil {
			choreUpdates["assigned_to"] = generation.LockedNextAssignedTo
			settledChore.NextDueDate = generation.LockedNextDueDate
			settledChore.AssignedTo = generation.LockedNextAssignedTo
		} else {
			// one-time / trigger / always chores compute no next due date and archive
			choreUpdates["is_active"] = false
			settledChore.IsActive = false
		}

		choreResult := tx.Model(&chModel.Chore{}).
			Where("id = ? AND status = ?", in.Chore.ID, chModel.ChoreStatusPendingApproval).
			Updates(choreUpdates)
		if choreResult.Error != nil {
			return choreResult.Error
		}
		if choreResult.RowsAffected == 0 {
			// A concurrent decision consumed the chore state; roll back and let
			// the caller replay against the committed decision.
			return reloadDecision(tx, generation.ID)
		}

		// Subtask reset shares the settlement transaction.
		if locked.FrequencyType != chModel.FrequencyTypeOnce {
			if err := tx.Model(&stModel.SubTask{}).Where("chore_id = ?", in.Chore.ID).
				Updates(map[string]interface{}{"completed_at": nil, "completed_by": nil}).Error; err != nil {
				return err
			}
		}

		if err := r.applyCompletionActions(c, tx, in.Chore); err != nil {
			return err
		}

		generation.Status = chModel.ChoreHistoryStatusCompleted
		generation.SyncVersion = version
		generation.DecidedBy = &in.AdminUserID
		generation.DecidedAt = &now
		if in.ApplyPoints && points != nil && *points > 0 {
			generation.Points = points
		}

		outcome = &ApprovalOutcome{History: generation, Chore: &settledChore}
		if in.OnSettled != nil {
			return in.OnSettled(tx, outcome)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return outcome, nil
}

// RejectInput identifies the generation to reject.
type RejectInput struct {
	ChoreID       int
	CircleID      int
	GenerationID  *int // optional explicit history id; defaults to the open generation
	AdminUserID   int
	RejectionNote *string
}

// RejectChore consumes one pending generation as rejected. Only the generation
// row and the chore's pending status change; no points, recurrence, subtask or
// Thing effect runs. A duplicate rejection returns *GenerationAlreadyDecided
// (Status=Rejected); rejecting an approved generation returns Status=Completed.
func (r *ChoreRepository) RejectChore(c context.Context, in RejectInput) (*chModel.ChoreHistory, error) {
	var history *chModel.ChoreHistory
	err := r.db.WithContext(c).Transaction(func(tx *gorm.DB) error {
		locked, err := lockChore(tx, r.dbType, in.ChoreID)
		if err != nil {
			return err
		}

		generation, err := resolveGeneration(tx, in.ChoreID, in.GenerationID)
		if err != nil {
			return err
		}

		version, err := r.nextSyncVersionWithDB(c, tx, locked.CircleID)
		if err != nil {
			return err
		}
		now := time.Now().UTC()

		consume := tx.Model(&chModel.ChoreHistory{}).
			Where("id = ? AND status = ?", generation.ID, chModel.ChoreHistoryStatusPendingApproval).
			Updates(map[string]interface{}{
				"status":       chModel.ChoreHistoryStatusRejected,
				"sync_version": version,
				"decided_by":   in.AdminUserID,
				"decided_at":   now,
				"updated_at":   now,
			})
		if consume.Error != nil {
			return consume.Error
		}
		if consume.RowsAffected == 0 {
			return reloadDecision(tx, generation.ID)
		}

		choreResult := tx.Model(&chModel.Chore{}).
			Where("id = ? AND status = ?", in.ChoreID, chModel.ChoreStatusPendingApproval).
			Updates(map[string]interface{}{
				"status":       chModel.ChoreStatusNoStatus,
				"sync_version": version,
			})
		if choreResult.Error != nil {
			return choreResult.Error
		}
		if choreResult.RowsAffected == 0 {
			return reloadDecision(tx, generation.ID)
		}

		generation.Status = chModel.ChoreHistoryStatusRejected
		generation.SyncVersion = version
		generation.DecidedBy = &in.AdminUserID
		generation.DecidedAt = &now
		history = generation
		return nil
	})
	if err != nil {
		return nil, err
	}
	return history, nil
}

// lockChore reads the chore row under a row lock within tx, so concurrent
// approve/reject calls serialize on the same generation.
func lockChore(tx *gorm.DB, dbType string, choreID int) (*chModel.Chore, error) {
	query := tx
	if dbType == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var locked chModel.Chore
	if err := query.First(&locked, choreID).Error; err != nil {
		return nil, err
	}
	return &locked, nil
}

// resolveGeneration finds the generation a decision targets. With an explicit
// id it validates ownership of that exact generation; without one it takes the
// currently open generation, falling back to the latest decision so a retried
// request after restart/lost response gets a deterministic result.
func resolveGeneration(tx *gorm.DB, choreID int, generationID *int) (*chModel.ChoreHistory, error) {
	if generationID != nil {
		var generation chModel.ChoreHistory
		if err := tx.Where("id = ? AND chore_id = ?", *generationID, choreID).First(&generation).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, ErrGenerationNotFound
			}
			return nil, err
		}
		if generation.Status != chModel.ChoreHistoryStatusPendingApproval {
			return nil, &GenerationAlreadyDecided{Status: generation.Status}
		}
		return &generation, nil
	}

	var generation chModel.ChoreHistory
	err := tx.Where("chore_id = ? AND status = ?", choreID, chModel.ChoreHistoryStatusPendingApproval).
		Order("id desc").First(&generation).Error
	if err == nil {
		return &generation, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	// No open generation: replay the most recent decision if one exists.
	var latest chModel.ChoreHistory
	latestErr := tx.Where("chore_id = ? AND status IN ?", choreID,
		[]chModel.ChoreHistoryStatus{chModel.ChoreHistoryStatusCompleted, chModel.ChoreHistoryStatusRejected}).
		Order("id desc").First(&latest).Error
	if latestErr == nil {
		return nil, &GenerationAlreadyDecided{Status: latest.Status}
	}
	if errors.Is(latestErr, gorm.ErrRecordNotFound) {
		return nil, ErrGenerationNotFound
	}
	return nil, latestErr
}

// reloadDecision reads a generation after losing a conditional update and
// translates its current status into a decided error.
func reloadDecision(tx *gorm.DB, generationID int) error {
	var current chModel.ChoreHistory
	if err := tx.Select("id", "status").First(&current, generationID).Error; err != nil {
		return err
	}
	if current.Status == chModel.ChoreHistoryStatusPendingApproval {
		// The generation is still open but the chore moved (e.g. another generation
		// path): treat as not found rather than silently settling.
		return ErrGenerationNotFound
	}
	return &GenerationAlreadyDecided{Status: current.Status}
}
