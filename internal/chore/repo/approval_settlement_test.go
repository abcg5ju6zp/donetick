package chore

import (
	"context"
	"errors"
	"testing"
	"time"

	chModel "donetick.com/core/internal/chore/model"
	cModel "donetick.com/core/internal/circle/model"
	stModel "donetick.com/core/internal/subtask/model"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func requireDecided(t *testing.T, err error, status chModel.ChoreHistoryStatus) {
	t.Helper()
	var decided *GenerationAlreadyDecided
	require.ErrorAs(t, err, &decided)
	require.Equal(t, status, decided.Status)
}

// setupApprovalChore creates an active daily, approval-required chore with
// points and a UserCircle membership to receive them.
func setupApprovalChore(t *testing.T, db *gorm.DB, points int) *chModel.Chore {
	t.Helper()
	require.NoError(t, db.Create(&cModel.UserCircle{
		UserID: testOwnerID, CircleID: testCircleID, Role: cModel.UserRoleAdmin, IsActive: true, Points: 0,
	}).Error)
	due := time.Now().UTC().Add(-time.Hour)
	chore := createChore(t, db, "approval chore", testOwnerID, false, nil)
	chore.FrequencyType = chModel.FrequencyTypeDaily
	chore.RequireApproval = true
	chore.Status = chModel.ChoreStatusNoStatus
	chore.Points = &points
	chore.NextDueDate = &due
	chore.AssignedTo = intPtr(testOwnerID)
	require.NoError(t, db.Save(chore).Error)
	return chore
}

// submitGeneration records a pending completion with a locked settlement plan.
func submitGeneration(t *testing.T, r *ChoreRepository, chore *chModel.Chore) (*chModel.ChoreHistory, time.Time, time.Time, int) {
	t.Helper()
	completedAt := time.Now().UTC()
	nextDue := completedAt.Add(24 * time.Hour)
	nextAssignee := testOwnerID + 100
	history, err := r.SetChorePendingApproval(context.Background(), PendingApprovalInput{
		Chore:          chore,
		UserID:         testOwnerID,
		CompletedDate:  &completedAt,
		NextDueDate:    &nextDue,
		NextAssignedTo: &nextAssignee,
		ApplyPoints:    true,
	})
	require.NoError(t, err)
	require.NotNil(t, history.PerformedAt)
	require.True(t, history.PerformedAt.Equal(completedAt))
	require.NotNil(t, history.LockedNextDueDate)
	require.True(t, history.LockedNextDueDate.Equal(nextDue))
	require.NotNil(t, history.LockedNextAssignedTo)
	require.Equal(t, nextAssignee, *history.LockedNextAssignedTo)
	if chore.Points != nil && *chore.Points > 0 {
		require.NotNil(t, history.LockedPoints)
		require.Equal(t, *chore.Points, *history.LockedPoints)
	}
	return history, completedAt, nextDue, nextAssignee
}

func userPoints(t *testing.T, db *gorm.DB) int {
	t.Helper()
	var membership cModel.UserCircle
	require.NoError(t, db.Where("user_id = ? AND circle_id = ?", testOwnerID, testCircleID).First(&membership).Error)
	return membership.Points
}

// Approving a generation settles every side effect exactly once; a duplicate
// approval is a deterministic replay that grants no points and does not rotate
// or advance the chore again.
func TestApprovalSettlesGenerationOnce(t *testing.T) {
	r, db := newTestChoreRepo(t)
	chore := setupApprovalChore(t, db, 10)
	generation, _, nextDue, nextAssignee := submitGeneration(t, r, chore)

	require.Equal(t, int64(0), generation.LockedChoreVersion, "locks the chore version the member completed against")

	onSettledCalls := 0
	outcome, err := r.ApproveChore(context.Background(), ApproveInput{
		Chore:       chore,
		AdminUserID: testOwnerID,
		ApplyPoints: true,
		OnSettled: func(tx *gorm.DB, settled *ApprovalOutcome) error {
			onSettledCalls++
			return nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, generation.ID, outcome.History.ID)
	require.Equal(t, 1, onSettledCalls)

	// Points granted once, recurrence advanced to the locked plan.
	require.Equal(t, 10, userPoints(t, db))
	var settled chModel.Chore
	require.NoError(t, db.First(&settled, chore.ID).Error)
	require.Equal(t, chModel.ChoreStatusNoStatus, settled.Status)
	require.True(t, settled.NextDueDate.Equal(nextDue), "next due date comes from the locked generation")
	require.NotNil(t, settled.AssignedTo)
	require.Equal(t, nextAssignee, *settled.AssignedTo, "next executor comes from the locked generation")
	require.True(t, settled.IsActive, "recurring chore stays active")

	var history chModel.ChoreHistory
	require.NoError(t, db.First(&history, generation.ID).Error)
	require.Equal(t, chModel.ChoreHistoryStatusCompleted, history.Status)
	require.NotNil(t, history.Points)
	require.Equal(t, 10, *history.Points)
	require.NotNil(t, history.DecidedBy)
	require.Equal(t, testOwnerID, *history.DecidedBy)

	// Duplicate approval after the response was lost (or service restarted):
	// same history identity, deterministic replay, no second settlement.
	replay, err := r.ApproveChore(context.Background(), ApproveInput{
		Chore:        chore,
		GenerationID: &generation.ID,
		AdminUserID:  testOwnerID,
		ApplyPoints:  true,
		OnSettled:    func(tx *gorm.DB, o *ApprovalOutcome) error { onSettledCalls++; return nil },
	})
	require.Nil(t, replay)
	requireDecided(t, err, chModel.ChoreHistoryStatusCompleted)
	require.Equal(t, 1, onSettledCalls, "replay never runs settlement hooks")
	require.Equal(t, 10, userPoints(t, db), "no duplicate points")
	var again chModel.Chore
	require.NoError(t, db.First(&again, chore.ID).Error)
	require.True(t, again.NextDueDate.Equal(nextDue), "due date not advanced again")
	require.Equal(t, settled.SyncVersion, again.SyncVersion)
}

// A duplicate completion submission while a generation is open is rejected
// deterministically instead of creating a second pending row.
func TestDuplicatePendingSubmissionRejected(t *testing.T) {
	r, db := newTestChoreRepo(t)
	chore := setupApprovalChore(t, db, 5)
	generation, _, _, _ := submitGeneration(t, r, chore)

	_, err := r.SetChorePendingApproval(context.Background(), PendingApprovalInput{
		Chore:         chore,
		UserID:        testOwnerID,
		CompletedDate: ptrTime(time.Now().UTC()),
		ApplyPoints:   true,
	})
	require.ErrorIs(t, err, ErrChoreAlreadyPending)

	var count int64
	require.NoError(t, db.Model(&chModel.ChoreHistory{}).Where("chore_id = ?", chore.ID).Count(&count).Error)
	require.Equal(t, int64(1), count)
	require.Equal(t, 0, userPoints(t, db))

	// The settled plan uses the locked points/assignee even if the chore is
	// edited while awaiting approval.
	require.NoError(t, db.Model(&chModel.Chore{}).Where("id = ?", chore.ID).
		Updates(map[string]interface{}{"points": 99, "assigned_to": testOtherID}).Error)
	outcome, err := r.ApproveChore(context.Background(), ApproveInput{
		Chore:        chore,
		GenerationID: &generation.ID,
		AdminUserID:  testOwnerID,
		ApplyPoints:  true,
	})
	require.NoError(t, err)
	require.NotNil(t, outcome.History.Points)
	require.Equal(t, 5, *outcome.History.Points, "locked points, not the edited value")
	require.Equal(t, 5, userPoints(t, db))
}

// After rejection, completing again opens a brand-new generation; a stale
// approval of the rejected generation can never take effect late.
func TestRejectionThenNewGeneration(t *testing.T) {
	r, db := newTestChoreRepo(t)
	chore := setupApprovalChore(t, db, 10)
	oldGeneration, _, _, _ := submitGeneration(t, r, chore)

	oldHistory, err := r.RejectChore(context.Background(), RejectInput{
		ChoreID: chore.ID, CircleID: chore.CircleID, AdminUserID: testOwnerID,
	})
	require.NoError(t, err)
	require.Equal(t, oldGeneration.ID, oldHistory.ID)
	require.Equal(t, 0, userPoints(t, db))

	// Idempotent rejection: second call replays, changes nothing.
	_, err = r.RejectChore(context.Background(), RejectInput{
		ChoreID: chore.ID, CircleID: chore.CircleID, GenerationID: &oldGeneration.ID, AdminUserID: testOwnerID,
	})
	requireDecided(t, err, chModel.ChoreHistoryStatusRejected)
	require.Equal(t, 0, userPoints(t, db))

	// A stale approval of the rejected generation must not settle anything.
	_, err = r.ApproveChore(context.Background(), ApproveInput{
		Chore: chore, GenerationID: &oldGeneration.ID, AdminUserID: testOwnerID, ApplyPoints: true,
	})
	requireDecided(t, err, chModel.ChoreHistoryStatusRejected)
	require.Equal(t, 0, userPoints(t, db))

	// Completing again creates a new generation.
	require.NoError(t, db.First(chore, chore.ID).Error)
	newGeneration, _, nextDue, nextAssignee := submitGeneration(t, r, chore)
	require.NotEqual(t, oldGeneration.ID, newGeneration.ID)

	var oldRow chModel.ChoreHistory
	require.NoError(t, db.First(&oldRow, oldGeneration.ID).Error)
	require.Equal(t, chModel.ChoreHistoryStatusRejected, oldRow.Status)

	// Approving the new generation settles it once.
	outcome, err := r.ApproveChore(context.Background(), ApproveInput{
		Chore: chore, GenerationID: &newGeneration.ID, AdminUserID: testOwnerID, ApplyPoints: true,
	})
	require.NoError(t, err)
	require.Equal(t, newGeneration.ID, outcome.History.ID)
	require.Equal(t, 10, userPoints(t, db))

	var settled chModel.Chore
	require.NoError(t, db.First(&settled, chore.ID).Error)
	require.True(t, settled.NextDueDate.Equal(nextDue))
	require.NotNil(t, settled.AssignedTo)
	require.Equal(t, nextAssignee, *settled.AssignedTo)

	// A late duplicate of the OLD approval still replays as rejected.
	_, err = r.ApproveChore(context.Background(), ApproveInput{
		Chore: chore, GenerationID: &oldGeneration.ID, AdminUserID: testOwnerID, ApplyPoints: true,
	})
	requireDecided(t, err, chModel.ChoreHistoryStatusRejected)
	require.Equal(t, 10, userPoints(t, db), "the old generation cannot add points late")
}

// Approve and reject are mutually exclusive per generation, in either order.
func TestApproveRejectMutuallyExclusive(t *testing.T) {
	r, db := newTestChoreRepo(t)

	// approve wins, reject loses
	approved := setupApprovalChore(t, db, 7)
	genA, _, _, _ := submitGeneration(t, r, approved)
	_, err := r.ApproveChore(context.Background(), ApproveInput{Chore: approved, GenerationID: &genA.ID, AdminUserID: testOwnerID, ApplyPoints: true})
	require.NoError(t, err)
	_, err = r.RejectChore(context.Background(), RejectInput{ChoreID: approved.ID, CircleID: approved.CircleID, GenerationID: &genA.ID, AdminUserID: testOwnerID})
	requireDecided(t, err, chModel.ChoreHistoryStatusCompleted)
	require.Equal(t, 7, userPoints(t, db))

	// reject wins, approve loses (different chore/membership in the same db)
	require.NoError(t, db.Create(&cModel.UserCircle{
		UserID: testOtherID, CircleID: testCircleID, Role: cModel.UserRoleMember, IsActive: true,
	}).Error)
	rejected := createChore(t, db, "rejected", testOwnerID, false, nil)
	rejected.FrequencyType = chModel.FrequencyTypeDaily
	rejected.RequireApproval = true
	rejected.AssignedTo = intPtr(testOtherID)
	require.NoError(t, db.Save(rejected).Error)

	completedAt := time.Now().UTC()
	genB, err := r.SetChorePendingApproval(context.Background(), PendingApprovalInput{
		Chore: rejected, UserID: testOtherID, CompletedDate: &completedAt,
		NextDueDate: ptrTime(completedAt.Add(24 * time.Hour)), ApplyPoints: true,
	})
	require.NoError(t, err)
	_, err = r.RejectChore(context.Background(), RejectInput{ChoreID: rejected.ID, CircleID: rejected.CircleID, GenerationID: &genB.ID, AdminUserID: testOwnerID})
	require.NoError(t, err)
	_, err = r.ApproveChore(context.Background(), ApproveInput{Chore: rejected, GenerationID: &genB.ID, AdminUserID: testOwnerID, ApplyPoints: true})
	requireDecided(t, err, chModel.ChoreHistoryStatusRejected)

	var other cModel.UserCircle
	require.NoError(t, db.Where("user_id = ?", testOtherID).First(&other).Error)
	require.Zero(t, other.Points, "rejected completion grants no points")
}

// Subtask reset is part of the settlement transaction: it runs on approval of
// a recurring chore, and never on a replay.
func TestSubtaskResetSharesSettlement(t *testing.T) {
	r, db := newTestChoreRepo(t)
	chore := setupApprovalChore(t, db, 0)
	generation, _, _, _ := submitGeneration(t, r, chore)

	completed := time.Now().UTC()
	require.NoError(t, db.Create(&stModel.SubTask{ChoreID: chore.ID, Name: "s1", CompletedAt: &completed, CompletedBy: testOwnerID}).Error)
	require.NoError(t, db.Create(&stModel.SubTask{ChoreID: chore.ID, Name: "s2", CompletedAt: &completed, CompletedBy: testOwnerID}).Error)

	_, err := r.ApproveChore(context.Background(), ApproveInput{Chore: chore, GenerationID: &generation.ID, AdminUserID: testOwnerID})
	require.NoError(t, err)
	var subtasks []stModel.SubTask
	require.NoError(t, db.Where("chore_id = ?", chore.ID).Find(&subtasks).Error)
	for _, s := range subtasks {
		require.Nil(t, s.CompletedAt, "subtask %s reset with settlement", s.Name)
		require.Zero(t, s.CompletedBy, "subtask %s reset with settlement", s.Name)
	}
}

// When a settlement hook fails, the whole settlement rolls back: no points,
// no recurrence advance, generation still open for retry.
func TestSettlementHookFailureRollsBack(t *testing.T) {
	r, db := newTestChoreRepo(t)
	chore := setupApprovalChore(t, db, 10)
	generation, _, _, _ := submitGeneration(t, r, chore)

	hookErr := errors.New("outbox unavailable")
	_, err := r.ApproveChore(context.Background(), ApproveInput{
		Chore: chore, GenerationID: &generation.ID, AdminUserID: testOwnerID, ApplyPoints: true,
		OnSettled: func(tx *gorm.DB, settled *ApprovalOutcome) error { return hookErr },
	})
	require.ErrorIs(t, err, hookErr)
	require.Equal(t, 0, userPoints(t, db))

	var still chModel.Chore
	require.NoError(t, db.First(&still, chore.ID).Error)
	require.Equal(t, chModel.ChoreStatusPendingApproval, still.Status, "nothing committed")
	var pending chModel.ChoreHistory
	require.NoError(t, db.First(&pending, generation.ID).Error)
	require.Equal(t, chModel.ChoreHistoryStatusPendingApproval, pending.Status)

	// Retrying with a healthy hook settles exactly once.
	_, err = r.ApproveChore(context.Background(), ApproveInput{
		Chore: chore, GenerationID: &generation.ID, AdminUserID: testOwnerID, ApplyPoints: true,
	})
	require.NoError(t, err)
	require.Equal(t, 10, userPoints(t, db))
}

// Approving with no explicit generation replays the most recent decision after
// restart/lost response, instead of erroring with a missing pending row.
func TestApproveWithoutIDReplaysLatestDecision(t *testing.T) {
	r, db := newTestChoreRepo(t)
	chore := setupApprovalChore(t, db, 3)
	generation, _, _, _ := submitGeneration(t, r, chore)

	require.NoError(t, db.First(chore, chore.ID).Error)
	_, err := r.RejectChore(context.Background(), RejectInput{ChoreID: chore.ID, CircleID: chore.CircleID, AdminUserID: testOwnerID})
	require.NoError(t, err)
	_ = generation

	_, err = r.ApproveChore(context.Background(), ApproveInput{Chore: chore, AdminUserID: testOwnerID, ApplyPoints: true})
	requireDecided(t, err, chModel.ChoreHistoryStatusRejected)
}

func ptrTime(t time.Time) *time.Time { return &t }
