package project

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"donetick.com/core/config"
	chModel "donetick.com/core/internal/chore/model"
	chRepo "donetick.com/core/internal/chore/repo"
	cModel "donetick.com/core/internal/circle/model"
	"donetick.com/core/internal/database"
	pModel "donetick.com/core/internal/project/model"
	pjRepo "donetick.com/core/internal/project/repo"
	uModel "donetick.com/core/internal/user/model"
)

func newProcessorTest(t *testing.T) (*AccessProcessor, *pjRepo.ProjectRepository, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "access.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, database.Migration(db))

	cfg := &config.Config{}
	cfg.Database.Type = "sqlite"
	choreRepository := chRepo.NewChoreRepository(db, cfg)
	projectRepository := pjRepo.NewProjectRepository(db, cfg, choreRepository)
	for _, id := range []int{1, 2, 3} {
		require.NoError(t, db.Create(&cModel.UserCircle{UserID: id, CircleID: 1, IsActive: true}).Error)
	}
	processor := NewAccessProcessor(projectRepository, choreRepository, nil, nil)
	return processor, projectRepository, db
}

type plannerCall struct {
	choreID int
	version int64
}

type fakePlanner struct {
	mu    sync.Mutex
	calls []plannerCall
	fail  bool
}

func (f *fakePlanner) GenerateNotificationsWithAccessVersion(_ context.Context, chore *chModel.Chore, accessVersion int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, plannerCall{choreID: chore.ID, version: accessVersion})
	return !f.fail
}

func (f *fakePlanner) callsSnapshot() []plannerCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]plannerCall(nil), f.calls...)
}

type revokeCall struct {
	circleID, choreID int
	name              string
	actorID           int
	accessVersion     int64
	users             []int
}

type refreshCall struct {
	circleID, projectID int
	accessVersion       int64
	changed             bool
	actorID             int
	recipients          []int
}

type fakeBroadcaster struct {
	mu          sync.Mutex
	revocations []revokeCall
	refreshes   []refreshCall
}

func (b *fakeBroadcaster) BroadcastChoreAccessRevoked(circleID, choreID int, name string, user *uModel.User, accessVersion int64, versions map[int]int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	users := make([]int, 0, len(versions))
	for id := range versions {
		users = append(users, id)
	}
	b.revocations = append(b.revocations, revokeCall{
		circleID: circleID, choreID: choreID, name: name, actorID: user.ID,
		accessVersion: accessVersion, users: users,
	})
}

func (b *fakeBroadcaster) BroadcastProjectUpdated(circleID, projectID int, accessVersion int64, changed bool, user *uModel.User, recipients []int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refreshes = append(b.refreshes, refreshCall{
		circleID: circleID, projectID: projectID, accessVersion: accessVersion,
		changed: changed, actorID: user.ID, recipients: append([]int(nil), recipients...),
	})
}

func (b *fakeBroadcaster) snapshot() ([]revokeCall, []refreshCall) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]revokeCall(nil), b.revocations...), append([]refreshCall(nil), b.refreshes...)
}

func membersUpdateReq(base int64, ids ...int) *pModel.ProjectReq {
	vis := pModel.ProjectVisibilityMembers
	return &pModel.ProjectReq{
		Name: "work", Visibility: &vis, MemberIDs: &ids, BaseAccessVersion: &base,
	}
}

// setupSharedProject creates a "members" project shared with user 2 containing
// one private chore assigned to user 2, with user 1 as owner.
func setupSharedProject(t *testing.T, db *gorm.DB, r *pjRepo.ProjectRepository) (*pModel.Project, *chModel.Chore) {
	t.Helper()
	project := &pModel.Project{
		Name: "work", CircleID: 1, CreatedBy: 1,
		IsPrivate: true, SharedWithMembers: true, MemberIDs: []int{2},
	}
	require.NoError(t, r.CreateProject(context.Background(), project))
	assignee := 2
	chore := &chModel.Chore{
		Name: "on site", CircleID: 1, CreatedBy: 1, ProjectID: &project.ID,
		IsPrivate: true, IsActive: true,
		AssignStrategy: chModel.AssignmentStrategyRandom, AssignedTo: &assignee,
	}
	require.NoError(t, db.Create(chore).Error)
	for _, id := range []int{1, 2} {
		require.NoError(t, db.Create(&chModel.ChoreAssignees{ChoreID: chore.ID, UserID: id}).Error)
	}
	return project, chore
}

func narrowToOwner(t *testing.T, r *pjRepo.ProjectRepository, project *pModel.Project, base int64) *chRepo.ProjectAccessResult {
	t.Helper()
	result, err := r.UpdateProjectWithAccess(context.Background(),
		&pModel.Project{ID: project.ID, Name: "work"}, membersUpdateReq(base), 1, 1)
	require.NoError(t, err)
	return result
}

func widenToMember(t *testing.T, r *pjRepo.ProjectRepository, project *pModel.Project, base int64) *chRepo.ProjectAccessResult {
	t.Helper()
	result, err := r.UpdateProjectWithAccess(context.Background(),
		&pModel.Project{ID: project.ID, Name: "work"}, membersUpdateReq(base, 2), 1, 1)
	require.NoError(t, err)
	return result
}

func eventStatus(t *testing.T, db *gorm.DB, eventID int64) pModel.ProjectAccessEvent {
	t.Helper()
	var event pModel.ProjectAccessEvent
	require.NoError(t, db.First(&event, eventID).Error)
	return event
}

// Right after commit the frozen post-commit work runs once: reminders carry the
// new access version, revoked users get the deletion with that version, both
// sides of the change get a refresh, and the outbox row is completed.
func TestAccessProcessorDispatchesVersionedPostProcessing(t *testing.T) {
	processor, r, db := newProcessorTest(t)
	project, chore := setupSharedProject(t, db, r)
	result := narrowToOwner(t, r, project, 0)
	require.Equal(t, int64(1), result.AccessVersion)

	planner := &fakePlanner{}
	broadcaster := &fakeBroadcaster{}
	processor.planner = planner
	processor.broadcaster = broadcaster

	processor.Dispatch(context.Background(), result.EventID)

	require.Equal(t, []plannerCall{{choreID: chore.ID, version: 1}}, planner.callsSnapshot(),
		"reminders are rebuilt once, tagged with the access version")

	revokes, refreshes := broadcaster.snapshot()
	require.Len(t, revokes, 1)
	require.Equal(t, chore.ID, revokes[0].choreID)
	require.Equal(t, []int{2}, revokes[0].users)
	require.Equal(t, int64(1), revokes[0].accessVersion)
	require.Equal(t, "on site", revokes[0].name)
	require.Len(t, refreshes, 1)
	require.Equal(t, int64(1), refreshes[0].accessVersion)
	require.True(t, refreshes[0].changed)
	require.Equal(t, []int{1, 2}, refreshes[0].recipients)

	require.Equal(t, pModel.AccessEventStatusCompleted, eventStatus(t, db, result.EventID).Status)
}

// When another device widened the audience while an old post-processing row was
// pending, the newer version wins: the old row neither rebuilds reminders nor
// re-revokes the restored member (its tombstone is gone), and the newer row does
// the work.
func TestAccessProcessorNewerVersionSuppressesOldRetry(t *testing.T) {
	processor, r, db := newProcessorTest(t)
	project, chore := setupSharedProject(t, db, r)

	first := narrowToOwner(t, r, project, 0)
	second := widenToMember(t, r, project, 1)
	require.Equal(t, int64(2), second.AccessVersion)

	planner := &fakePlanner{}
	broadcaster := &fakeBroadcaster{}
	processor.planner = planner
	processor.broadcaster = broadcaster

	// Replay the stale v1 row after v2 committed.
	processor.Dispatch(context.Background(), first.EventID)
	revokes, refreshes := broadcaster.snapshot()
	require.Empty(t, revokes, "an old event must not re-revoke restored access")
	require.Empty(t, planner.callsSnapshot(), "the newer version owns the reminder rebuild")
	require.Len(t, refreshes, 1)
	require.Equal(t, int64(1), refreshes[0].accessVersion, "the idempotent refresh hint still fires")
	require.Equal(t, pModel.AccessEventStatusCompleted, eventStatus(t, db, first.EventID).Status)

	// The current version performs the rebuild for the granted member.
	processor.Dispatch(context.Background(), second.EventID)
	require.Equal(t, []plannerCall{{choreID: chore.ID, version: 2}}, planner.callsSnapshot())
	revokes, refreshes = broadcaster.snapshot()
	require.Empty(t, revokes)
	require.Len(t, refreshes, 2)
	require.Equal(t, int64(2), refreshes[1].accessVersion)
	require.Equal(t, pModel.AccessEventStatusCompleted, eventStatus(t, db, second.EventID).Status)
}

// Re-dispatching a completed row performs the work at most once.
func TestAccessProcessorDispatchIsIdempotent(t *testing.T) {
	processor, r, db := newProcessorTest(t)
	project, _ := setupSharedProject(t, db, r)
	result := narrowToOwner(t, r, project, 0)

	planner := &fakePlanner{}
	broadcaster := &fakeBroadcaster{}
	processor.planner = planner
	processor.broadcaster = broadcaster

	processor.Dispatch(context.Background(), result.EventID)
	processor.Dispatch(context.Background(), result.EventID)
	processor.ProcessPending(context.Background())

	require.Len(t, planner.callsSnapshot(), 1)
	revokes, refreshes := broadcaster.snapshot()
	require.Len(t, revokes, 1)
	require.Len(t, refreshes, 1)
}

// A failing reminder rebuild leaves the row durable and scheduled for retry; it
// is reclaimed once due, and a fresh processor (as after a restart) sweeps it.
func TestAccessProcessorFailurePersistsAndReplays(t *testing.T) {
	processor, r, db := newProcessorTest(t)
	project, _ := setupSharedProject(t, db, r)
	result := narrowToOwner(t, r, project, 0)

	planner := &fakePlanner{fail: true}
	processor.planner = planner
	processor.broadcaster = &fakeBroadcaster{}

	processor.Dispatch(context.Background(), result.EventID)
	row := eventStatus(t, db, result.EventID)
	require.Equal(t, pModel.AccessEventStatusPending, row.Status)
	require.Equal(t, 1, row.Attempts)
	require.True(t, row.RunAfter.After(time.Now()))
	require.NotEmpty(t, row.LastError)

	// The backoff has not elapsed: the sweep does not claim the row.
	require.NoError(t, processor.ProcessPending(context.Background()))
	require.Equal(t, 1, eventStatus(t, db, result.EventID).Attempts)

	// Backoff elapses (or a restart runs the sweep): the row is retried.
	require.NoError(t, db.Model(&pModel.ProjectAccessEvent{}).Where("id = ?", result.EventID).
		Update("run_after", time.Now().UTC().Add(-time.Minute)).Error)
	require.NoError(t, processor.ProcessPending(context.Background()))
	require.Equal(t, 2, eventStatus(t, db, result.EventID).Attempts)
	require.Equal(t, pModel.AccessEventStatusPending, eventStatus(t, db, result.EventID).Status)

	// A brand-new processor simulates the restart: it picks the same row up.
	restarted := NewAccessProcessor(r, processor.choreRepo, nil, nil)
	restartedPlanner := &fakePlanner{fail: false}
	restarted.planner = restartedPlanner
	require.NoError(t, db.Model(&pModel.ProjectAccessEvent{}).Where("id = ?", result.EventID).
		Update("run_after", time.Now().UTC().Add(-time.Minute)).Error)
	require.NoError(t, restarted.ProcessPending(context.Background()))
	require.Equal(t, pModel.AccessEventStatusCompleted, eventStatus(t, db, result.EventID).Status)
	require.Len(t, restartedPlanner.callsSnapshot(), 1)
	require.Equal(t, int64(1), restartedPlanner.callsSnapshot()[0].version)
}

// With neither planner nor broadcaster the row still completes, so disabling
// realtime features does not stall the outbox.
func TestAccessProcessorCompletesWithoutOptionalDependencies(t *testing.T) {
	processor, r, db := newProcessorTest(t)
	project, _ := setupSharedProject(t, db, r)
	result := narrowToOwner(t, r, project, 0)

	processor.Dispatch(context.Background(), result.EventID)
	require.Equal(t, pModel.AccessEventStatusCompleted, eventStatus(t, db, result.EventID).Status)
}
