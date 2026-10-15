package repo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	chModel "donetick.com/core/internal/chore/model"
	nModel "donetick.com/core/internal/notifier/model"
	pModel "donetick.com/core/internal/project/model"
	syncModel "donetick.com/core/internal/sync/model"
)

func membersReq(ids ...int) *pModel.ProjectReq {
	vis := pModel.ProjectVisibilityMembers
	return &pModel.ProjectReq{
		Name:              "work",
		Visibility:        &vis,
		MemberIDs:         idsPtr(ids...),
		BaseAccessVersion: nil,
	}
}

func membersReqAt(base int64, ids ...int) *pModel.ProjectReq {
	req := membersReq(ids...)
	req.BaseAccessVersion = &base
	return req
}

func ptr[T any](v T) *T { return &v }

// A narrowing audience update takes the project's next monotonic access
// version inside the transaction; the task, the tombstone and the durable
// post-processing row all carry it. Widening again takes the next version and
// cleans the old tombstones.
func TestAudienceUpdateAssignsMonotonicAccessVersion(t *testing.T) {
	r, db := newTestRepo(t)
	seedCircle(t, db)
	ctx := context.Background()
	work := newSharedProject(t, r, "work", pModel.ProjectVisibilityMembers, memberID)
	chore := newProjectChore(t, db, work.ID, "on site", ownerID, chModel.AssignmentStrategyRandom, intp(memberID), ownerID, memberID)
	history := addHistory(t, db, chore.ID, memberID, "installed")
	require.NoError(t, db.Create(&nModel.Notification{ChoreID: chore.ID, CircleID: sharingCirc, UserID: memberID, Text: "reminder"}).Error)

	preview, err := r.PreviewProjectAccess(ctx, work.ID, membersReq(), ownerID, sharingCirc)
	require.NoError(t, err)
	require.Equal(t, int64(0), preview.AccessVersion, "baseline of a never-versioned project is 0")

	result, err := r.UpdateProjectWithAccess(ctx, &pModel.Project{ID: work.ID, Name: "work"}, membersReqAt(0), ownerID, sharingCirc)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.AccessVersion)
	require.NotZero(t, result.EventID)
	require.Contains(t, result.Revocations[chore.ID], memberID)

	var updated pModel.Project
	require.NoError(t, db.First(&updated, work.ID).Error)
	require.Equal(t, int64(1), updated.AccessVersion)

	var updatedChore chModel.Chore
	require.NoError(t, db.First(&updatedChore, chore.ID).Error)
	require.Equal(t, int64(1), updatedChore.AccessVersion)

	var tombstones []syncModel.Tombstone
	require.NoError(t, db.Where("user_id = ?", memberID).Find(&tombstones).Error)
	require.NotEmpty(t, tombstones)
	for _, tomb := range tombstones {
		require.Equal(t, int64(1), tomb.AccessVersion, "tombstone %d must carry the access version", tomb.EntityID)
	}

	var event pModel.ProjectAccessEvent
	require.NoError(t, db.First(&event, result.EventID).Error)
	require.Equal(t, pModel.AccessEventStatusPending, event.Status)
	require.Equal(t, int64(1), event.AccessVersion)
	require.Equal(t, ownerID, event.ActorUserID)
	require.Equal(t, []int{chore.ID}, event.Payload.NotificationChoreIDs)
	require.Equal(t, []int{ownerID, memberID}, event.Payload.Recipients, "both sides of the change get a refresh")
	require.False(t, event.Payload.CircleWide)
	require.Equal(t, result.Revocations[chore.ID][memberID], event.Payload.Revocations[chore.ID][memberID])
	require.Equal(t, "on site", event.Payload.ChoreNames[chore.ID])
	require.WithinDuration(t, time.Now(), event.RunAfter, time.Minute)

	// Another device widens again immediately: version 2, re-grant history and
	// drop the user's tombstones.
	result2, err := r.UpdateProjectWithAccess(ctx, &pModel.Project{ID: work.ID, Name: "work"}, membersReqAt(1, memberID), ownerID, sharingCirc)
	require.NoError(t, err)
	require.Equal(t, int64(2), result2.AccessVersion)
	require.NotZero(t, result2.EventID)

	require.NoError(t, db.First(&updated, work.ID).Error)
	require.Equal(t, int64(2), updated.AccessVersion)
	var tombstoneCount int64
	require.NoError(t, db.Model(&syncModel.Tombstone{}).Where("user_id = ?", memberID).Count(&tombstoneCount).Error)
	require.Zero(t, tombstoneCount, "re-grant deletes the revocation tombstones")
	var rehist chModel.ChoreHistory
	require.NoError(t, db.First(&rehist, history.ID).Error)
	require.Equal(t, int64(2), rehist.AccessVersion, "re-sent history carries the new version")
	var events []pModel.ProjectAccessEvent
	require.NoError(t, db.Order("access_version asc").Find(&events).Error)
	require.Len(t, events, 2)
	require.Equal(t, int64(1), events[0].AccessVersion)
	require.Equal(t, int64(2), events[1].AccessVersion)

	// Repeating the same update changes nothing and returns no result.
	again, err := r.UpdateProjectWithAccess(ctx, &pModel.Project{ID: work.ID, Name: "work"}, membersReqAt(2, memberID), ownerID, sharingCirc)
	require.NoError(t, err)
	require.Nil(t, again)
	require.NoError(t, db.First(&updated, work.ID).Error)
	require.Equal(t, int64(2), updated.AccessVersion, "idempotent retry does not allocate a version")
}

// Renames/colors, and audience updates with no affected task (an empty project),
// keep the pre-existing behavior: no access version and no outbox row.
func TestRenameAndEmptyAudienceUpdatesDoNotVersion(t *testing.T) {
	r, db := newTestRepo(t)
	seedCircle(t, db)
	ctx := context.Background()
	work := newSharedProject(t, r, "work", pModel.ProjectVisibilityMembers, memberID)

	// Name-only edit.
	require.NoError(t, r.UpdateProject(ctx, &pModel.Project{ID: work.ID, Name: "renamed"}, nil, ownerID, sharingCirc))
	require.Zero(t, eventCount(t, db))
	require.Zero(t, projectVersion(t, db, work.ID))

	// Audience change on a project without any task: the post-commit broadcast
	// still fires through the legacy inline path, but no version is assigned.
	empty, err := r.UpdateProjectWithAccess(ctx, &pModel.Project{ID: work.ID, Name: "renamed"}, membersReq(), ownerID, sharingCirc)
	require.NoError(t, err)
	require.NotNil(t, empty, "an audience change still reports a plan")
	require.Zero(t, empty.AccessVersion)
	require.Zero(t, empty.EventID)
	require.Zero(t, eventCount(t, db))
	require.Zero(t, projectVersion(t, db, work.ID))

	// Adding a member to a still-empty project behaves the same.
	withMember, err := r.UpdateProjectWithAccess(ctx, &pModel.Project{ID: work.ID, Name: "renamed"}, membersReqAt(0, memberID, outsiderID), ownerID, sharingCirc)
	require.NoError(t, err)
	require.Zero(t, withMember.AccessVersion)
	require.Zero(t, eventCount(t, db))
}

// An update pinned to the preview baseline is rejected once another device
// moved the audience first, so preview and plan always share one baseline.
func TestAudienceUpdateRejectsStaleBaseline(t *testing.T) {
	r, db := newTestRepo(t)
	seedCircle(t, db)
	ctx := context.Background()
	work := newSharedProject(t, r, "work", pModel.ProjectVisibilityMembers, memberID)
	newProjectChore(t, db, work.ID, "c", ownerID, chModel.AssignmentStrategyRandom, intp(memberID), ownerID, memberID)

	_, err := r.UpdateProjectWithAccess(ctx, &pModel.Project{ID: work.ID, Name: "work"}, membersReqAt(0), ownerID, sharingCirc)
	require.NoError(t, err)

	// A second device still holds the baseline from before the first update and
	// tries to widen to a different audience from that stale plan.
	_, err = r.UpdateProjectWithAccess(ctx, &pModel.Project{ID: work.ID, Name: "work"}, membersReqAt(0, outsiderID), ownerID, sharingCirc)
	require.ErrorIs(t, err, ErrAccessChangedSincePreview)

	// Replaying the already-applied change with the old baseline is idempotent,
	// so a lost response can be safely retried.
	retry, err := r.UpdateProjectWithAccess(ctx, &pModel.Project{ID: work.ID, Name: "work"}, membersReqAt(0), ownerID, sharingCirc)
	require.NoError(t, err)
	require.Nil(t, retry)

	// The current baseline works.
	_, err = r.UpdateProjectWithAccess(ctx, &pModel.Project{ID: work.ID, Name: "work"}, membersReqAt(1, memberID), ownerID, sharingCirc)
	require.NoError(t, err)
}

// A change involving a circle-wide audience on either side reaches the whole
// circle; the payload remembers that instead of an explicit recipient list.
func TestCircleAudienceChangeEventReachesCircle(t *testing.T) {
	r, db := newTestRepo(t)
	seedCircle(t, db)
	ctx := context.Background()
	project := newSharedProject(t, r, "was shared", pModel.ProjectVisibilityCircle)
	chore := newProjectChore(t, db, project.ID, "c", ownerID, chModel.AssignmentStrategyRandom, intp(outsiderID), ownerID, outsiderID)
	require.NoError(t, db.Model(&chModel.Chore{}).Where("id = ?", chore.ID).Update("is_private", false).Error)

	vis := pModel.ProjectVisibilityMembers
	req := &pModel.ProjectReq{Name: "was shared", Visibility: &vis, MemberIDs: idsPtr(memberID), BaseAccessVersion: ptr(int64(0))}
	result, err := r.UpdateProjectWithAccess(ctx, &pModel.Project{ID: project.ID, Name: "was shared"}, req, ownerID, sharingCirc)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.AccessVersion)

	var event pModel.ProjectAccessEvent
	require.NoError(t, db.First(&event, result.EventID).Error)
	require.True(t, event.Payload.CircleWide)
	require.Empty(t, event.Payload.Recipients, "nil recipients mean a circle-wide broadcast")
	require.Contains(t, event.Payload.Revocations[chore.ID], outsiderID)
}

func eventCount(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var count int64
	require.NoError(t, db.Model(&pModel.ProjectAccessEvent{}).Count(&count).Error)
	return count
}

func projectVersion(t *testing.T, db *gorm.DB, projectID int) int64 {
	t.Helper()
	var project pModel.Project
	require.NoError(t, db.Select("access_version").First(&project, projectID).Error)
	return project.AccessVersion
}
