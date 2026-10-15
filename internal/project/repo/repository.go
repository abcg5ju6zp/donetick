package repo

import (
	"context"
	"errors"
	"sort"
	"time"

	config "donetick.com/core/config"
	chModel "donetick.com/core/internal/chore/model"
	chRepo "donetick.com/core/internal/chore/repo"
	"donetick.com/core/internal/project/access"
	pModel "donetick.com/core/internal/project/model"
	syncModel "donetick.com/core/internal/sync/model"
	"donetick.com/core/logging"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	// ErrProjectNotFound hides whether a project exists from users who can't see it.
	ErrProjectNotFound = errors.New("project not found")
	// ErrInvalidVisibility rejects an unknown visibility value.
	ErrInvalidVisibility = errors.New("invalid project visibility")
	// ErrMemberNotInCircle rejects sharing with somebody outside the project's circle.
	ErrMemberNotInCircle = errors.New("project members must be active members of the circle")
	// ErrAccessChangedSincePreview rejects an update pinned to a preview baseline
	// after another device changed the audience first, so the preview and the
	// applied plan always share one baseline.
	ErrAccessChangedSincePreview = errors.New("project audience changed since the access preview")
)

type ProjectRepository struct {
	db        *gorm.DB
	choreRepo *chRepo.ChoreRepository
}

func NewProjectRepository(db *gorm.DB, cfg *config.Config, choreRepo *chRepo.ChoreRepository) *ProjectRepository {
	return &ProjectRepository{db: db, choreRepo: choreRepo}
}

// bumpChoreSyncVersions assigns a fresh sync version to every chore in choreIDs so
// clients syncing on sync_version pick up the project change. Versions are reserved as a
// contiguous range and applied one per chore, so no two chores share a version (sharing
// one would let sync pagination skip records). Must run inside tx.
func (r *ProjectRepository) bumpChoreSyncVersions(ctx context.Context, tx *gorm.DB, circleID int, choreIDs []int) error {
	if len(choreIDs) < 1 {
		return nil
	}

	var endVersion int64
	if err := tx.WithContext(ctx).Raw(`
		INSERT INTO sync_cursors (circle_id, entity_type, max_version)
		VALUES (?, ?, ?)
		ON CONFLICT (circle_id, entity_type) DO UPDATE SET max_version = sync_cursors.max_version + ?
		RETURNING max_version`,
		circleID, syncModel.EntityTypeChore, len(choreIDs), len(choreIDs),
	).Scan(&endVersion).Error; err != nil {
		return err
	}

	startVersion := endVersion - int64(len(choreIDs)) + 1
	for i, choreID := range choreIDs {
		if err := tx.WithContext(ctx).Model(&chModel.Chore{}).Where("id = ?", choreID).
			Update("sync_version", startVersion+int64(i)).Error; err != nil {
			return err
		}
	}
	return nil
}

// choreIDsInProject returns the chores currently assigned to projectID within circleID.
func (r *ProjectRepository) choreIDsInProject(ctx context.Context, tx *gorm.DB, projectID int, circleID int) ([]int, error) {
	var choreIDs []int
	if err := tx.WithContext(ctx).Model(&chModel.Chore{}).
		Where("project_id = ? AND circle_id = ?", projectID, circleID).
		Pluck("id", &choreIDs).Error; err != nil {
		return nil, err
	}
	return choreIDs, nil
}

// GetCircleProjects returns the projects of a circle the user is allowed to see:
// every circle-wide one, the private ones they created, and the ones shared with
// them as a member. Member lists are loaded.
func (r *ProjectRepository) GetCircleProjects(ctx context.Context, circleID int, userID int) ([]*pModel.Project, error) {
	var projects []*pModel.Project
	if err := r.db.WithContext(ctx).
		Where(`circle_id = ? AND (is_private = ? OR created_by = ? OR (shared_with_members = ? AND EXISTS (
			SELECT 1 FROM project_members pm WHERE pm.project_id = projects.id AND pm.user_id = ?)))`,
			circleID, false, userID, true, userID).
		Order("name ASC").Find(&projects).Error; err != nil {
		return nil, err
	}
	if err := access.LoadMembers(ctx, r.db, projects...); err != nil {
		return nil, err
	}
	return projects, nil
}

// GetProjectByID loads a project of the circle with its member list. It does not
// check visibility; callers that act for a user must call CanView.
func (r *ProjectRepository) GetProjectByID(ctx context.Context, projectID int, circleID int) (*pModel.Project, error) {
	var project pModel.Project
	if err := r.db.WithContext(ctx).Where("id = ? AND circle_id = ?", projectID, circleID).First(&project).Error; err != nil {
		return nil, err
	}
	if err := access.LoadMembers(ctx, r.db, &project); err != nil {
		return nil, err
	}
	return &project, nil
}

// CreateProject stores a project. Members are only stored for visibility
// "members"; they must be active members of the project's circle.
func (r *ProjectRepository) CreateProject(ctx context.Context, project *pModel.Project) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		members := project.MemberIDs
		if project.Visibility() != pModel.ProjectVisibilityMembers {
			members = nil
		}
		members, err := r.validateMembers(ctx, tx, project.CircleID, project.CreatedBy, members)
		if err != nil {
			return err
		}
		if err := tx.WithContext(ctx).Create(project).Error; err != nil {
			return err
		}
		if err := r.replaceMembers(ctx, tx, project, members, project.CreatedBy); err != nil {
			return err
		}
		project.SetMembers(members)
		return nil
	})
}

// UpdateProject updates a project owned by the user. isPrivate is optional: when
// nil the current visibility is kept. Kept for callers that only know the boolean
// flag; see UpdateProjectWithAccess.
func (r *ProjectRepository) UpdateProject(ctx context.Context, project *pModel.Project, isPrivate *bool, userID int, circleID int) error {
	_, err := r.UpdateProjectWithAccess(ctx, project, &pModel.ProjectReq{IsPrivate: isPrivate}, userID, circleID)
	return err
}

// UpdateProjectWithAccess updates name, description, color and icon of a project
// owned by the user and, when the request asks for it, its audience. An audience
// change is applied to every chore of the project in the same transaction (see
// ChoreRepository.ApplyProjectAccessChange). The returned result is nil when the
// audience did not change.
//
// An audience update that affects at least one task is assigned the project's
// next monotonically increasing access version inside this transaction. The
// task changes, tombstones, re-sent history and a post-processing row
// (project_access_events) all carry that version; until the transaction commits,
// the project keeps its old audience and no revocation is sent.
func (r *ProjectRepository) UpdateProjectWithAccess(ctx context.Context, project *pModel.Project, req *pModel.ProjectReq, userID int, circleID int) (*chRepo.ProjectAccessResult, error) {
	log := logging.FromContext(ctx)
	var result *chRepo.ProjectAccessResult

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		existing, err := r.lockOwnedProject(ctx, tx, project.ID, userID, circleID, "user does not have permission to update this project")
		if err != nil {
			return err
		}

		before := chRepo.AccessOf(existing)
		target, members, err := r.resolveTarget(ctx, tx, existing, req)
		if err != nil {
			return err
		}
		after := chRepo.ProjectAccess{Visibility: target, MemberIDs: members}

		accessChanged := !sameAccess(before, after)
		// The preview baseline only guards audience updates; a name/color edit
		// pinned to an old preview is harmless and keeps working.
		if accessChanged && req != nil && req.BaseAccessVersion != nil && *req.BaseAccessVersion != existing.AccessVersion {
			return ErrAccessChangedSincePreview
		}

		updates := map[string]interface{}{
			"name":                project.Name,
			"description":         project.Description,
			"color":               project.Color,
			"icon":                project.Icon,
			"is_private":          target != pModel.ProjectVisibilityCircle,
			"shared_with_members": target == pModel.ProjectVisibilityMembers,
		}
		if err := tx.WithContext(ctx).Model(&pModel.Project{}).Where("id = ? AND circle_id = ?", project.ID, circleID).Updates(updates).Error; err != nil {
			log.Error("Error updating project", "error", err)
			return err
		}
		if err := r.replaceMembers(ctx, tx, existing, members, userID); err != nil {
			log.Error("Error updating project members", "error", err)
			return err
		}

		if accessChanged {
			// Fix the plan for the old and the new audience under the project
			// lock, so it matches what the owner previewed on this baseline.
			plan, err := r.choreRepo.PlanProjectAccessChange(ctx, tx, circleID, project.ID, existing.CreatedBy, before, after)
			if err != nil {
				log.Error("Error planning project access change", "error", err, "projectID", project.ID)
				return err
			}
			plan.AccessVersion = existing.AccessVersion

			// Only an update that changes a task's assignment or audience takes
			// a new access version and a post-processing row; audience updates
			// that affect no task (e.g. an empty project) keep existing behavior.
			var accessVersion int64
			if len(plan.ChangedChores()) > 0 {
				accessVersion, err = r.bumpProjectAccessVersion(ctx, tx, project.ID)
				if err != nil {
					log.Error("Error bumping project access version", "error", err, "projectID", project.ID)
					return err
				}
			}

			result, err = r.choreRepo.ApplyPlannedProjectAccessChange(ctx, tx, circleID, existing.CreatedBy, plan, accessVersion)
			if err != nil {
				log.Error("Error propagating project access to chores", "error", err, "projectID", project.ID)
				return err
			}
			if accessVersion > 0 {
				eventID, err := r.insertAccessEvent(ctx, tx, circleID, project.ID, userID, accessVersion, result)
				if err != nil {
					log.Error("Error persisting project access post-processing", "error", err, "projectID", project.ID)
					return err
				}
				result.EventID = eventID
			}
			return nil
		}

		// Clients render the project (name/color/icon) alongside its chores, so the
		// chores have to be re-synced even though the chore rows are untouched.
		choreIDs, err := r.choreIDsInProject(ctx, tx, project.ID, circleID)
		if err != nil {
			log.Error("Error getting chores for project", "error", err)
			return err
		}
		return r.bumpChoreSyncVersions(ctx, tx, circleID, choreIDs)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// bumpProjectAccessVersion reserves the next monotonically increasing audience
// version for a project. Must run inside the audience-update transaction while
// the project row is locked.
func (r *ProjectRepository) bumpProjectAccessVersion(ctx context.Context, tx *gorm.DB, projectID int) (int64, error) {
	if err := tx.WithContext(ctx).Model(&pModel.Project{}).Where("id = ?", projectID).
		UpdateColumn("access_version", gorm.Expr("access_version + 1")).Error; err != nil {
		return 0, err
	}
	var version int64
	if err := tx.WithContext(ctx).Model(&pModel.Project{}).Where("id = ?", projectID).
		Select("access_version").Scan(&version).Error; err != nil {
		return 0, err
	}
	return version, nil
}

// insertAccessEvent freezes the post-commit work of one audience update — who
// lost which task (with each tombstone's sync version), which reminders must be
// rebuilt and who must refresh — into project_access_events inside the same
// transaction. Must run inside tx.
func (r *ProjectRepository) insertAccessEvent(ctx context.Context, tx *gorm.DB, circleID, projectID, actorID int, accessVersion int64, result *chRepo.ProjectAccessResult) (int64, error) {
	plan := result.Plan
	payload := pModel.AccessEventPayload{
		Before:               pModel.AccessSnapshot{Visibility: plan.Before.Visibility, MemberIDs: append([]int(nil), plan.Before.MemberIDs...)},
		After:                pModel.AccessSnapshot{Visibility: plan.After.Visibility, MemberIDs: append([]int(nil), plan.After.MemberIDs...)},
		NotificationChoreIDs: append([]int(nil), result.NotificationChoreIDs...),
		Revocations:          result.Revocations,
	}
	payload.CircleWide = plan.Before.Visibility == pModel.ProjectVisibilityCircle || plan.After.Visibility == pModel.ProjectVisibilityCircle
	if !payload.CircleWide {
		viewers := map[int]struct{}{plan.OwnerID: {}}
		for _, id := range plan.Before.MemberIDs {
			viewers[id] = struct{}{}
		}
		for _, id := range plan.After.MemberIDs {
			viewers[id] = struct{}{}
		}
		payload.Recipients = sortedViewerIDs(viewers)
	}
	if len(result.Revocations) > 0 {
		payload.ChoreNames = make(map[int]string, len(result.Revocations))
		for _, change := range plan.Chores {
			if _, ok := result.Revocations[change.ChoreID]; ok {
				payload.ChoreNames[change.ChoreID] = change.Name
			}
		}
	}

	event := &pModel.ProjectAccessEvent{
		CircleID:      circleID,
		ProjectID:     projectID,
		AccessVersion: accessVersion,
		ActorUserID:   actorID,
		Status:        pModel.AccessEventStatusPending,
		RunAfter:      time.Now().UTC(),
		Payload:       payload,
	}
	if err := tx.WithContext(ctx).Create(event).Error; err != nil {
		return 0, err
	}
	return event.ID, nil
}

func sortedViewerIDs(set map[int]struct{}) []int {
	ids := make([]int, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

// PreviewProjectAccess reports, without changing anything, what an update request
// would do to the project's chores, assignments and audience.
func (r *ProjectRepository) PreviewProjectAccess(ctx context.Context, projectID int, req *pModel.ProjectReq, userID int, circleID int) (*chRepo.ProjectAccessPlan, error) {
	var plan *chRepo.ProjectAccessPlan
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		existing, err := r.lockOwnedProject(ctx, tx, projectID, userID, circleID, "user does not have permission to update this project")
		if err != nil {
			return err
		}
		target, members, err := r.resolveTarget(ctx, tx, existing, req)
		if err != nil {
			return err
		}
		plan, err = r.choreRepo.PlanProjectAccessChange(ctx, tx, circleID, projectID, existing.CreatedBy,
			chRepo.AccessOf(existing), chRepo.ProjectAccess{Visibility: target, MemberIDs: members})
		if err != nil {
			return err
		}
		plan.AccessVersion = existing.AccessVersion
		return nil
	})
	return plan, err
}

func (r *ProjectRepository) DeleteProject(ctx context.Context, projectID int, userID int, circleID int) error {
	_, err := r.DeleteProjectWithResult(ctx, projectID, userID, circleID)
	return err
}

// DeleteProjectWithResult deletes a project owned by the user and returns the
// chores whose pending reminders were removed because somebody lost sight of
// them; the caller regenerates those after commit.
func (r *ProjectRepository) DeleteProjectWithResult(ctx context.Context, projectID int, userID int, circleID int) ([]int, error) {
	log := logging.FromContext(ctx)
	var notificationChores []int

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Serialize deletion with project privacy changes and chore creation/moves.
		project, err := r.lockOwnedProject(ctx, tx, projectID, userID, circleID, "user does not have permission to delete this project")
		if err != nil {
			return err
		}
		if project.IsDefault {
			return errors.New("cannot delete default project")
		}

		detached, err := r.choreRepo.DetachProjectChores(ctx, tx, circleID, project)
		if err != nil {
			log.Error("Error detaching chores when deleting project", "error", err)
			return err
		}
		notificationChores = detached
		if err := tx.Where("project_id = ?", projectID).Delete(&pModel.ProjectMember{}).Error; err != nil {
			log.Error("Error deleting project members", "error", err)
			return err
		}
		if err := tx.Where("id = ? AND circle_id = ?", projectID, circleID).Delete(&pModel.Project{}).Error; err != nil {
			log.Error("Error deleting project", "error", err)
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return notificationChores, nil
}

// GetRestrictedProjects returns the projects of a circle that are not visible to
// the whole circle (private or shared with selected members), members loaded,
// keyed by ID.
func (r *ProjectRepository) GetRestrictedProjects(ctx context.Context, circleID int) (map[int]*pModel.Project, error) {
	var projects []*pModel.Project
	if err := r.db.WithContext(ctx).Where("circle_id = ? AND is_private = ?", circleID, true).Find(&projects).Error; err != nil {
		return nil, err
	}
	if err := access.LoadMembers(ctx, r.db, projects...); err != nil {
		return nil, err
	}
	byID := make(map[int]*pModel.Project, len(projects))
	for _, p := range projects {
		byID[p.ID] = p
	}
	return byID, nil
}

// RemoveProjectsOwnedBy deletes the restricted (private or shared with selected
// members) projects a user owns in a circle they are leaving, so no such project
// is left behind that nobody can change or re-scope: remaining chores are detached
// and follow their own privacy from then on (members get tombstones for the ones
// they only saw through the project). Circle-wide and default projects stay, as
// upstream keeps them.
func (r *ProjectRepository) RemoveProjectsOwnedBy(ctx context.Context, circleID int, ownerID int) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var projects []*pModel.Project
		if err := tx.WithContext(ctx).Where("circle_id = ? AND created_by = ? AND is_default = ? AND is_private = ?", circleID, ownerID, false, true).Find(&projects).Error; err != nil {
			return err
		}
		if err := access.LoadMembers(ctx, tx, projects...); err != nil {
			return err
		}
		for _, project := range projects {
			if _, err := r.choreRepo.DetachProjectChores(ctx, tx, circleID, project); err != nil {
				return err
			}
			if err := tx.Where("project_id = ?", project.ID).Delete(&pModel.ProjectMember{}).Error; err != nil {
				return err
			}
			if err := tx.Where("id = ?", project.ID).Delete(&pModel.Project{}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// lockOwnedProject loads a project with its members under a row lock (Postgres)
// and checks that userID owns it. Users who can't see the project get
// ErrProjectNotFound, so its existence does not leak; users who can see it but
// don't own it get notOwnerMsg.
func (r *ProjectRepository) lockOwnedProject(ctx context.Context, tx *gorm.DB, projectID, userID, circleID int, notOwnerMsg string) (*pModel.Project, error) {
	var project pModel.Project
	query := tx.WithContext(ctx)
	if tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := query.Where("id = ? AND circle_id = ?", projectID, circleID).First(&project).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrProjectNotFound
		}
		return nil, err
	}
	if err := access.LoadMembers(ctx, tx, &project); err != nil {
		return nil, err
	}
	if project.CreatedBy != userID {
		if !project.CanView(userID) {
			return nil, ErrProjectNotFound
		}
		return nil, errors.New(notOwnerMsg)
	}
	return &project, nil
}

// resolveTarget turns an update request into the validated target audience.
func (r *ProjectRepository) resolveTarget(ctx context.Context, tx *gorm.DB, existing *pModel.Project, req *pModel.ProjectReq) (pModel.ProjectVisibility, []int, error) {
	if req == nil {
		req = &pModel.ProjectReq{}
	}
	if req.Visibility != nil && !req.Visibility.Valid() {
		return "", nil, ErrInvalidVisibility
	}
	target, members := req.ResolveAccess(existing.Visibility(), existing.MemberIDs)
	members, err := r.validateMembers(ctx, tx, existing.CircleID, existing.CreatedBy, members)
	if err != nil {
		return "", nil, err
	}
	return target, members, nil
}

// validateMembers dedupes the member list, drops the owner and checks that every
// remaining user is an active member of the circle.
func (r *ProjectRepository) validateMembers(ctx context.Context, tx *gorm.DB, circleID, ownerID int, memberIDs []int) ([]int, error) {
	seen := map[int]struct{}{}
	members := make([]int, 0, len(memberIDs))
	for _, id := range memberIDs {
		if id == ownerID {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		members = append(members, id)
	}
	sort.Ints(members)
	if len(members) == 0 {
		return members, nil
	}
	var count int64
	if err := tx.WithContext(ctx).Table("user_circles").
		Where("circle_id = ? AND is_active = ? AND user_id IN ?", circleID, true, members).
		Distinct("user_id").Count(&count).Error; err != nil {
		return nil, err
	}
	if int(count) != len(members) {
		return nil, ErrMemberNotInCircle
	}
	return members, nil
}

// replaceMembers makes the project_members rows of project equal to members.
func (r *ProjectRepository) replaceMembers(ctx context.Context, tx *gorm.DB, project *pModel.Project, members []int, actingUserID int) error {
	keep := make(map[int]struct{}, len(members))
	for _, id := range members {
		keep[id] = struct{}{}
	}
	var existing []pModel.ProjectMember
	if err := tx.WithContext(ctx).Where("project_id = ?", project.ID).Find(&existing).Error; err != nil {
		return err
	}
	have := make(map[int]struct{}, len(existing))
	remove := make([]int, 0)
	for _, row := range existing {
		have[row.UserID] = struct{}{}
		if _, ok := keep[row.UserID]; !ok {
			remove = append(remove, row.UserID)
		}
	}
	if len(remove) > 0 {
		if err := tx.WithContext(ctx).Where("project_id = ? AND user_id IN ?", project.ID, remove).
			Delete(&pModel.ProjectMember{}).Error; err != nil {
			return err
		}
	}
	for _, id := range members {
		if _, ok := have[id]; ok {
			continue
		}
		if err := tx.WithContext(ctx).Create(&pModel.ProjectMember{
			ProjectID: project.ID,
			UserID:    id,
			CircleID:  project.CircleID,
			AddedBy:   actingUserID,
		}).Error; err != nil {
			return err
		}
	}
	return nil
}

func sameAccess(a, b chRepo.ProjectAccess) bool {
	if a.Visibility != b.Visibility {
		return false
	}
	if a.Visibility != pModel.ProjectVisibilityMembers {
		return true
	}
	x := append([]int(nil), a.MemberIDs...)
	y := append([]int(nil), b.MemberIDs...)
	sort.Ints(x)
	sort.Ints(y)
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// GetAccessEvent loads one audience-update post-processing row.
func (r *ProjectRepository) GetAccessEvent(ctx context.Context, id int64) (*pModel.ProjectAccessEvent, error) {
	var event pModel.ProjectAccessEvent
	if err := r.db.WithContext(ctx).First(&event, id).Error; err != nil {
		return nil, err
	}
	return &event, nil
}

// ListDueAccessEvents returns post-processing rows that are due, oldest first:
// pending rows whose backoff has elapsed, and processing rows whose lease was
// never released (the process died while handling them), both picked up again
// after a restart.
func (r *ProjectRepository) ListDueAccessEvents(ctx context.Context, now time.Time, staleBefore time.Time, limit int) ([]*pModel.ProjectAccessEvent, error) {
	var events []*pModel.ProjectAccessEvent
	err := r.db.WithContext(ctx).
		Where("(status = ? AND run_after <= ?) OR (status = ? AND run_after < ?)",
			pModel.AccessEventStatusPending, now, pModel.AccessEventStatusProcessing, staleBefore).
		Order("id asc").Limit(limit).Find(&events).Error
	if err != nil {
		return nil, err
	}
	return events, nil
}

// ClaimAccessEvent atomically takes a row for processing: a due pending row, or
// a processing row whose lease expired, flips to processing with a fresh lease
// and increments attempts. It reports false when another worker claimed it.
func (r *ProjectRepository) ClaimAccessEvent(ctx context.Context, id int64, now time.Time, lease time.Duration) (bool, error) {
	res := r.db.WithContext(ctx).Model(&pModel.ProjectAccessEvent{}).
		Where("id = ? AND ((status = ? AND run_after <= ?) OR (status = ? AND run_after < ?))",
			id, pModel.AccessEventStatusPending, now, pModel.AccessEventStatusProcessing, now.Add(-lease)).
		Updates(map[string]interface{}{
			"status":     pModel.AccessEventStatusProcessing,
			"attempts":   gorm.Expr("attempts + 1"),
			"run_after":  now.Add(lease),
			"last_error": "",
		})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected == 1, nil
}

// CompleteAccessEvent marks post-processing of an audience update as done.
func (r *ProjectRepository) CompleteAccessEvent(ctx context.Context, id int64, completedAt time.Time) error {
	return r.db.WithContext(ctx).Model(&pModel.ProjectAccessEvent{}).Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":       pModel.AccessEventStatusCompleted,
			"completed_at": completedAt,
			"last_error":   "",
		}).Error
}

// FailAccessEvent hands a row back to the pending queue with an exponential
// backoff so persistent post-processing failures do not spin, while staying
// durable across restarts.
func (r *ProjectRepository) FailAccessEvent(ctx context.Context, id int64, runAfter time.Time, lastError string) error {
	return r.db.WithContext(ctx).Model(&pModel.ProjectAccessEvent{}).Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":     pModel.AccessEventStatusPending,
			"run_after":  runAfter,
			"last_error": lastError,
		}).Error
}

// CurrentProjectAccessVersion returns the project's current audience version and
// whether the project still exists. Post-processing uses it to detect that a
// newer audience update has superseded its snapshot.
func (r *ProjectRepository) CurrentProjectAccessVersion(ctx context.Context, projectID int) (int64, bool, error) {
	var project pModel.Project
	err := r.db.WithContext(ctx).Select("id", "access_version").First(&project, projectID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return project.AccessVersion, true, nil
}

// AccessTombstoneAlive reports whether the exact user-scoped chore tombstone
// written for an audience update still exists. Re-granting access deletes it, so
// a stale post-processing retry does not re-send a revocation for access that a
// newer version restored.
func (r *ProjectRepository) AccessTombstoneAlive(ctx context.Context, circleID, choreID, userID int, syncVersion int64) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&syncModel.Tombstone{}).
		Where("circle_id = ? AND entity_type = ? AND entity_id = ? AND user_id = ? AND sync_version = ?",
			circleID, syncModel.EntityTypeChore, choreID, userID, syncVersion).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
