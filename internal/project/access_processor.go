package project

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	chModel "donetick.com/core/internal/chore/model"
	chRepo "donetick.com/core/internal/chore/repo"
	nps "donetick.com/core/internal/notifier/service"
	pModel "donetick.com/core/internal/project/model"
	pRepo "donetick.com/core/internal/project/repo"
	"donetick.com/core/internal/realtime"
	uModel "donetick.com/core/internal/user/model"
	"donetick.com/core/logging"
)

// accessNotificationPlanner is the slice of the notification planner the
// post-processing needs; *nps.NotificationPlanner satisfies it.
type accessNotificationPlanner interface {
	GenerateNotificationsWithAccessVersion(ctx context.Context, chore *chModel.Chore, accessVersion int64) bool
}

// accessEventBroadcaster is the slice of the realtime broadcaster the
// post-processing needs; *realtime.EventBroadcaster satisfies it.
type accessEventBroadcaster interface {
	BroadcastChoreAccessRevoked(circleID, choreID int, choreName string, user *uModel.User, accessVersion int64, versions map[int]int64)
	BroadcastProjectUpdated(circleID, projectID int, accessVersion int64, accessChanged bool, user *uModel.User, recipients []int)
}

const (
	accessProcessInterval = 15 * time.Second
	accessProcessLease    = 2 * time.Minute
	accessProcessBatch    = 20
)

// AccessProcessor performs the post-commit work of audience updates — reminder
// rebuilds and realtime broadcasts — from the durable project_access_events
// outbox. The HTTP handler dispatches a row immediately after commit; failures
// stay pending with an exponential backoff, rows unfinished by a crashed
// process lose their lease, and the periodic sweep replays both after a restart.
//
// Versioning keeps replays honest: a reminder rebuild is skipped once a newer
// audience version superseded it (the newer update rebuilds), and a revocation
// is replayed only while its exact tombstone still exists, so an old event can
// never re-revoke access that a later version restored.
type AccessProcessor struct {
	pRepo       *pRepo.ProjectRepository
	choreRepo   *chRepo.ChoreRepository
	planner     accessNotificationPlanner
	broadcaster accessEventBroadcaster

	interval time.Duration
	lease    time.Duration
	batch    int

	stop chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

// NewAccessProcessor wires the processor. Either optional dependency may be nil
// (e.g. in tests), in which case that part of the post-processing is skipped but
// the outbox row is still completed.
func NewAccessProcessor(pRepo *pRepo.ProjectRepository, choreRepo *chRepo.ChoreRepository, planner *nps.NotificationPlanner, rts *realtime.RealTimeService) *AccessProcessor {
	p := &AccessProcessor{
		pRepo:     pRepo,
		choreRepo: choreRepo,
		interval:  accessProcessInterval,
		lease:     accessProcessLease,
		batch:     accessProcessBatch,
		stop:      make(chan struct{}),
	}
	// Guard against typed-nil interfaces: a nil *NotificationPlanner stored in
	// the interface field would otherwise read as non-nil.
	if planner != nil {
		p.planner = planner
	}
	if rts != nil {
		p.broadcaster = rts.GetEventBroadcaster()
	}
	return p
}

// Dispatch performs the post-commit work of one audience update right away. Any
// failure is persisted on the row (status back to pending with a backoff) and
// handled again by the background sweep, so the caller never blocks the HTTP
// response on reminders or broadcasts.
func (p *AccessProcessor) Dispatch(ctx context.Context, eventID int64) {
	if err := p.run(ctx, eventID); err != nil {
		logging.FromContext(ctx).Error("project access post-processing deferred",
			"error", err, "eventID", eventID)
	}
}

// Start runs the periodic sweep. It first processes everything a previous
// process left behind, so a restart between commit and dispatch loses no work.
func (p *AccessProcessor) Start(ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := p.ProcessPending(ctx); err != nil {
			logging.FromContext(ctx).Error("project access post-processing startup sweep failed", "error", err)
		}
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := p.ProcessPending(ctx); err != nil {
					logging.FromContext(ctx).Error("project access post-processing sweep failed", "error", err)
				}
			}
		}
	}()
}

// Stop ends the background sweep.
func (p *AccessProcessor) Stop() {
	p.once.Do(func() { close(p.stop) })
	p.wg.Wait()
}

// ProcessPending claims and handles every due outbox row.
func (p *AccessProcessor) ProcessPending(ctx context.Context) error {
	now := time.Now().UTC()
	events, err := p.pRepo.ListDueAccessEvents(ctx, now, now.Add(-p.lease), p.batch)
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := p.run(ctx, event.ID); err != nil {
			logging.FromContext(ctx).Error("project access post-processing failed",
				"error", err, "eventID", event.ID, "projectID", event.ProjectID, "accessVersion", event.AccessVersion)
		}
	}
	return nil
}

// run claims one row, performs its frozen work, and either completes it or hands
// it back to the pending queue with a backoff. A row another worker already
// claimed is left alone.
func (p *AccessProcessor) run(ctx context.Context, eventID int64) error {
	now := time.Now().UTC()
	claimed, err := p.pRepo.ClaimAccessEvent(ctx, eventID, now, p.lease)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}
	event, err := p.pRepo.GetAccessEvent(ctx, eventID)
	if err != nil {
		return err
	}
	if event.Status != pModel.AccessEventStatusProcessing {
		return nil
	}

	if err := p.processEvent(ctx, event); err != nil {
		_ = p.pRepo.FailAccessEvent(ctx, eventID, now.Add(p.backoff(event.Attempts)), err.Error())
		return err
	}
	return p.pRepo.CompleteAccessEvent(ctx, eventID, time.Now().UTC())
}

// processEvent performs the frozen post-commit work of one audience update.
func (p *AccessProcessor) processEvent(ctx context.Context, event *pModel.ProjectAccessEvent) error {
	payload := event.Payload
	actor := &uModel.User{ID: event.ActorUserID}

	currentVersion, projectExists, err := p.pRepo.CurrentProjectAccessVersion(ctx, event.ProjectID)
	if err != nil {
		return err
	}

	// Reminders: only rebuild when this row still describes the project's
	// current audience. The pending reminders were dropped in the updating
	// transaction; if a newer audience version committed since, that newer
	// update owns the rebuild from the current state — replaying this one would
	// only race it. Rebuilding always reads the CURRENT chore and audience, so
	// it never recreates a reminder for somebody who cannot see the task.
	if projectExists && currentVersion == event.AccessVersion && p.planner != nil && len(payload.NotificationChoreIDs) > 0 {
		for _, choreID := range payload.NotificationChoreIDs {
			chore, err := p.choreRepo.GetChoreForSystem(ctx, choreID)
			if err != nil {
				return err
			}
			if !p.planner.GenerateNotificationsWithAccessVersion(ctx, chore, event.AccessVersion) {
				return fmt.Errorf("reminder rebuild failed for chore %d", choreID)
			}
		}
	}

	if p.broadcaster != nil {
		// Revocations: a tombstone deleted by a later re-grant means the user
		// sees the chore again; skip it so the stale event cannot re-revoke.
		for _, choreID := range sortedIntSet(payload.Revocations) {
			versions := make(map[int]int64)
			for userID, syncVersion := range payload.Revocations[choreID] {
				alive, err := p.pRepo.AccessTombstoneAlive(ctx, event.CircleID, choreID, userID, syncVersion)
				if err != nil {
					return err
				}
				if alive {
					versions[userID] = syncVersion
				}
			}
			if len(versions) > 0 {
				p.broadcaster.BroadcastChoreAccessRevoked(event.CircleID, choreID, payload.ChoreNames[choreID], actor, event.AccessVersion, versions)
			}
		}

		// The refresh hint is idempotent and carries no project details, so it is
		// safe to (re)send even after a newer update: recipients refetch and only
		// learn what they may see. A circle-wide audience on either side reaches
		// the whole circle; otherwise the union of both audiences.
		var recipients []int
		if !payload.CircleWide {
			recipients = payload.Recipients
		}
		p.broadcaster.BroadcastProjectUpdated(event.CircleID, event.ProjectID, event.AccessVersion, true, actor, recipients)
	}
	return nil
}

// backoff is the delay before a failed row is retried: 15s, 30s, 60s, ... up to
// an hour.
func (p *AccessProcessor) backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := 15 * time.Second
	for i := 1; i < attempts; i++ {
		d *= 2
	}
	if max := time.Hour; d > max {
		d = max
	}
	return d
}

func sortedIntSet[V any](m map[int]V) []int {
	ids := make([]int, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}
