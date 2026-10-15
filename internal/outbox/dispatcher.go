package outbox

import (
	"context"
	"errors"
	"time"

	chModel "donetick.com/core/internal/chore/model"
	"donetick.com/core/internal/events"
	"donetick.com/core/internal/realtime"
	uModel "donetick.com/core/internal/user/model"
	"donetick.com/core/logging"
	"gorm.io/gorm"
)

// WebhookSender delivers one webhook event synchronously and returns an error
// when delivery should be retried. *events.EventsProducer implements it.
type WebhookSender interface {
	SendWebhookNow(ctx context.Context, target string, event events.Event) error
}

// ChoreLoader reloads a chore with the relations the broadcaster needs.
type ChoreLoader interface {
	GetChoreForSystem(ctx context.Context, choreID int) (*chModel.Chore, error)
}

// HistoryLoader reloads a completion generation.
type HistoryLoader interface {
	GetChoreHistoryByID(ctx context.Context, choreID int, historyID int) (*chModel.ChoreHistory, error)
}

// UserLoader reloads the acting user for a broadcast.
type UserLoader interface {
	GetUserByID(ctx context.Context, userID int) (*uModel.User, error)
}

// Dispatcher retries durable external deliveries. It never touches business
// state: settlement happens once, in the originating transaction; this loop
// only attempts (and retries) the side effects afterwards.
type Dispatcher struct {
	repo        *Repository
	sender      WebhookSender
	broadcaster *realtime.EventBroadcaster
	chores      ChoreLoader
	histories   HistoryLoader
	users       UserLoader
	kick        chan struct{}
	interval    time.Duration
	batchSize   int
}

// NewDispatcher wires the retry loop. Any loader may be nil for setups that
// only use one delivery kind.
func NewDispatcher(repo *Repository, sender WebhookSender, rts *realtime.RealTimeService,
	chores ChoreLoader, histories HistoryLoader, users UserLoader) *Dispatcher {
	d := &Dispatcher{
		repo:      repo,
		sender:    sender,
		chores:    chores,
		histories: histories,
		users:     users,
		kick:      make(chan struct{}, 1),
		interval:  30 * time.Second,
		batchSize: 100,
	}
	if rts != nil {
		d.broadcaster = rts.GetEventBroadcaster()
	}
	return d
}

// Kick nudges the loop to dispatch immediately after a new settlement.
func (d *Dispatcher) Kick() {
	if d == nil || d.kick == nil {
		return
	}
	select {
	case d.kick <- struct{}{}:
	default:
	}
}

// Start runs the dispatch loop until ctx is cancelled.
func (d *Dispatcher) Start(ctx context.Context) {
	logger := logging.FromContext(ctx)
	// Sweep once shortly after boot so deliveries persisted before a restart
	// are retried without waiting for the first interval.
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	cleanupInterval := 24 * time.Hour
	lastCleanup := time.Now().UTC()
	runSweep := func() {
		if err := d.DispatchDue(ctx); err != nil {
			logger.Errorw("outbox dispatch sweep failed", "error", err)
		}
		if now := time.Now().UTC(); now.Sub(lastCleanup) >= cleanupInterval {
			if _, err := d.repo.DeleteSent(ctx, now.Add(-30*24*time.Hour)); err != nil {
				logger.Errorw("outbox sent-event cleanup failed", "error", err)
			}
			lastCleanup = now
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			runSweep()
			timer.Reset(d.interval)
		case <-d.kick:
			runSweep()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d.interval)
		}
	}
}

// DispatchDue attempts one batch of due deliveries.
func (d *Dispatcher) DispatchDue(ctx context.Context) error {
	pending, err := d.repo.ListDue(ctx, d.batchSize)
	if err != nil {
		return err
	}
	for _, event := range pending {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := d.deliver(ctx, event)
		if err == nil {
			if markErr := d.repo.MarkSent(ctx, event.ID); markErr != nil {
				logging.FromContext(ctx).Errorw("failed to mark outbox event sent", "error", markErr, "id", event.ID)
			}
			continue
		}
		attempts := event.Attempts + 1
		nextAt := time.Now().UTC().Add(backoff(attempts))
		if recErr := d.repo.RecordFailure(ctx, event.ID, attempts, event.MaxAttempts, err, nextAt); recErr != nil {
			logging.FromContext(ctx).Errorw("failed to record outbox failure", "error", recErr, "id", event.ID)
		}
	}
	return nil
}

// deliver performs one attempt. A missing chore (deleted after settlement) is
// treated as success: there is nothing left to announce and retrying cannot help.
func (d *Dispatcher) deliver(ctx context.Context, event *Event) error {
	switch event.Kind {
	case KindWebhook:
		if d.sender == nil || event.Target == "" {
			return nil
		}
		out := events.Event{
			Type:      events.EventType(event.EventType),
			Timestamp: event.CreatedAt.UTC(),
			Data: events.ChoreData{
				Chore:       event.Payload.Chore,
				Username:    event.Payload.Username,
				DisplayName: event.Payload.DisplayName,
				Note:        stringValue(event.Payload.Note),
			},
		}
		return d.sender.SendWebhookNow(ctx, event.Target, out)
	case KindRealtime:
		if d.broadcaster == nil || d.chores == nil || d.users == nil || event.HistoryID == nil {
			return nil
		}
		chore, err := d.chores.GetChoreForSystem(ctx, event.ChoreID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		history, err := d.histories.GetChoreHistoryByID(ctx, event.ChoreID, *event.HistoryID)
		if err != nil {
			return err
		}
		user, err := d.users.GetUserByID(ctx, event.Payload.PerformerID)
		if err != nil {
			return err
		}
		d.broadcaster.BroadcastChoreCompleted(chore, user, history, event.Payload.Note)
		return nil
	default:
		// Unknown kinds can never succeed; RecordFailure will dead-letter them.
		return errors.New("unknown outbox event kind: " + string(event.Kind))
	}
}

// backoff is exponential, starting at 30s and capped at 1h.
func backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := time.Duration(30) * time.Second
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= time.Hour {
			return time.Hour
		}
	}
	return d
}

func stringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
