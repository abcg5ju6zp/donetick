package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"donetick.com/core/config"
	"donetick.com/core/internal/events"
)

func newTestRepo(t *testing.T) (*Repository, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&Event{}))
	cfg := &config.Config{}
	cfg.Database.Type = "sqlite"
	return NewRepository(db, cfg), db
}

func TestEnqueueIsIdempotentPerGeneration(t *testing.T) {
	r, db := newTestRepo(t)
	historyID := 7
	ev := func(kind Kind, target string) *Event {
		return &Event{
			CircleID: 1, ChoreID: 3, HistoryID: &historyID,
			Kind: kind, EventType: string(events.EventTypeTaskCompleted), Target: target,
		}
	}

	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		return r.Enqueue(tx, ev(KindRealtime, ""), ev(KindWebhook, "https://example.com/hook"))
	}))
	// Simulate a duplicate settlement callback: same dedupe keys are ignored.
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		return r.Enqueue(tx, ev(KindRealtime, ""), ev(KindWebhook, "https://example.com/hook"))
	}))

	var count int64
	require.NoError(t, db.Model(&Event{}).Count(&count).Error)
	require.Equal(t, int64(2), count)

	// A newer generation enqueues its own deliveries independently.
	nextGen := 8
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		return r.Enqueue(tx,
			&Event{CircleID: 1, ChoreID: 3, HistoryID: &nextGen, Kind: KindRealtime, EventType: string(events.EventTypeTaskCompleted)},
			&Event{CircleID: 1, ChoreID: 3, HistoryID: &nextGen, Kind: KindWebhook, EventType: string(events.EventTypeTaskCompleted), Target: "https://example.com/hook"},
		)
	}))
	require.NoError(t, db.Model(&Event{}).Count(&count).Error)
	require.Equal(t, int64(4), count, "different generations never dedupe against each other")

	due, err := r.ListDue(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, due, 4)
}

type fakeSender struct {
	failUntil int
	calls     int
}

func (f *fakeSender) SendWebhookNow(ctx context.Context, target string, event events.Event) error {
	f.calls++
	if f.calls <= f.failUntil {
		return errors.New("webhook endpoint unavailable")
	}
	return nil
}

func TestDispatcherRetriesFailedWebhookAndMarksSent(t *testing.T) {
	r, db := newTestRepo(t)
	sender := &fakeSender{failUntil: 2}
	d := NewDispatcher(r, sender, nil, nil, nil, nil)

	historyID := 11
	require.NoError(t, db.Create(&Event{
		CircleID: 1, ChoreID: 5, HistoryID: &historyID,
		Kind: KindWebhook, EventType: string(events.EventTypeTaskCompleted),
		Target: "https://example.com/hook", Status: StatusPending, MaxAttempts: 5,
		AvailableAt: time.Now().UTC().Add(-time.Minute),
	}).Error)

	// Two failed attempts leave the row pending with backoff and no settlement side effect.
	sweepAndBackdate := func() {
		t.Helper()
		require.NoError(t, d.DispatchDue(context.Background()))
		require.NoError(t, db.Model(&Event{}).Where("id = 1").
			Update("available_at", time.Now().UTC().Add(-time.Minute)).Error)
	}
	sweepAndBackdate()
	sweepAndBackdate()
	var row Event
	require.NoError(t, db.First(&row, 1).Error)
	require.Equal(t, StatusPending, row.Status)
	require.Equal(t, 2, row.Attempts)
	require.True(t, row.AvailableAt.Before(time.Now().UTC()), "backdated")
	require.NotNil(t, row.LastError)
	require.Equal(t, 2, sender.calls)

	// After the endpoint recovers the next sweep sends it.
	require.NoError(t, d.DispatchDue(context.Background()))
	require.NoError(t, db.First(&row, 1).Error)
	require.Equal(t, StatusSent, row.Status)
	require.NotNil(t, row.SentAt)
	require.Equal(t, 3, sender.calls)

	// Nothing left to retry.
	due, err := r.ListDue(context.Background(), 10)
	require.NoError(t, err)
	require.Empty(t, due)
}

func TestDispatcherDeadLettersAfterMaxAttempts(t *testing.T) {
	r, db := newTestRepo(t)
	sender := &fakeSender{failUntil: 999}
	d := NewDispatcher(r, sender, nil, nil, nil, nil)

	require.NoError(t, db.Create(&Event{
		CircleID: 1, ChoreID: 5, Kind: KindWebhook, EventType: "task.completed",
		Target: "https://example.com/hook", Status: StatusPending, Attempts: 2, MaxAttempts: 3,
		AvailableAt: time.Now().UTC().Add(-time.Minute),
	}).Error)
	require.NoError(t, d.DispatchDue(context.Background()))

	var row Event
	require.NoError(t, db.First(&row, 1).Error)
	require.Equal(t, StatusDead, row.Status)
	require.Equal(t, 3, row.Attempts)
}

func TestRealtimeEventWithoutRuntimeMarkedSent(t *testing.T) {
	r, db := newTestRepo(t)
	// No broadcaster wired: the dispatcher must not retry forever.
	d := NewDispatcher(r, &fakeSender{}, nil, nil, nil, nil)

	historyID := 13
	require.NoError(t, db.Create(&Event{
		CircleID: 1, ChoreID: 5, HistoryID: &historyID,
		Kind: KindRealtime, EventType: "chore.completed", Status: StatusPending, MaxAttempts: 5,
		AvailableAt: time.Now().UTC(), Payload: Payload{PerformerID: 1},
	}).Error)
	require.NoError(t, d.DispatchDue(context.Background()))

	var row Event
	require.NoError(t, db.First(&row, 1).Error)
	require.Equal(t, StatusSent, row.Status)
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	require.Equal(t, 30*time.Second, backoff(1))
	require.Equal(t, time.Minute, backoff(2))
	require.Equal(t, time.Hour, backoff(20))
}
