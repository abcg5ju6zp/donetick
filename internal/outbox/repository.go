package outbox

import (
	"context"
	"time"

	"donetick.com/core/config"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Repository persists outbox events. Enqueue accepts the caller's transaction
// so the delivery rows commit atomically with the business state change.
type Repository struct {
	db     *gorm.DB
	dbType string
}

func NewRepository(db *gorm.DB, cfg *config.Config) *Repository {
	return &Repository{db: db, dbType: cfg.Database.Type}
}

// Enqueue stores events inside tx. Duplicate rows for the same
// generation/kind/event/target are ignored, so a duplicate settlement can
// never create a duplicate delivery.
func (r *Repository) Enqueue(tx *gorm.DB, events ...*Event) error {
	if len(events) == 0 {
		return nil
	}
	now := time.Now().UTC()
	for _, event := range events {
		if event.Status == "" {
			event.Status = StatusPending
		}
		if event.MaxAttempts == 0 {
			event.MaxAttempts = 20
		}
		if event.AvailableAt.IsZero() {
			event.AvailableAt = now
		}
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&events).Error
}

// ListDue returns pending events that are available, newest first is avoided:
// oldest first keeps delivery order. limit bounds each dispatch sweep.
func (r *Repository) ListDue(ctx context.Context, limit int) ([]*Event, error) {
	var events []*Event
	query := r.db.WithContext(ctx).
		Where("status = ? AND available_at <= ?", StatusPending, time.Now().UTC()).
		Order("available_at asc, id asc").
		Limit(limit)
	if r.dbType == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
	}
	if err := query.Find(&events).Error; err != nil {
		return nil, err
	}
	return events, nil
}

// MarkSent records successful delivery.
func (r *Repository) MarkSent(ctx context.Context, id int64) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Model(&Event{}).Where("id = ?", id).
		Updates(map[string]interface{}{"status": StatusSent, "sent_at": now}).Error
}

// RecordFailure counts the attempt with exponential backoff. Events that
// exhaust MaxAttempts go to StatusDead instead of retrying forever.
func (r *Repository) RecordFailure(ctx context.Context, id int64, attempts int, maxAttempts int, cause error, nextAt time.Time) error {
	status := StatusPending
	if attempts >= maxAttempts {
		status = StatusDead
	}
	var lastError *string
	if cause != nil {
		msg := cause.Error()
		if len(msg) > 2000 {
			msg = msg[:2000]
		}
		lastError = &msg
	}
	return r.db.WithContext(ctx).Model(&Event{}).Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":       status,
			"attempts":     attempts,
			"available_at": nextAt,
			"last_error":   lastError,
		}).Error
}

// DeleteSent removes delivered events older than before, keeping the table small.
func (r *Repository) DeleteSent(ctx context.Context, before time.Time) (int64, error) {
	result := r.db.WithContext(ctx).Where("status = ? AND sent_at < ?", StatusSent, before).Delete(&Event{})
	return result.RowsAffected, result.Error
}
