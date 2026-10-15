package outbox

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	chModel "donetick.com/core/internal/chore/model"
)

// Kind is the delivery channel of an outbox event.
type Kind string

const (
	// KindWebhook is an HTTP webhook POST (events.EventsProducer).
	KindWebhook Kind = "webhook"
	// KindRealtime is a realtime broadcast to connected circle members.
	KindRealtime Kind = "realtime"
)

// Status values for outbox events.
const (
	StatusPending = "pending"
	StatusSent    = "sent"
	// StatusDead means delivery exhausted MaxAttempts and will not be retried.
	StatusDead = "dead"
)

// Event is one durable external delivery. Rows are written inside the same
// database transaction as the state change they announce, so an event exists
// if and only if the change committed. Delivery is retried asynchronously;
// the settled task itself is never touched again.
type Event struct {
	ID          int64      `json:"id" gorm:"primaryKey;autoIncrement"`
	CircleID    int        `json:"circleId" gorm:"column:circle_id;index;not null"`
	ChoreID     int        `json:"choreId" gorm:"column:chore_id;index;not null"`
	HistoryID   *int       `json:"historyId,omitempty" gorm:"column:history_id;uniqueIndex:idx_outbox_dedup,priority:1"` // completion generation this event belongs to
	Kind        Kind       `json:"kind" gorm:"column:kind;size:16;not null;uniqueIndex:idx_outbox_dedup,priority:2"`
	EventType   string     `json:"eventType" gorm:"column:event_type;size:64;not null;uniqueIndex:idx_outbox_dedup,priority:3"`
	Target      string     `json:"target,omitempty" gorm:"column:target;size:1024;not null;default:'';uniqueIndex:idx_outbox_dedup,priority:4"` // webhook URL; empty for realtime
	Payload     Payload    `json:"payload" gorm:"column:payload;type:json"`
	Status      string     `json:"status" gorm:"column:status;size:16;not null;default:pending;index"`
	Attempts    int        `json:"attempts" gorm:"column:attempts;not null;default:0"`
	MaxAttempts int        `json:"maxAttempts" gorm:"column:max_attempts;not null;default:20"`
	AvailableAt time.Time  `json:"availableAt" gorm:"column:available_at;index;not null"`
	SentAt      *time.Time `json:"sentAt,omitempty" gorm:"column:sent_at"`
	LastError   *string    `json:"lastError,omitempty" gorm:"column:last_error;type:text"`
	CreatedAt   time.Time  `json:"createdAt" gorm:"column:created_at;autoCreateTime"`
	UpdatedAt   time.Time  `json:"updatedAt" gorm:"column:updated_at;autoUpdateTime"`
}

// TableName pins the table name.
func (Event) TableName() string { return "outbox_events" }

// Payload carries what the dispatcher needs to deliver the event. Webhook rows
// snapshot everything (the event must stay identical on retry); realtime rows
// carry references and reload the current chore/history at delivery time.
type Payload struct {
	// Webhook snapshot.
	Chore       *chModel.Chore `json:"chore,omitempty"`
	Username    string         `json:"username,omitempty"`
	DisplayName string         `json:"displayName,omitempty"`
	Note        *string        `json:"note,omitempty"`

	// Realtime reconstruction.
	PerformerID int `json:"performerId,omitempty"`
}

// Value serializes Payload to JSON.
func (p Payload) Value() (driver.Value, error) {
	return json.Marshal(p)
}

// Scan deserializes Payload from JSON.
func (p *Payload) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case []byte:
		return json.Unmarshal(v, p)
	case string:
		return json.Unmarshal([]byte(v), p)
	default:
		return errors.New("type assertion to []byte or string failed")
	}
}
