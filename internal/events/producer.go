package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"donetick.com/core/config"
	chModel "donetick.com/core/internal/chore/model"
	uModel "donetick.com/core/internal/user/model"
	"donetick.com/core/logging"
	"go.uber.org/zap"
)

const (
	METHOD_POST       = "POST"
	HEAD_CONTENT_TYPE = "Content-Type"
	CONTENT_TYPE_JSON = "application/json"
)

type EventType string

const (
	EventTypeUnknown      EventType = ""
	EventTypeTaskCreated  EventType = "task.created"
	EventTypeTaskReminder EventType = "task.reminder"
	// EventTypeTaskUpdated    EventType = "task.updated"
	EventTypeTaskCompleted    EventType = "task.completed"
	EventTypeSubTaskCompleted EventType = "subtask.completed"
	// EventTypeTaskReassigned EventType = "task.reassigned"
	EventTypeTaskSkipped  EventType = "task.skipped"
	EventTypeThingChanged EventType = "thing.changed"
)

type Event struct {
	Type      EventType   `json:"type"`
	URL       string      `json:"-"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

type ChoreData struct {
	Chore       *chModel.Chore `json:"chore"`
	Username    string         `json:"username"`
	DisplayName string         `json:"display_name"`
	Note        string         `json:"note"`
}

type EventsProducer struct {
	client *http.Client
	queue  chan Event
	logger *zap.SugaredLogger
}

func (p *EventsProducer) Start(ctx context.Context) {

	p.logger = logging.FromContext(ctx)

	go func() {
		for event := range p.queue {
			p.processEvent(event)
		}
	}()
}

func NewEventsProducer(cfg *config.Config) *EventsProducer {
	return &EventsProducer{
		client: &http.Client{
			Timeout: cfg.WebhookConfig.Timeout,
		},
		queue: make(chan Event, cfg.WebhookConfig.QueueSize),
	}
}

func (p *EventsProducer) publishEvent(event Event) {
	select {
	case p.queue <- event:
		// Successfully added to queue
	default:
		log.Println("Webhook queue is full, dropping event")
	}
}

func (p *EventsProducer) processEvent(event Event) {
	if err := p.SendWebhookNow(context.Background(), event.URL, event); err != nil {
		p.logger.Debugw("Failed to send webhook event", "error", err, "url", event.URL)
	}
}

// SendWebhookNow delivers one webhook event synchronously and returns an error
// on transport or 4xx/5xx failure. Durable retry paths (the outbox dispatcher)
// use this instead of the fire-and-forget queue, so the same failure is simply
// retried later without re-running any business logic.
func (p *EventsProducer) SendWebhookNow(ctx context.Context, target string, event Event) error {
	if target == "" {
		return errors.New("webhook target is empty")
	}
	p.logger.Debugw("Sending webhook event", "type", event.Type, "url", target)

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal webhook event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, METHOD_POST, target, bytes.NewBuffer(eventJSON))
	if err != nil {
		return fmt.Errorf("create webhook request: %w", err)
	}
	req.Header.Set(HEAD_CONTENT_TYPE, CONTENT_TYPE_JSON)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("send webhook event: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook request failed with status %d", resp.StatusCode)
	}
	return nil
}

func (p *EventsProducer) ChoreCreated(ctx context.Context, webhookURL *string, chore *chModel.Chore, creator *uModel.User) {
	if webhookURL == nil || choreHiddenFromCircle(chore) {
		p.logger.Debug("No subscribers for circle, skipping webhook")
		return
	}

	event := Event{
		Type:      EventTypeTaskCreated,
		URL:       *webhookURL,
		Timestamp: time.Now().UTC(),
		Data: ChoreData{
			Chore:       chore,
			Username:    creator.Username,
			DisplayName: creator.DisplayName,
		},
	}
	p.publishEvent(event)
}

func (p *EventsProducer) ChoreCompleted(ctx context.Context, webhookURL *string, chore *chModel.Chore, performer *uModel.User) {
	if webhookURL == nil || choreHiddenFromCircle(chore) {
		p.logger.Debug("No subscribers for circle, skipping webhook")
		return
	}

	event := Event{
		Type:      EventTypeTaskCompleted,
		URL:       *webhookURL,
		Timestamp: time.Now().UTC(),
		Data: ChoreData{Chore: chore,
			Username:    performer.Username,
			DisplayName: performer.DisplayName,
		},
	}
	p.publishEvent(event)
}

func (p *EventsProducer) ChoreSkipped(ctx context.Context, webhookURL *string, chore *chModel.Chore, performer *uModel.User) {
	if webhookURL == nil || choreHiddenFromCircle(chore) {
		p.logger.Debug("No Webhook URL for circle, skipping webhook")
		return
	}

	event := Event{
		Type:      EventTypeTaskSkipped,
		URL:       *webhookURL,
		Timestamp: time.Now().UTC(),
		Data: ChoreData{Chore: chore,
			Username:    performer.Username,
			DisplayName: performer.DisplayName,
		},
	}
	p.publishEvent(event)
}

func (p *EventsProducer) NotificationEvent(ctx context.Context, url string, event interface{}) {
	// print the event and the url :
	p.logger.Debug("Sending notification event")

	p.publishEvent(Event{
		URL:       url,
		Type:      EventTypeTaskReminder,
		Timestamp: time.Now().UTC(),
		Data:      event,
	})
}

func (p *EventsProducer) ThingsUpdated(ctx context.Context, url *string, data interface{}) {
	if url == nil {
		p.logger.Debug("No subscribers for circle, skipping webhook")
		return
	}
	p.publishEvent(Event{
		URL:       *url,
		Type:      EventTypeThingChanged,
		Timestamp: time.Now().UTC(),
		Data:      data,
	})
}

func (p *EventsProducer) SubtaskUpdated(ctx context.Context, url *string, data interface{}) {
	if url == nil {
		p.logger.Debug("No subscribers for circle, skipping webhook")
		return
	}
	p.publishEvent(Event{
		URL:       *url,
		Type:      EventTypeSubTaskCompleted,
		Timestamp: time.Now().UTC(),
		Data:      data,
	})
}

// choreHiddenFromCircle reports whether a chore lives in a project that is not
// visible to the whole circle. The circle webhook is a circle-wide integration, so
// such chores are never sent to it. An unloaded project counts as hidden.
func choreHiddenFromCircle(chore *chModel.Chore) bool {
	return ChoreHiddenFromCircle(chore)
}

// ChoreHiddenFromCircle reports whether a chore lives in a project that is not
// visible to the whole circle. The circle webhook is a circle-wide integration,
// so such chores are never sent to it. An unloaded project counts as hidden.
func ChoreHiddenFromCircle(chore *chModel.Chore) bool {
	if chore == nil {
		return true
	}
	if chore.ProjectID != nil && chore.Project == nil {
		return true
	}
	return chore.Project != nil && chore.Project.IsPrivate
}
