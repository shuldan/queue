package queue

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Envelope struct {
	ID        string            `json:"id"`
	Topic     string            `json:"topic"`
	Data      []byte            `json:"data"`
	Headers   map[string]string `json:"headers,omitempty"`
	Attempt   int               `json:"attempt"`
	CreatedAt time.Time         `json:"created_at"`
}

func newEnvelope(topic string, data []byte, headers map[string]string) *Envelope {
	return &Envelope{
		ID:        uuid.New().String(),
		Topic:     topic,
		Data:      data,
		Headers:   headers,
		CreatedAt: time.Now().UTC(),
	}
}

func marshalEnvelope(env *Envelope) ([]byte, error) {
	return json.Marshal(env)
}

func unmarshalEnvelope(data []byte) (*Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}

	return &env, nil
}
