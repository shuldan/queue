package queue

import (
	"testing"
	"time"
)

func TestNewEnvelope_Fields(t *testing.T) {
	t.Parallel()
	before := time.Now().UTC()
	env := newEnvelope("orders", []byte("data"), map[string]string{"h": "v"})
	after := time.Now().UTC()

	if env.ID == "" {
		t.Error("expected non-empty ID")
	}
	if env.Topic != "orders" {
		t.Errorf("expected topic 'orders', got %q", env.Topic)
	}
	if string(env.Data) != "data" {
		t.Errorf("expected data 'data', got %q", string(env.Data))
	}
	if env.Headers["h"] != "v" {
		t.Errorf("expected header h=v, got %v", env.Headers)
	}
	if env.Attempt != 0 {
		t.Errorf("expected attempt 0, got %d", env.Attempt)
	}
	if env.CreatedAt.Before(before) || env.CreatedAt.After(after) {
		t.Errorf("CreatedAt out of range: %v", env.CreatedAt)
	}
}

func TestNewEnvelope_NilHeaders(t *testing.T) {
	t.Parallel()
	env := newEnvelope("t", []byte("x"), nil)
	if env.Headers != nil {
		t.Errorf("expected nil headers, got %v", env.Headers)
	}
}

func TestMarshalUnmarshalEnvelope_Roundtrip(t *testing.T) {
	t.Parallel()
	env := newEnvelope("t", []byte("payload"), map[string]string{"a": "b"})
	data, err := marshalEnvelope(env)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	got, err := unmarshalEnvelope(data)
	if err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if got.ID != env.ID {
		t.Errorf("ID: expected %q, got %q", env.ID, got.ID)
	}
	if got.Topic != env.Topic {
		t.Errorf("Topic: expected %q, got %q", env.Topic, got.Topic)
	}
	if string(got.Data) != string(env.Data) {
		t.Errorf("Data: expected %q, got %q", env.Data, got.Data)
	}
}

func TestUnmarshalEnvelope_InvalidJSON(t *testing.T) {
	t.Parallel()
	_, err := unmarshalEnvelope([]byte("{invalid"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestUnmarshalEnvelope_EmptyData(t *testing.T) {
	t.Parallel()
	_, err := unmarshalEnvelope([]byte{})
	if err == nil {
		t.Fatal("expected error for empty data")
	}
}

func TestMarshalEnvelope_NilEnvelope(t *testing.T) {
	t.Parallel()
	data, err := marshalEnvelope(nil)
	if err != nil {
		t.Fatalf("expected no error for nil envelope, got %v", err)
	}
	if string(data) != "null" {
		t.Errorf("expected 'null', got %q", string(data))
	}
}
