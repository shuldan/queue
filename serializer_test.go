package queue

import (
	"testing"
)

type testPayload struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func TestJSONSerializer_MarshalUnmarshal_Roundtrip(t *testing.T) {
	t.Parallel()
	s := JSONSerializer{}
	input := testPayload{Name: "test", Value: 42}
	data, err := s.Marshal(input)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	var output testPayload
	if err := s.Unmarshal(data, &output); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if output.Name != input.Name || output.Value != input.Value {
		t.Errorf("expected %+v, got %+v", input, output)
	}
}

func TestJSONSerializer_Marshal_InvalidValue(t *testing.T) {
	t.Parallel()
	s := JSONSerializer{}
	_, err := s.Marshal(make(chan int))
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestJSONSerializer_Unmarshal_InvalidJSON(t *testing.T) {
	t.Parallel()
	s := JSONSerializer{}
	var v testPayload
	err := s.Unmarshal([]byte("{bad"), &v)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestJSONSerializer_Unmarshal_EmptyData(t *testing.T) {
	t.Parallel()
	s := JSONSerializer{}
	var v testPayload
	err := s.Unmarshal([]byte{}, &v)
	if err == nil {
		t.Fatal("expected error for empty data")
	}
}

func TestJSONSerializer_Marshal_Nil(t *testing.T) {
	t.Parallel()
	s := JSONSerializer{}
	data, err := s.Marshal(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "null" {
		t.Errorf("expected 'null', got %q", string(data))
	}
}
