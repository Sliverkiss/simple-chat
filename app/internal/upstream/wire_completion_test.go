package upstream

// Wire alignment — completion body encoding (apk-alignment.md I3, revised).
//
// The app's Json config has encodeDefaults=true (ak5/bi5; zyb.A0() returns
// true), so ChatFullCompletionRequest (qj1/sj1) serializes every field on
// every send: ref_file_ids (empty array when no files), thinking_enabled
// and search_enabled as literal booleans, and preempt unconditionally
// (no default mask; false = non-preempting send).

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
)

func TestCompletionBodyEncodesDefaults(t *testing.T) {
	var mu sync.Mutex
	var bodies []map[string]any

	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
		"/api/v0/chat/completion": func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			bodies = append(bodies, body)
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"v\":\"ok\"}\n")
			io.WriteString(w, "event: close\ndata: {}\n")
		},
	})
	defer m.srv.Close()

	c := m.client()
	// Default send: no files, thinking on (zero ThinkingDisabled), search off.
	s1, err := c.Completion(context.Background(), "tok1", CompletionRequest{SessionID: "s", Prompt: "p"})
	if err != nil {
		t.Fatal(err)
	}
	s1.Close()
	// Thinking off + a file ref: the literal-false and non-empty-array
	// encodings.
	s2, err := c.Completion(context.Background(), "tok1", CompletionRequest{SessionID: "s", Prompt: "p", ThinkingDisabled: true, RefFileIDs: []string{"f1"}})
	if err != nil {
		t.Fatal(err)
	}
	s2.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("captured %d bodies, want 2", len(bodies))
	}
	for i, body := range bodies {
		if v, ok := body["preempt"]; !ok || v != false {
			t.Errorf("body %d: preempt = %v (%t), want false present", i, v, ok)
		}
		if _, ok := body["parent_message_id"]; !ok {
			t.Errorf("body %d: parent_message_id missing", i)
		}
	}
	if v, ok := bodies[0]["ref_file_ids"]; !ok {
		t.Error("body 0: ref_file_ids missing; encodeDefaults=true always sends it")
	} else if arr, ok := v.([]any); !ok || len(arr) != 0 {
		t.Errorf("body 0: ref_file_ids = %v, want []", v)
	}
	if v := bodies[1]["ref_file_ids"]; v == nil {
		t.Error("body 1: ref_file_ids missing")
	} else if arr, ok := v.([]any); !ok || len(arr) != 1 || arr[0] != "f1" {
		t.Errorf("body 1: ref_file_ids = %v, want [f1]", v)
	}
	if v, ok := bodies[0]["thinking_enabled"]; !ok || v != true {
		t.Errorf("thinking on: thinking_enabled = %v (%t), want true", v, ok)
	}
	if v, ok := bodies[1]["thinking_enabled"]; !ok || v != false {
		t.Errorf("thinking off: thinking_enabled = %v (%t), want false (encodeDefaults)", v, ok)
	}
	if v, ok := bodies[0]["search_enabled"]; !ok || v != false {
		t.Errorf("search off: search_enabled = %v (%t), want false present", v, ok)
	}
}

// TestCompletionBodySamplingParams pins the openai-compat pass-through
// (apk-alignment.md K3): the app never sends sampling params, but the
// upstream tolerates them and the gateway forwards them when nonzero
// (app/docs-upstream-params.md). Zero values are omitted.
func TestCompletionBodySamplingParams(t *testing.T) {
	var mu sync.Mutex
	var bodies []map[string]any

	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"challenge": solvableChallenge("/api/v0/chat/completion")})
		},
		"/api/v0/chat/completion": func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			bodies = append(bodies, body)
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"v\":\"ok\"}\n")
			io.WriteString(w, "event: close\ndata: {}\n")
		},
	})
	defer m.srv.Close()

	c := m.client()
	s1, err := c.Completion(context.Background(), "tok1", CompletionRequest{SessionID: "s", Prompt: "p",
		Temperature: 0.7, TopP: 0.9, MaxTokens: 512})
	if err != nil {
		t.Fatal(err)
	}
	s1.Close()
	// Zero values: all three keys omitted (documented pass-through contract).
	s2, err := c.Completion(context.Background(), "tok1", CompletionRequest{SessionID: "s", Prompt: "p"})
	if err != nil {
		t.Fatal(err)
	}
	s2.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("captured %d bodies, want 2", len(bodies))
	}
	if v, ok := bodies[0]["temperature"]; !ok || v != 0.7 {
		t.Errorf("body 0: temperature = %v (%t), want 0.7", v, ok)
	}
	if v, ok := bodies[0]["top_p"]; !ok || v != 0.9 {
		t.Errorf("body 0: top_p = %v (%t), want 0.9", v, ok)
	}
	if v, ok := bodies[0]["max_tokens"]; !ok || v != float64(512) {
		t.Errorf("body 0: max_tokens = %v (%t), want 512", v, ok)
	}
	for _, k := range []string{"temperature", "top_p", "max_tokens"} {
		if _, ok := bodies[1][k]; ok {
			t.Errorf("body 1: %s must be omitted when zero, got %v", k, bodies[1][k])
		}
	}
}
