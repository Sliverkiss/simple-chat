package sse

import (
	"errors"
	"os"
	"strings"
	"testing"
)

// collect drains the interpreter and returns the final state.
func collect(t *testing.T, input string) State {
	t.Helper()
	interp := New()
	if err := interp.Feed(input); err != nil {
		t.Fatalf("feed: %v", err)
	}
	return interp.Snapshot()
}

func TestReplayLiveCompletionSample(t *testing.T) {
	b, err := os.ReadFile("../recon-data/completion_sample.txt")
	if err != nil {
		t.Skipf("sample not available: %v", err)
	}
	st := collect(t, string(b))
	want := "你好！有什么我可以帮你的吗？"
	if st.Text != want {
		t.Errorf("Text = %q, want %q", st.Text, want)
	}
	if st.Usage.TotalTokens != 41 {
		t.Errorf("Usage.TotalTokens = %d, want 41", st.Usage.TotalTokens)
	}
	if !st.Finished {
		t.Error("expected Finished after event: close")
	}
	if st.Err != nil {
		t.Errorf("unexpected stream error: %v", st.Err)
	}
}

func TestReplayLiveVisionSample(t *testing.T) {
	b, err := os.ReadFile("../recon-data/vision_sse.txt")
	if err != nil {
		t.Skipf("sample not available: %v", err)
	}
	st := collect(t, string(b))
	if !strings.Contains(st.Text, "left") || !strings.Contains(st.Text, "red") {
		t.Errorf("Text should describe red left half, got: %.200s", st.Text)
	}
	if !st.Finished {
		t.Error("expected Finished")
	}
	if st.Usage.TotalTokens == 0 {
		t.Error("expected nonzero usage")
	}
}

func TestOmittedPathAndOpAreSticky(t *testing.T) {
	st := collect(t, "data: {\"p\":\"response/fragments/-1/content\",\"o\":\"APPEND\",\"v\":\"he\"}\n\ndata: {\"v\":\"llo\"}\n")
	if st.Text != "hello" {
		t.Errorf("Text = %q, want %q", st.Text, "hello")
	}
}

func TestBatchOpsAppliedInOrder(t *testing.T) {
	st := collect(t, `data: {"p":"response","o":"BATCH","v":[{"p":"accumulated_token_usage","v":123},{"p":"quasi_status","v":"FINISHED"}]}
`)
	if st.Usage.TotalTokens != 123 {
		t.Errorf("usage = %d, want 123", st.Usage.TotalTokens)
	}
}

func TestStatusSetFinished(t *testing.T) {
	st := collect(t, `data: {"p":"response/status","o":"SET","v":"FINISHED"}
`)
	if !st.Finished {
		t.Error("response/status SET FINISHED must set Finished")
	}
}

func TestThinkFragmentsMapToReasoning(t *testing.T) {
	input := "data: {\"v\":{\"response\":{\"fragments\":[" +
		"{\"id\":1,\"type\":\"THINK\",\"content\":\"pondering\"}," +
		"{\"id\":2,\"type\":\"RESPONSE\",\"content\":\"answer\"}]}}}\n" +
		"data: {\"v\":\" more\"}\n"
	st := collect(t, input)
	if st.Reasoning != "pondering" {
		t.Errorf("Reasoning = %q, want %q", st.Reasoning, "pondering")
	}
	if st.Text != "answer more" {
		t.Errorf("Text = %q, want %q", st.Text, "answer more")
	}
}

func TestSkipNoisePaths(t *testing.T) {
	input := "data: {\"p\":\"quasi_status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n" +
		"data: {\"p\":\"response/fragments/-1/status\",\"o\":\"SET\",\"v\":\"WIP\"}\n" +
		"data: {\"p\":\"elapsed_secs\",\"o\":\"SET\",\"v\":3}\n"
	st := collect(t, input)
	if st.Finished {
		t.Error("quasi_status FINISHED must not mark stream finished")
	}
	if st.Text != "" {
		t.Errorf("noise must not produce text, got %q", st.Text)
	}
}

func TestMidStreamErrorObject(t *testing.T) {
	st := collect(t, "data: {\"v\":{\"response\":{\"fragments\":[{\"id\":2,\"type\":\"RESPONSE\",\"content\":\"partial\"}]}}}\ndata: {\"error\":{\"code\":\"content_filter\"}}\n")
	if st.Err == nil {
		t.Fatal("expected stream error")
	}
	if !strings.Contains(st.Err.Error(), "content_filter") {
		t.Errorf("error should mention content_filter, got %v", st.Err)
	}
	if st.Text != "partial" {
		t.Errorf("text before error should be preserved, got %q", st.Text)
	}
}

// A generic (non-content-filter) error object keeps the plain stream-error
// shape and must NOT match the content_filter sentinel.
func TestGenericErrorObjectNotContentFilter(t *testing.T) {
	st := collect(t, "data: {\"error\":\"server exploded\"}\n")
	if st.Err == nil {
		t.Fatal("expected stream error")
	}
	if strings.Contains(st.Err.Error(), "content_filter") {
		t.Errorf("generic error must not be typed content_filter: %v", st.Err)
	}
	if errors.Is(st.Err, ErrContentFilter) {
		t.Errorf("generic error must not match the ErrContentFilter sentinel: %v", st.Err)
	}
}

func TestDoneSentinelTolerated(t *testing.T) {
	st := collect(t, "data: {\"v\":{\"response\":{\"fragments\":[{\"id\":2,\"type\":\"RESPONSE\",\"content\":\"hi\"}]}}}\ndata: [DONE]\n")
	if st.Text != "hi" {
		t.Errorf("Text = %q", st.Text)
	}
	if !st.Finished {
		t.Error("[DONE] must finish the stream")
	}
}

func TestIncrementalFeed(t *testing.T) {
	interp := New()
	var deltas []string
	interp.SetOnDelta(func(d Delta) {
		// Callbacks fire under the interpreter lock; only append here.
		if d.Text != "" {
			deltas = append(deltas, d.Text)
		}
	})
	for _, chunk := range []string{"data: {\"p\":\"response/fragm", "ents/-1/content\",\"o\":\"APPEND\",\"v\":\"a\"}\n\ndata: {\"v\":\"b\"}\n"} {
		if err := interp.Feed(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Join(deltas, ""); got != "ab" {
		t.Errorf("deltas = %q, want %q", got, "ab")
	}
	if interp.Snapshot().Text != "ab" {
		t.Errorf("Text = %q", interp.Snapshot().Text)
	}
	if !interp.Snapshot().Finished && len(deltas) == 0 {
		t.Error("no deltas observed")
	}
}

func TestEventCloseTerminates(t *testing.T) {
	st := collect(t, "event: close\ndata: {\"click_behavior\":\"none\"}\n")
	if !st.Finished {
		t.Error("event: close must set Finished")
	}
}

func TestNonDataLinesIgnored(t *testing.T) {
	st := collect(t, "event: ready\ndata: {\"request_message_id\":1}\n\nevent: title\ndata: {\"content\":\"你好\"}\n")
	if st.Text != "" {
		t.Errorf("ready/title payloads must not emit text, got %q", st.Text)
	}
}

// The upstream signals mid-stream errors via `event: hint` with a JSON
// payload {"type":"error","content":"...","clear_response":true,
// "finish_reason":"parallel_chat_limit"} (live-captured 2026-09-19: another
// generation is running on the same account). The interpreter must surface
// it, not swallow it into an empty completion.
func TestHintErrorSurfaces(t *testing.T) {
	interp := New()
	stream := "event: ready\ndata: {\"request_message_id\":1,\"response_message_id\":2,\"model_type\":\"default\"}\n\n" +
		"event: hint\ndata: {\"type\":\"error\",\"content\":\"another generation is running\",\"clear_response\":true,\"finish_reason\":\"parallel_chat_limit\"}\n\n" +
		"event: close\ndata: {}\n"
	if err := interp.Feed(stream); err != nil {
		t.Fatalf("feed: %v", err)
	}
	st := interp.Snapshot()
	if st.Err == nil {
		t.Fatal("hint error must surface as state error")
	}
	var se *StreamError
	if !errors.As(st.Err, &se) {
		t.Fatalf("want *StreamError, got %T", st.Err)
	}
	if se.FinishReason != "parallel_chat_limit" {
		t.Errorf("finish_reason = %q", se.FinishReason)
	}
	if !strings.Contains(se.Content, "another generation") {
		t.Errorf("content = %q", se.Content)
	}
}

func TestStreamErrorMethods(t *testing.T) {
	e := &StreamError{Type: "error", Content: "busy", FinishReason: "parallel_chat_limit"}
	if !e.IsParallelLimit() {
		t.Error("IsParallelLimit")
	}
	if e.Error() != "stream error (parallel_chat_limit): busy" {
		t.Errorf("Error() = %q", e.Error())
	}
	if (&StreamError{Content: "x"}).Error() != "stream error: x" {
		t.Error("plain message variant")
	}
	if (&StreamError{Content: "x"}).IsParallelLimit() {
		t.Error("no finish reason = not parallel limit")
	}
}

func TestContentFilterCode(t *testing.T) {
	interp := New()
	stream := "data: {\"code\":\"content_filter\",\"error\":\"blocked\"}\n\n"
	if err := interp.Feed(stream); err != nil {
		t.Fatal(err)
	}
	st := interp.Snapshot()
	if st.Err == nil || !strings.Contains(st.Err.Error(), "blocked") {
		t.Errorf("err = %v", st.Err)
	}
	if !errors.Is(st.Err, ErrContentFilter) {
		t.Errorf("content_filter code must match ErrContentFilter, got %v", st.Err)
	}
}

// An error object naming content_filter (no code field) also matches the
// sentinel: the gateway keys the typed 400 off errors.Is.
func TestContentFilterErrorObjectMatchesSentinel(t *testing.T) {
	interp := New()
	stream := "data: {\"error\":{\"code\":\"content_filter\"}}\n\n"
	if err := interp.Feed(stream); err != nil {
		t.Fatal(err)
	}
	st := interp.Snapshot()
	if !errors.Is(st.Err, ErrContentFilter) {
		t.Errorf("error object naming content_filter must match ErrContentFilter, got %v", st.Err)
	}
}

func TestOnDeltaCallback(t *testing.T) {
	interp := New()
	var got []Delta
	interp.OnDelta(func(d Delta) { got = append(got, d) })
	if err := interp.Feed("data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"a\"}]}}}\n"); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Text != "a" {
		t.Errorf("deltas = %v", got)
	}
}

func TestFragmentsAppendPath(t *testing.T) {
	interp := New()
	stream := "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"start\"}]}}}\n" +
		"data: {\"p\":\"response/fragments\",\"o\":\"APPEND\",\"v\":[{\"type\":\"RESPONSE\",\"content\":\" more\"}]}\n" +
		"data: {\"p\":\"response/fragments/-1/content\",\"v\":\" tail\"}\n"
	if err := interp.Feed(stream); err != nil {
		t.Fatal(err)
	}
	st := interp.Snapshot()
	if st.Text != "start more tail" {
		t.Errorf("text = %q", st.Text)
	}
}

// Reasoning-mode regressions (thinking default ON):
//   1. bare-string deltas after the initial payload must continue the LAST
//      fragment's kind — a THINK fragment followed by {"v":"..."} deltas is
//      all reasoning, not content;
//   2. a bare string on the response/fragments path (after a fragment APPEND,
//      p/o omitted) appends to the current fragment kind.
func TestBareDeltaContinuesLastFragmentKind(t *testing.T) {
	interp := New()
	stream := "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"THINK\",\"content\":\"think \"}]}}}\n" +
		"data: {\"v\":\"deeply \"}\n" +
		"data: {\"p\":\"response/fragments\",\"o\":\"APPEND\",\"v\":[{\"type\":\"RESPONSE\",\"content\":\"ans\"}]}\n" +
		"data: {\"v\":\"wer\"}\n"
	if err := interp.Feed(stream); err != nil {
		t.Fatal(err)
	}
	st := interp.Snapshot()
	if st.Reasoning != "think deeply " {
		t.Errorf("reasoning = %q, want %q", st.Reasoning, "think deeply ")
	}
	if st.Text != "answer" {
		t.Errorf("text = %q, want %q", st.Text, "answer")
	}
}

// Delta order across the think→response transition: reasoning deltas fire
// before content deltas, interleaved exactly as the fragments arrive.
func TestDeltaOrderAcrossTransition(t *testing.T) {
	interp := New()
	var order []string
	interp.SetOnDelta(func(d Delta) {
		switch {
		case d.Reasoning != "":
			order = append(order, "R:"+d.Reasoning)
		case d.Text != "":
			order = append(order, "T:"+d.Text)
		}
	})
	stream := "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"THINK\",\"content\":\"a\"}]}}}\n" +
		"data: {\"v\":\"b\"}\n" +
		"data: {\"p\":\"response/fragments\",\"o\":\"APPEND\",\"v\":[{\"type\":\"RESPONSE\",\"content\":\"c\"}]}\n" +
		"data: {\"v\":\"d\"}\n"
	if err := interp.Feed(stream); err != nil {
		t.Fatal(err)
	}
	want := []string{"R:a", "R:b", "T:c", "T:d"}
	if len(order) != len(want) {
		t.Fatalf("deltas = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("delta[%d] = %q, want %q (order: %v)", i, order[i], want[i], order)
		}
	}
}
