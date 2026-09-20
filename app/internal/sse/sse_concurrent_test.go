package sse

import (
	"sync"
	"testing"
)

// TestInterpreterPerRequestIsolation feeds one captured upstream stream into
// many concurrent interpreters — each request must decode its own stream
// independently (the interpreter is per-request, never shared).
func TestInterpreterPerRequestIsolation(t *testing.T) {
	stream := "event: ready\ndata: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"RESPONSE\",\"content\":\"ok\"}]}}}\n\n" +
		"data: {\"p\":\"response\",\"o\":\"BATCH\",\"v\":[{\"p\":\"accumulated_token_usage\",\"v\":40},{\"p\":\"quasi_status\",\"v\":\"FINISHED\"}]}\n\n" +
		"data: {\"p\":\"response/status\",\"o\":\"SET\",\"v\":\"FINISHED\"}\n\n" +
		"event: close\ndata: {}\n"
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			interp := New()
			if err := interp.Feed(stream); err != nil {
				t.Errorf("feed: %v", err)
			}
			st := interp.Snapshot()
			if st.Text != "ok" {
				t.Errorf("text = %q, want ok", st.Text)
			}
			if !st.Finished {
				t.Error("not finished")
			}
			if st.Usage.TotalTokens != 40 {
				t.Errorf("usage = %+v, want 40 total tokens", st.Usage)
			}
		}()
	}
	wg.Wait()
}
