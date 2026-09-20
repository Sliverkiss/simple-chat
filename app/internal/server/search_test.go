package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Web-search chat switch (web-search-research.md Phase 2):
// "search": {"type": "enabled"} must set search_enabled:true on the upstream
// completion payload, stream and non-stream; absent stays false; malformed
// values are a 400 before any upstream call; citations surface in a
// documented non-standard message field.

// searchFixture is an upstream mock that records the search_enabled flag of
// every completion payload and replays a fixed SSE body.
type searchFixture struct {
	srv      *httptest.Server
	search   []bool // search_enabled observed per completion
	thinking []bool
}

func newSearchFixture(t *testing.T, streamBody string) *searchFixture {
	f := &searchFixture{}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"user": map[string]any{"token": "tok", "id": "u1"},
			},
		}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"chat_session": map[string]any{"id": "sess1"},
			},
		}})
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": nil,
		}})
	})
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"code": 0, "msg": "", "data": map[string]any{
			"biz_code": 0, "biz_msg": "", "biz_data": map[string]any{
				"challenge": solvableChallenge(r),
			},
		}})
	})
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		search, _ := body["search_enabled"].(bool)
		thinking, _ := body["thinking_enabled"].(bool)
		f.search = append(f.search, search)
		f.thinking = append(f.thinking, thinking)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, streamBody)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// searchStream: SEARCH fragment with queries + results, then the first
// RESPONSE chunk nested in a response BATCH (live shape, recon-data/search_sse.txt),
// one bare delta, final BATCH.
const searchStream = "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"SEARCH\",\"status\":\"WIP\",\"content\":null,\"queries\":[{\"query\":\"q\"}],\"results\":[]}]}}}\n" +
	"data: {\"p\":\"response/fragments/-1/results\",\"v\":[{\"url\":\"https://a.example\",\"title\":\"A\",\"snippet\":\"sa\",\"site_name\":\"Example\",\"cite_index\":1},{\"url\":\"https://b.example\",\"title\":\"B\",\"snippet\":\"sb\",\"cite_index\":2}]}\n" +
	"data: {\"p\":\"response/fragments/-1\",\"o\":\"BATCH\",\"v\":[{\"p\":\"status\",\"v\":\"FINISHED\"},{\"p\":\"content\",\"v\":\"搜索到 2 个网页\"}]}\n" +
	"data: {\"p\":\"response\",\"o\":\"BATCH\",\"v\":[{\"p\":\"fragments\",\"o\":\"APPEND\",\"v\":[{\"id\":3,\"type\":\"RESPONSE\",\"content\":\"see\",\"references\":[],\"stage_id\":2}]},{\"p\":\"has_pending_fragment\",\"o\":\"SET\",\"v\":false}]}\n" +
	"data: {\"v\":\" [citation:1]\"}\n" +
	"data: {\"p\":\"response\",\"o\":\"BATCH\",\"v\":[{\"p\":\"accumulated_token_usage\",\"v\":9},{\"p\":\"quasi_status\",\"v\":\"FINISHED\"}]}\n" +
	"event: close\ndata: {}\n"

func postSearchCompletion(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestSearchSwitchNonStreamPassthrough(t *testing.T) {
	f := newSearchFixture(t, searchStream)
	srv := newTestServer(t, f.srv.URL)
	resp := postSearchCompletion(t, srv.URL, `{"model":"deepseek-flash","messages":[{"role":"user","content":"news"}],"search":{"type":"enabled"}}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(f.search) != 1 || f.search[0] != true {
		t.Errorf("search_enabled upstream = %v, want [true]", f.search)
	}
	if len(f.thinking) != 1 || f.thinking[0] != true {
		t.Errorf("thinking default must stay on, got %v", f.thinking)
	}

	var body struct {
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				Citations []struct {
					URL   string `json:"url"`
					Title string `json:"title"`
				} `json:"citations"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Choices) != 1 {
		t.Fatalf("choices = %d", len(body.Choices))
	}
	msg := body.Choices[0].Message
	if msg.Content != "see [citation:1]" {
		t.Errorf("content = %q", msg.Content)
	}
	if len(msg.Citations) != 2 || msg.Citations[0].URL != "https://a.example" || msg.Citations[1].Title != "B" {
		t.Errorf("citations = %+v", msg.Citations)
	}
}

func TestSearchSwitchDefaultOff(t *testing.T) {
	f := newSearchFixture(t, searchStream)
	srv := newTestServer(t, f.srv.URL)
	resp := postSearchCompletion(t, srv.URL, `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(f.search) != 1 || f.search[0] != false {
		t.Errorf("search_enabled upstream = %v, want [false] by default", f.search)
	}
}

func TestSearchSwitchStreamPassthroughAndCitations(t *testing.T) {
	f := newSearchFixture(t, searchStream)
	srv := newTestServer(t, f.srv.URL)
	resp := postSearchCompletion(t, srv.URL, `{"model":"deepseek-flash","messages":[{"role":"user","content":"news"}],"stream":true,"search":{"type":"enabled"}}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(f.search) != 1 || f.search[0] != true {
		t.Errorf("search_enabled upstream = %v, want [true]", f.search)
	}
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	if !strings.Contains(s, `"content":"see"`) {
		t.Errorf("stream missing first content chunk (BATCH-nested fragment): %.200s", s)
	}
	if !strings.Contains(s, "[citation:1]") {
		t.Errorf("stream missing citation marker chunk: %.200s", s)
	}
	if !strings.Contains(s, `"citations":[`) {
		t.Errorf("final chunk missing citations field: %.300s", s)
	}
}

func TestSearchSwitchMalformedIs400(t *testing.T) {
	f := newSearchFixture(t, searchStream)
	srv := newTestServer(t, f.srv.URL)
	for _, body := range []string{
		`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"search":{"type":"yes"}}`,
		`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"search":"enabled"}`,
		`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"search":{"type":123}}`,
		`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"search":{"enabled":true}}`,
	} {
		resp := postSearchCompletion(t, srv.URL, body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("body %.60s: status = %d, want 400", body, resp.StatusCode)
		}
	}
	if len(f.search) != 0 {
		t.Errorf("malformed requests must not reach upstream; got %d completions", len(f.search))
	}
}
