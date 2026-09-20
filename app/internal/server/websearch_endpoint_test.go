package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// POST /v1/web_search (web-search-research.md): a minimal standalone search
// surface backed by the upstream SEARCH fragment's structured results.
// One completion with search on + thinking off; response carries the
// collected queries and results. Malformed body → 400; upstream errors map
// through the same error surface as chat completions.

func TestWebSearchEndpointReturnsResults(t *testing.T) {
	f := newSearchFixture(t, searchStream)
	srv := newTestServer(t, f.srv.URL)
	resp, err := http.Post(srv.URL+"/v1/web_search", "application/json",
		strings.NewReader(`{"query":"latest tech news"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(f.search) != 1 || f.search[0] != true {
		t.Errorf("search_enabled upstream = %v, want [true]", f.search)
	}
	if len(f.thinking) != 1 || f.thinking[0] != false {
		t.Errorf("thinking_enabled upstream = %v, want [false] (forced off to stay on the simple SEARCH path)", f.thinking)
	}

	var body struct {
		Object  string   `json:"object"`
		Queries []string `json:"queries"`
		Results []struct {
			URL       string `json:"url"`
			Title     string `json:"title"`
			Snippet   string `json:"snippet"`
			CiteIndex int    `json:"cite_index"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Object != "web_search" {
		t.Errorf("object = %q", body.Object)
	}
	if len(body.Queries) != 1 || body.Queries[0] != "q" {
		t.Errorf("queries = %+v", body.Queries)
	}
	if len(body.Results) != 2 {
		t.Fatalf("results = %d, want 2", len(body.Results))
	}
	if body.Results[0].URL != "https://a.example" || body.Results[0].Title != "A" || body.Results[0].CiteIndex != 1 {
		t.Errorf("results[0] = %+v", body.Results[0])
	}
	if body.Results[1].URL != "https://b.example" {
		t.Errorf("results[1] = %+v", body.Results[1])
	}
}

func TestWebSearchEndpointMalformedBody(t *testing.T) {
	f := newSearchFixture(t, searchStream)
	srv := newTestServer(t, f.srv.URL)
	for _, body := range []string{
		`{}`,
		`{"query":""}`,
		`{"query":123}`,
		`not json`,
	} {
		resp, err := http.Post(srv.URL+"/v1/web_search", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("body %.30s: status = %d, want 400", body, resp.StatusCode)
		}
	}
	if len(f.search) != 0 {
		t.Errorf("malformed requests must not reach upstream, got %d completions", len(f.search))
	}
}

// A search stream with zero results is an honest empty result list, not an
// error and not a silent retry loop.
func TestWebSearchEndpointEmptyResults(t *testing.T) {
	emptyStream := "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"SEARCH\",\"status\":\"WIP\",\"content\":null,\"queries\":[{\"query\":\"q\"}],\"results\":[]}]}}}\n" +
		"data: {\"p\":\"response/fragments/-1\",\"o\":\"BATCH\",\"v\":[{\"p\":\"status\",\"v\":\"FINISHED\"},{\"p\":\"content\",\"v\":\"搜索到 0 个网页\"}]}\n" +
		"data: {\"p\":\"response\",\"o\":\"BATCH\",\"v\":[{\"p\":\"fragments\",\"o\":\"APPEND\",\"v\":[{\"id\":3,\"type\":\"RESPONSE\",\"content\":\"no idea\",\"references\":[],\"stage_id\":2}]},{\"p\":\"has_pending_fragment\",\"o\":\"SET\",\"v\":false}]}\n" +
		"data: {\"p\":\"response\",\"o\":\"BATCH\",\"v\":[{\"p\":\"accumulated_token_usage\",\"v\":3},{\"p\":\"quasi_status\",\"v\":\"FINISHED\"}]}\n" +
		"event: close\ndata: {}\n"
	f := newSearchFixture(t, emptyStream)
	srv := newTestServer(t, f.srv.URL)
	resp, err := http.Post(srv.URL+"/v1/web_search", "application/json",
		strings.NewReader(`{"query":"something obscure"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var body struct {
		Results []json.RawMessage `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Results) != 0 {
		t.Errorf("results = %d, want 0", len(body.Results))
	}
	if len(f.search) != 1 {
		t.Errorf("empty results must not trigger retries, got %d completions", len(f.search))
	}
}

// Web-search error mapping: a completion call that fails retryably (transport
// error) exhausts the one-switch retry and surfaces a 502 — not a panic, not
// a hang, and exactly one client response.
func TestWebSearchEndpointRetryExhaustion(t *testing.T) {
	var completions atomic.Int32 // handler runs on a server goroutine; the
	// test body reads it after Post returns with no happens-before edge.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) { sessOK(w, "s1") })
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		completions.Add(1)
		hj, _ := w.(http.Hijacker)
		if conn, _, err := hj.Hijack(); err == nil {
			conn.Close() // transport error mid-handshake
		}
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	up := httptest.NewServer(mux)
	defer up.Close()

	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/web_search", "application/json",
		strings.NewReader(`{"query":"anything"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	if completions.Load() < 1 {
		t.Errorf("upstream completion called %d times", completions.Load())
	}
}

// Mid-stream error mapping: a stream that carries an upstream error object
// surfaces the mapped client error (502 upstream_failure), not a 200 with
// no body.
func TestWebSearchEndpointStreamError(t *testing.T) {
	errStream := "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"SEARCH\",\"status\":\"WIP\",\"content\":null,\"queries\":[],\"results\":[]}]}}}\n" +
		"data: {\"error\":\"internal failure\"}\n" +
		"event: close\ndata: {}\n"
	f := newSearchFixture(t, errStream)
	srv := newTestServer(t, f.srv.URL)
	resp, err := http.Post(srv.URL+"/v1/web_search", "application/json",
		strings.NewReader(`{"query":"anything"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

// Transport cut after headers (readErr path): honest 502, no partial JSON.
func TestWebSearchEndpointTransportCut(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/users/login", func(w http.ResponseWriter, r *http.Request) { authOK(w) })
	mux.HandleFunc("POST /api/v0/chat_session/create", func(w http.ResponseWriter, r *http.Request) { sessOK(w, "s1") })
	mux.HandleFunc("POST /api/v0/chat/create_pow_challenge", func(w http.ResponseWriter, r *http.Request) { powOK(w, r) })
	mux.HandleFunc("POST /api/v0/chat/completion", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		hj, _ := w.(http.Hijacker)
		if conn, _, err := hj.Hijack(); err == nil {
			conn.Close() // cut mid-stream after headers
		}
	})
	mux.HandleFunc("POST /api/v0/chat_session/delete", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, mc{"code": 0, "msg": "", "data": mc{"biz_code": 0, "biz_msg": "", "biz_data": nil}})
	})
	up := httptest.NewServer(mux)
	defer up.Close()

	gw := newTestServer(t, up.URL)
	defer gw.Close()

	resp, err := http.Post(gw.URL+"/v1/web_search", "application/json",
		strings.NewReader(`{"query":"anything"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}
