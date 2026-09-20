package upstream

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// sessionDrawer is a paginated mock of chat_session/fetch_page: newest
// first, has_more until the last page, and strict-cursor semantics (a
// request's cursor must reference a listed entry; the response returns
// entries strictly older than it).
type sessionDrawer struct {
	mu      chan struct{} // unused; single-threaded tests
	entries []SessionInfo
}

func (d *sessionDrawer) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/v0/chat_session/fetch_page" {
		http.NotFound(w, r)
		return
	}
	q := r.URL.Query()
	// Collect the page: all entries strictly older than the cursor entry
	// (matched by pinned+updated_at), or the whole drawer when parameterless.
	olderThan := -1.0
	if q.Has("lte_cursor.pinned") || q.Has("lte_cursor.updated_at") {
		ua, err := strconv.ParseFloat(q.Get("lte_cursor.updated_at"), 64)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		olderThan = ua
	}
	var page []SessionInfo
	for _, s := range d.entries { // newest first
		if s.UpdatedAt < olderThan || olderThan < 0 {
			page = append(page, s)
		}
	}
	const pageSize = 2
	hasMore := len(page) > pageSize
	if hasMore {
		page = page[:pageSize]
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"chat_sessions":[`)
	for i, s := range page {
		if i > 0 {
			fmt.Fprint(w, ",")
		}
		fmt.Fprintf(w, `{"id":%q,"pinned":%t,"updated_at":%s}`, s.ID, s.Pinned,
			strconv.FormatFloat(s.UpdatedAt, 'f', -1, 64))
	}
	fmt.Fprintf(w, `],"has_more":%t}}}`, hasMore)
}

// TestListSessionsSinglePage: a drawer that fits one parameterless page
// returns its sessions (newest first) with one request.
func TestListSessionsSinglePage(t *testing.T) {
	d := &sessionDrawer{entries: []SessionInfo{
		{ID: "new", Pinned: false, UpdatedAt: 300},
		{ID: "mid", Pinned: true, UpdatedAt: 200},
		{ID: "old", Pinned: false, UpdatedAt: 100},
	}}
	srv := httptest.NewServer(http.HandlerFunc(d.handle))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, Mobile: "13800000000", Password: "pw"})

	sessions, err := c.ListSessions(context.Background(), "tok")
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 3 {
		t.Fatalf("sessions = %d, want 3", len(sessions))
	}
	if sessions[0].ID != "new" || sessions[1].ID != "mid" || sessions[2].ID != "old" {
		t.Fatalf("order wrong: %v", sessions)
	}
	if !sessions[1].Pinned {
		t.Fatal("pinned flag lost in parse")
	}
}

// TestListSessionsPaginatesCursor: 5 sessions, page size 2 → three pages,
// the cursor carrying the previous page's oldest (pinned, updated_at);
// all 5 collected in newest-first order.
func TestListSessionsPaginatesCursor(t *testing.T) {
	var ids []string
	entries := make([]SessionInfo, 5)
	for i := 0; i < 5; i++ {
		id := "s" + strconv.Itoa(5-i) // s5 newest
		entries[i] = SessionInfo{ID: id, Pinned: false, UpdatedAt: float64(500 - i)}
		ids = append(ids, id)
	}
	d := &sessionDrawer{entries: entries}
	var hits []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits = append(hits, r.URL.RawQuery)
		d.handle(w, r)
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, Mobile: "13800000000", Password: "pw"})

	sessions, err := c.ListSessions(context.Background(), "tok")
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 5 || sessions[0].ID != "s5" || sessions[4].ID != "s1" {
		t.Fatalf("sessions = %v", sessions)
	}
	if len(hits) != 3 {
		t.Fatalf("fetch_page hits = %d, want 3", len(hits))
	}
	if hits[0] != "" {
		t.Fatalf("first page query = %q, want parameterless", hits[0])
	}
	if !strings.Contains(hits[1], "lte_cursor.pinned=false&lte_cursor.updated_at=") &&
		!strings.Contains(hits[1], "lte_cursor.updated_at=4") {
		t.Fatalf("cursor page query = %q, want lte_cursor with page-1 oldest entry", hits[1])
	}
}

// TestListSessionsBadBizData: malformed biz_data surfaces an error, not a
// silent empty list.
func TestListSessionsBadBizData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":"not an object"}}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, Mobile: "13800000000", Password: "pw"})

	if _, err := c.ListSessions(context.Background(), "tok"); err == nil {
		t.Fatal("want error on malformed biz_data, got nil")
	}
}

// TestListSessionsBizError: a nonzero biz_code (e.g. muted) surfaces as
// BizError and stops the walk.
func TestListSessionsBizError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"code":0,"msg":"","data":{"biz_code":5,"biz_msg":"user is muted"}}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, Mobile: "13800000000", Password: "pw"})

	_, err := c.ListSessions(context.Background(), "tok")
	be, ok := err.(*BizError)
	if !ok {
		t.Fatalf("error = %T, want *BizError", err)
	}
	if be.BizCode != 5 {
		t.Fatalf("BizCode = %d, want 5", be.BizCode)
	}
}

// TestListSessionsPageBounds: a drawer that always claims has_more stops
// at maxSessionPages — the walk is bounded even against a lying upstream.
func TestListSessionsPageBounds(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Write([]byte(`{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":` +
			`{"chat_sessions":[{"id":"s","pinned":false,"updated_at":1}],"has_more":true}}}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, Mobile: "13800000000", Password: "pw"})

	sessions, err := c.ListSessions(context.Background(), "tok")
	if err != nil {
		t.Fatal(err)
	}
	if hits != maxSessionPages {
		t.Fatalf("fetch_page hits = %d, want %d (bounded walk)", hits, maxSessionPages)
	}
	if len(sessions) != maxSessionPages {
		t.Fatalf("sessions = %d, want %d", len(sessions), maxSessionPages)
	}
}
