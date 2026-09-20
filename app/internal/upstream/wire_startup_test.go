package upstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestUsersCurrentWireShape: GET /api/v0/users/current with bearer token and
// app headers; success envelope decodes; nonzero biz surfaces as BizError.
func TestUsersCurrentWireShape(t *testing.T) {
	var gotMethod, gotAuth, gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v0/users/current" {
			t.Errorf("path = %s, want /api/v0/users/current", r.URL.Path)
		}
		gotMethod = r.Method
		gotAuth = r.Header.Get("Authorization")
		gotUA = r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"id":1}}}`))
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, Mobile: "13800000000", Password: "pw"})
	if err := c.UsersCurrent(context.Background(), "tok123"); err != nil {
		t.Fatalf("UsersCurrent: %v", err)
	}
	if gotMethod != http.MethodGet {
		t.Errorf("method = %s, want GET", gotMethod)
	}
	if gotAuth != "Bearer tok123" {
		t.Errorf("Authorization = %q, want Bearer tok123", gotAuth)
	}
	if gotUA == "" {
		t.Error("User-Agent missing")
	}
}

// TestUsersCurrentBizError: a biz-failure envelope surfaces as BizError with
// the biz code intact.
func TestUsersCurrentBizError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"code":0,"msg":"","data":{"biz_code":5,"biz_msg":"user is muted"}}`))
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, Mobile: "13800000000", Password: "pw"})
	err := c.UsersCurrent(context.Background(), "tok123")
	if err == nil {
		t.Fatal("want BizError, got nil")
	}
	be, ok := err.(*BizError)
	if !ok {
		t.Fatalf("error type = %T, want *BizError", err)
	}
	if be.BizCode != 5 {
		t.Errorf("BizCode = %d, want 5", be.BizCode)
	}
}

// TestFetchSessionPageWireShape: GET /api/v0/chat_session/fetch_page with no
// query parameters (parameterless first page, od1:820-838) and bearer token.
func TestFetchSessionPageWireShape(t *testing.T) {
	var gotMethod, gotAuth, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v0/chat_session/fetch_page" {
			t.Errorf("path = %s, want /api/v0/chat_session/fetch_page", r.URL.Path)
		}
		gotMethod = r.Method
		gotAuth = r.Header.Get("Authorization")
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":{"sessions":[],"has_more":false}}}`))
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, Mobile: "13800000000", Password: "pw"})
	if err := c.FetchSessionPage(context.Background(), "tok123"); err != nil {
		t.Fatalf("FetchSessionPage: %v", err)
	}
	if gotMethod != http.MethodGet {
		t.Errorf("method = %s, want GET", gotMethod)
	}
	if gotAuth != "Bearer tok123" {
		t.Errorf("Authorization = %q, want Bearer tok123", gotAuth)
	}
	if gotQuery != "" {
		t.Errorf("RawQuery = %q, want empty (parameterless first page)", gotQuery)
	}
}
