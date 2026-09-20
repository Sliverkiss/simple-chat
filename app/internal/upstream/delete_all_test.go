// Tests for DeleteAllSessions: the APK's "clear all chat history"
// (profile → delete_all_chat_button, r02.java case 1). The wire shape is
// confirmed from the decompiled call site: POST /api/v0/chat_session/
// delete_all with NO request serializer attached (create's case 0 calls
// rl2.n0 to set the JSON body; delete_all's case 1 never does) — a
// bodyless authenticated POST answered by the standard envelope.
package upstream

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestDeleteAllSessionsWireShape: a bodyless authenticated POST to
// /api/v0/chat_session/delete_all, envelope-checked like every other call.
func TestDeleteAllSessionsWireShape(t *testing.T) {
	var gotMethod, gotPath string
	var gotAuth string
	var gotBodyLen int64
	var gotContentType bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotBodyLen = r.ContentLength
		gotContentType = r.Header.Get("Content-Type") != ""
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"code":0,"msg":"","data":{"biz_code":0,"biz_msg":"","biz_data":null}}`)
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, Mobile: "13800000000", Password: "pw"})

	if err := c.DeleteAllSessions(context.Background(), "tok"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %s, want POST", gotMethod)
	}
	if gotPath != "/api/v0/chat_session/delete_all" {
		t.Fatalf("path = %s, want /api/v0/chat_session/delete_all", gotPath)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("Authorization = %q, want %q", gotAuth, "Bearer tok")
	}
	if gotBodyLen > 0 {
		t.Fatalf("request carried a body of %d bytes, want none (r02.java case 1 attaches no serializer)", gotBodyLen)
	}
	if gotContentType {
		t.Fatal("request carried a Content-Type, want none (no JSON serializer attached)")
	}
}

// TestDeleteAllSessionsBizError: a nonzero biz_code surfaces as BizError.
func TestDeleteAllSessionsBizError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"code":0,"msg":"","data":{"biz_code":5,"biz_msg":"user is muted","biz_data":null}}`)
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, Mobile: "13800000000", Password: "pw"})

	err := c.DeleteAllSessions(context.Background(), "tok")
	if err == nil {
		t.Fatal("biz 5 (muted) accepted as success")
	}
	if be, ok := err.(*BizError); !ok || be.BizCode != 5 {
		t.Fatalf("err = %v, want BizError with biz_code 5", err)
	}
}
