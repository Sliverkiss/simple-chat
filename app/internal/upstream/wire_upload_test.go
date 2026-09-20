package upstream

// Wire alignment — file upload (apk-alignment.md I4).
//
// The app's upload (qy1) sends X-DS-PoW-Response, X-Thinking-Enabled "1"/"0",
// x-file-size <long>, and optional x-model-type on POST /api/v0/file/
// upload_file. Ours sent only the PoW header.

import (
	"context"
	"net/http"
	"testing"
)

func TestUploadHeadersMatchApp(t *testing.T) {
	m := newMock(map[string]func(w http.ResponseWriter, r *http.Request){
		"/api/v0/chat/create_pow_challenge": func(w http.ResponseWriter, r *http.Request) {
			ch := solvableChallenge("/api/v0/file/upload_file")
			ch["expire_after"] = 600000
			writeEnvelope(w, 0, "", map[string]any{"challenge": ch})
		},
		"/api/v0/file/upload_file": func(w http.ResponseWriter, r *http.Request) {
			assertAppHeaders(t, r)
			if v := r.Header.Get("X-Thinking-Enabled"); v != "0" {
				t.Errorf("X-Thinking-Enabled = %q, want \"0\"", v)
			}
			if v := r.Header.Get("x-file-size"); v != "7" { // len("pngdata")
				t.Errorf("x-file-size = %q, want \"7\"", v)
			}
			if v := r.Header.Get("x-model-type"); v != "" {
				t.Errorf("x-model-type = %q, want absent", v)
			}
			writeEnvelope(w, 0, "", map[string]any{"id": "file-1", "status": "SUCCESS"})
		},
		"/api/v0/file/fetch_files": func(w http.ResponseWriter, r *http.Request) {
			writeEnvelope(w, 0, "", map[string]any{"files": []map[string]any{{"id": "file-1", "status": "SUCCESS"}}})
		},
	})
	defer m.srv.Close()

	id, err := m.client().UploadImageAndWait(context.Background(), "tok1", []byte("pngdata"), "image.png")
	if err != nil {
		t.Fatal(err)
	}
	if id != "file-1" {
		t.Errorf("file id = %q", id)
	}
}
