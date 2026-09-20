package openai

import "testing"

// parseSearchSwitch (web-search-research.md, Phase 2 chat switch):
//
//	"search": {"type": "enabled"}  → true
//	"search": {"type": "disabled"} → false
//	absent or null                  → false (search default OFF — it slows
//	                                       responses; wrong default for bulk
//	                                       text processing)
//	malformed                       → 400-worthy error
func TestParseSearchSwitch(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		want    bool
		wantErr bool
	}{
		{"absent", `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`, false, false},
		{"null", `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"search":null}`, false, false},
		{"enabled", `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"search":{"type":"enabled"}}`, true, false},
		{"disabled", `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"search":{"type":"disabled"}}`, false, false},
		{"bad type value", `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"search":{"type":"maybe"}}`, false, true},
		{"non-object", `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"search":"enabled"}`, false, true},
		{"missing type", `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"search":{}}`, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := ParseRequest(tc.body)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got nil (search=%v)", req.SearchEnabled)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if req.SearchEnabled != tc.want {
				t.Errorf("search = %v, want %v", req.SearchEnabled, tc.want)
			}
		})
	}
}
