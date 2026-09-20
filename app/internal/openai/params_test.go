package openai

// TASK_UPSTREAM_PARAMS — sampling-parameter parsing.
//
// max_completion_tokens is OpenAI's current name for the max_tokens knob;
// modern SDKs send it instead of the deprecated max_tokens. The gateway maps
// it onto max_tokens (the only name the upstream pass-through knows), with
// max_completion_tokens winning when both are present (OpenAI semantics:
// max_tokens is the deprecated alias).

import "testing"

func TestParseRequestMaxCompletionTokens(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{
			name: "max_completion_tokens alone maps onto max_tokens",
			body: `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":256}`,
			want: 256,
		},
		{
			name: "max_tokens alone still works",
			body: `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"max_tokens":128}`,
			want: 128,
		},
		{
			name: "both present: max_completion_tokens wins",
			body: `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"max_tokens":128,"max_completion_tokens":256}`,
			want: 256,
		},
		{
			name: "zero max_completion_tokens falls back to max_tokens",
			body: `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],"max_tokens":128,"max_completion_tokens":0}`,
			want: 128,
		},
		{
			name: "neither present stays zero",
			body: `{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}`,
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := ParseRequest(tt.body)
			if err != nil {
				t.Fatalf("ParseRequest: %v", err)
			}
			if req.MaxTokens != tt.want {
				t.Errorf("MaxTokens = %d, want %d", req.MaxTokens, tt.want)
			}
		})
	}
}

func TestParseRequestSamplingParamsSurvive(t *testing.T) {
	req, err := ParseRequest(`{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}],
		"temperature":0.7,"top_p":0.9,"max_completion_tokens":512,
		"presence_penalty":0.5,"frequency_penalty":0.5,"stop":["\n"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if req.Temperature != 0.7 {
		t.Errorf("Temperature = %v, want 0.7", req.Temperature)
	}
	if req.TopP != 0.9 {
		t.Errorf("TopP = %v, want 0.9", req.TopP)
	}
	if req.MaxTokens != 512 {
		t.Errorf("MaxTokens = %d, want 512", req.MaxTokens)
	}
}
