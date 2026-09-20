package openai

import (
	"time"

	"simple-chat/internal/sse"
)

// usage mirrors the OpenAI usage object.
type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// choice is a non-stream completion choice.
type choice struct {
	Index   int `json:"index"`
	Message struct {
		Role             string `json:"role"`
		Content          string `json:"content"`
		ReasoningContent string `json:"reasoning_content,omitempty"`
		// Citations is the structured web-search results collected from the
		// upstream SEARCH fragment when the request opted into search
		// ("search": {"type": "enabled"}). Non-standard, documented in
		// README; [citation:N] markers in content key into CiteIndex.
		Citations []sse.SearchResult `json:"citations,omitempty"`
	} `json:"message"`
	FinishReason string `json:"finish_reason"`
}

// CompletionResponse is the non-stream /v1/chat/completions response.
type CompletionResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []choice `json:"choices"`
	Usage   usage    `json:"usage"`
}

// NewCompletionResponse builds a complete non-stream response. reasoning is
// the model's deep-thinking text; empty reasoning omits the field. citations
// (web-search hits) attach when the request ran with search enabled.
func NewCompletionResponse(id, model, reasoning, content string, totalTokens int, finishReason string, citations []sse.SearchResult) *CompletionResponse {
	resp := &CompletionResponse{
		ID:      id,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: make([]choice, 1),
		Usage:   usage{TotalTokens: totalTokens, PromptTokens: totalTokens},
	}
	resp.Choices[0].Message.Role = "assistant"
	resp.Choices[0].Message.Content = content
	resp.Choices[0].Message.ReasoningContent = reasoning
	resp.Choices[0].Message.Citations = citations
	resp.Choices[0].FinishReason = finishReason
	return resp
}

// streamChoice is one streaming choice.
type streamChoice struct {
	Index int `json:"index"`
	Delta struct {
		Role             string `json:"role,omitempty"`
		Content          string `json:"content,omitempty"`
		ReasoningContent string `json:"reasoning_content,omitempty"`
	} `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

// StreamChunk is one SSE chunk of a streaming response.
type StreamChunk struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []streamChoice `json:"choices"`
	Usage   *usage         `json:"usage,omitempty"`
	// Citations on the final chunk only: web-search hits collected from the
	// upstream SEARCH fragment (search-enabled requests). Non-standard,
	// documented in README.
	Citations []sse.SearchResult `json:"citations,omitempty"`
}

// NewStreamChunk builds a content chunk. Pass role="assistant" for the first
// chunk, "" afterwards.
func NewStreamChunk(id, model, content, role string) *StreamChunk {
	chunk := &StreamChunk{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: make([]streamChoice, 1),
	}
	chunk.Choices[0].Delta.Role = role
	chunk.Choices[0].Delta.Content = content
	return chunk
}

// NewReasoningStreamChunk builds a reasoning (deep-thinking) chunk — the
// THINK deltas that precede content deltas. role="assistant" on the first
// chunk of the completion, "" afterwards.
func NewReasoningStreamChunk(id, model, reasoning, role string) *StreamChunk {
	chunk := &StreamChunk{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: make([]streamChoice, 1),
	}
	chunk.Choices[0].Delta.Role = role
	chunk.Choices[0].Delta.ReasoningContent = reasoning
	return chunk
}

// NewFinalChunk builds the terminating chunk with finish_reason and usage.
// citations (web-search hits) attach when the request ran search-enabled.
func NewFinalChunk(id, model string, totalTokens int, finishReason string, citations []sse.SearchResult) *StreamChunk {
	chunk := &StreamChunk{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: make([]streamChoice, 1),
	}
	chunk.Choices[0].FinishReason = &finishReason
	u := usage{TotalTokens: totalTokens, PromptTokens: totalTokens}
	chunk.Usage = &u
	chunk.Citations = citations
	return chunk
}

// WebSearchResponse is the POST /v1/web_search response: the queries the
// upstream model generated for the fan-out and the structured hits it
// collected. Non-OpenAI-standard; documented in README.
type WebSearchResponse struct {
	Object  string             `json:"object"` // "web_search"
	Queries []string           `json:"queries"`
	Results []sse.SearchResult `json:"results"`
}

// NewWebSearchResponse builds the standalone search response. Nil slices
// become empty arrays so clients see a stable shape.
func NewWebSearchResponse(queries []string, results []sse.SearchResult) *WebSearchResponse {
	if queries == nil {
		queries = []string{}
	}
	if results == nil {
		results = []sse.SearchResult{}
	}
	return &WebSearchResponse{Object: "web_search", Queries: queries, Results: results}
}

// model is one /v1/models entry.
type model struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// modelsList is the /v1/models response.
type modelsList struct {
	Object string  `json:"object"`
	Data   []model `json:"data"`
}

// ModelsResponse returns the single-model list.
func ModelsResponse() *modelsList {
	return &modelsList{
		Object: "list",
		Data: []model{{
			ID:      ModelName,
			Object:  "model",
			Created: 1735689600, // static; the model list never changes
			OwnedBy: "simple-chat",
		}},
	}
}

// errorBody is the OpenAI error envelope.
type errorBody struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code"`
	} `json:"error"`
}

// ErrorBody builds an OpenAI-shaped error object.
func ErrorBody(message, typ, code string) *errorBody {
	e := &errorBody{}
	e.Error.Message = message
	e.Error.Type = typ
	e.Error.Code = code
	return e
}
