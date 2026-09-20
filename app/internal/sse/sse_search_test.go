package sse

import (
	"os"
	"strings"
	"testing"
)

func readFileIfExists(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Search-mode regressions (web-search-research.md risk #4):
//
//  1. a `response` BATCH may carry a fragments APPEND (first RESPONSE chunk in
//     search mode) — applyBatch must route it through fragment handling, not
//     silently drop it;
//  2. `response/fragments/-1/results` carries the structured search results
//     array — it must be captured into state.SearchResults, not ignored;
//  3. the SEARCH fragment's `queries` from the initial payload must be
//     captured into state.SearchQueries;
//  4. the `response/fragments/-1` BATCH (search-fragment status + summary
//     content "搜索到 20 个网页") is noise for text accumulation — the summary
//     must NOT leak into the answer text.
func TestBatchNestedFragmentsAppendIsText(t *testing.T) {
	stream := "data: {\"v\":{\"response\":{\"fragments\":[{\"type\":\"SEARCH\",\"status\":\"WIP\",\"content\":null,\"queries\":[{\"query\":\"q1\"}],\"results\":[]}]}}}\n" +
		"data: {\"p\":\"response/fragments/-1/results\",\"v\":[{\"url\":\"https://example.com\",\"title\":\"Example\",\"snippet\":\"s\",\"cite_index\":1}]}\n" +
		"data: {\"p\":\"response/fragments/-1\",\"o\":\"BATCH\",\"v\":[{\"p\":\"status\",\"v\":\"FINISHED\"},{\"p\":\"content\",\"v\":\"搜索到 20 个网页\"}]}\n" +
		"data: {\"p\":\"response\",\"o\":\"BATCH\",\"v\":[{\"p\":\"fragments\",\"o\":\"APPEND\",\"v\":[{\"id\":3,\"type\":\"RESPONSE\",\"content\":\"今天是\",\"references\":[],\"stage_id\":2}]},{\"p\":\"has_pending_fragment\",\"o\":\"SET\",\"v\":false}]}\n" +
		"data: {\"v\":\"9月20日\"}\n" +
		"data: {\"p\":\"response\",\"o\":\"BATCH\",\"v\":[{\"p\":\"accumulated_token_usage\",\"v\":350},{\"p\":\"quasi_status\",\"v\":\"FINISHED\"}]}\n" +
		"event: close\n"
	st := collect(t, stream)
	if st.Text != "今天是9月20日" {
		t.Errorf("text = %q, want %q (first BATCH-nested fragment chunk must not be dropped)", st.Text, "今天是9月20日")
	}
	if !st.Finished {
		t.Error("stream must be finished after event: close")
	}
	if st.Usage.TotalTokens != 350 {
		t.Errorf("usage = %d, want 350", st.Usage.TotalTokens)
	}
	if got := len(st.SearchResults); got != 1 {
		t.Fatalf("search results = %d, want 1", got)
	}
	if st.SearchResults[0].URL != "https://example.com" || st.SearchResults[0].Title != "Example" || st.SearchResults[0].CiteIndex != 1 {
		t.Errorf("search result = %+v", st.SearchResults[0])
	}
	if len(st.SearchQueries) != 1 || st.SearchQueries[0] != "q1" {
		t.Errorf("search queries = %v, want [q1]", st.SearchQueries)
	}
}

// The full live search capture replays end-to-end: text assembled, results
// captured, search summary not leaking into text.
func TestReplayLiveSearchSample(t *testing.T) {
	input, err := readFileIfExists("../../recon-data/search_sse.txt")
	if err != nil {
		t.Skip("live capture not available")
	}
	st := collect(t, input)
	if len(st.SearchResults) != 20 {
		t.Errorf("search results = %d, want 20", len(st.SearchResults))
	}
	if st.SearchResults[0].URL != "https://ai.cnmo.com/news/818731.html" {
		t.Errorf("first result url = %q", st.SearchResults[0].URL)
	}
	if len(st.SearchQueries) != 5 {
		t.Errorf("search queries = %d, want 5", len(st.SearchQueries))
	}
	if !st.Finished {
		t.Error("stream must be finished")
	}
	if st.Text == "" || !containsCitationMarker(st.Text) {
		t.Error("text must be assembled and carry [citation:N] markers")
	}
	if !contains(st.Text, "今天是") {
		t.Error("first BATCH-nested chunk '今天是' must be present in text")
	}
	if contains(st.Text, "搜索到 20 个网页") {
		t.Error("search-fragment summary must not leak into answer text")
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

func containsCitationMarker(s string) bool { return strings.Contains(s, "[citation:") }
