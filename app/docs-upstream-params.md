# Upstream Parameter Surface — What /api/v0/chat/completion Actually Accepts

Status: researched 2026-09-20 (TASK_UPSTREAM_PARAMS)
Question: which OpenAI `/v1/chat/completions` params can the gateway genuinely
forward upstream, and which must stay stripped?

## TL;DR

The official Android client's completion request has **exactly 10 fields —
none of them sampling parameters**. The upstream *tolerates* extra JSON keys
(proven live by our own shipped traffic and by ds2api at scale), but there is
**no evidence any sampling param is honored**. Our current pass-through of
`temperature` / `top_p` / `max_tokens` is a deliberate, documented,
live-proven divergence (apk-alignment.md K3) — kept. The one real gap found:
modern OpenAI clients send `max_completion_tokens`, which we silently dropped;
it is now mapped onto `max_tokens`. Live-probe budget used: **0 of 3**.

## Track 1 — APK evidence (primary)

`ChatFullCompletionRequest` (decompiled DeepSeek Android 2.5.3, jadx 1.5.6,
`/root/apk-analysis/decompiled/sources/defpackage/qj1.java` — kotlinx
serializer descriptor). The complete field list, straight from the descriptor:

| # | Field | Type | Optional | Observed values | We send |
|---|---|---|---|---|---|
| 0 | `chat_session_id` | String | no | session id | yes |
| 1 | `parent_message_id` | Integer (nullable) | no | null | yes (null) |
| 2 | `prompt` | String | no | flattened prompt | yes |
| 3 | `ref_file_ids` | List<String> | yes | `[]` / file ids | yes (`[]`) |
| 4 | `thinking_enabled` | boolean | yes | true/false | yes |
| 5 | `search_enabled` | boolean | yes | true/false | yes |
| 6 | `audio_id` | String (nullable) | yes | null (voice input path only) | never |
| 7 | `preempt` | boolean | no | false (non-preempting send) | yes (false) |
| 8 | `model_type` | String (nullable) | yes | null / "default" ("expert"/"vision" disabled server-side) | never |
| 9 | `action` | String (nullable, wrapped `CompletionAction`) | yes | unknown values (regenerate/edit actions) | never |

Construction sites (`di1.java:45`, `xh1.java:41`, `mi1.java:51`) confirm:
`audio_id` is always the literal `null` on the text path; `model_type` is fed
from the session's model-type state flow when there is no parent message;
`action` carries a nullable string from the caller. The app's Json config is
`encodeDefaults=true` + `explicitNulls=true` (apk-alignment.md I1/I3
revision), so all 10 keys serialize every time — nullable ones as literal
`null`.

**The app never sends `temperature`, `top_p`, `max_tokens`, `stop`, penalties,
or any other sampling knob.** There is no client-side control for them, and
the DTO has no field that could carry them. Conclusion: the "true" parameter
surface of the upstream, as exercised by the official client, is exactly the
10 fields above.

## Track 2 — open-source proxies (secondary)

| Project | Params forwarded upstream | Notes / evidence of effect |
|---|---|---|
| CJackHwang/ds2api (Go, 4.7k stars) | `temperature`, `top_p`, `max_tokens`, `max_completion_tokens`, `presence_penalty`, `frequency_penalty`, `stop` — blind pass-through into the completion body (`request_normalize.go:collectOpenAIChatPassThrough` → `standard_request.go:CompletionPayload`) | No claim anywhere that the upstream honors them; code comments treat it as "harmless to send". Heavy live traffic behind it — proves the server *accepts* the keys without error. |
| snake-aabb-wtf/deepseek-web2api-free | documents `temperature`/`top_p` as "传递给 DeepSeek 但效果取决于服务端" (passed through; effect depends on server); `max_tokens` explicitly **"被忽略（DeepSeek 不支持）"** (ignored — DeepSeek doesn't support it) | The only project that makes an effectiveness claim: max_tokens ignored. |
| Wu-jiyan/deepseek-reverse-api | forwards `temperature` (default null) | No effectiveness evidence. |

Consensus: everyone forwards, nobody demonstrates the upstream honors any of
it. One project explicitly says `max_tokens` is ignored.

## Our own live evidence

The gateway has forwarded `temperature`/`top_p`/`max_tokens` (when nonzero)
since the initial commit (client.go payload builder), on the same account
pool that has carried all prior live verification. No request has ever been
rejected or altered by the presence of these keys — consistent with
"tolerated, silently ignored" server behavior.

## Decision table

| OpenAI param | Decision | Evidence |
|---|---|---|
| `temperature` | **forward** (unchanged — already wired) | tolerated live (our traffic + ds2api); honoring unproven and unprovable with ≤3 probes |
| `top_p` | **forward** (unchanged) | same |
| `max_tokens` | **forward** (unchanged) | tolerated live; snake-aabb says ignored — still forwarded for client compatibility, zero cost |
| `max_completion_tokens` | **forward, mapped onto `max_tokens`** (NEW in this task) | OpenAI's current name for the same knob; modern SDKs send it; we dropped it silently until now. When both are present, `max_completion_tokens` wins (OpenAI semantics: `max_tokens` is deprecated). |
| `presence_penalty`, `frequency_penalty`, `stop` | **strip** (unchanged) | ds2api forwards them, but every added non-app field deepens the fingerprint divergence from the real client (the APK sends none of them) with no proven benefit. Our mimicry goal wins. |
| everything else (`logprobs`, `logit_bias`, `seed`, `n`, `response_format`, `stream_options`, `user`, `store`, `metadata`, `service_tier`, `prediction`, …) | **strip** (unchanged) | no evidence of any kind; OpenAI-only surface |
| `tools`, `tool_choice`, `functions`, `function_call`, `parallel_tool_calls` | **strip** (unchanged) | upstream has no tool calling — already settled (docs-spec-field-strip.md) |

## Live-probe budget: 0 of 3 used

A probe could only distinguish "honored" vs "ignored" for a sampling param
with statistical sampling of many completions — a single temperature-0 vs
default request on a short response cannot. Acceptance is already proven by
shipped traffic. Code-evidence-only was sufficient; burning account-budget
requests for unanswerable signal is against the probe rules.

## Follow-up list (app-sent fields we omit — NOT implemented)

Wire changes require user approval per task rules. Documented here only:

1. **`model_type`** — the app always serializes it (encodeDefaults), value
   `"default"` on the standard path (settings `model_configs` currently has
   expert/vision `enabled:false`). ds2api sends `model_type:"default"`
   explicitly. Adding `"model_type": "default"` to our payload would move us
   closer to the app's literal body shape. Low risk, but a wire change.
2. **`audio_id` / `action` as literal `null` keys** — with encodeDefaults the
   app emits `"audio_id": null` (and `"action": null` on the plain-send path)
   on every completion. We omit the keys entirely. Cosmetic body-shape
   alignment only; the server demonstrably accepts our omission (all live
   verification ran without them).
3. **`action` values** — unknown set of strings (tied to regenerate/edit
   flows, `CompletionAction` wrapper `dm2`/`fm2`). Would need targeted APK
   work to enumerate. No gateway use case today.

## Test coverage added

- `app/internal/openai/params_test.go` — `max_completion_tokens` parsing:
  alone, coexisting with `max_tokens` (alias wins), zero value falls back,
  absent stays zero; sampling params survive kitchen-sink sanitization.
- `app/internal/upstream/wire_completion_test.go` —
  `TestCompletionBodySamplingParams`: nonzero temperature/top_p/max_tokens
  land on the wire; zero values are omitted (pinning current behavior).
- `app/internal/server/params_forward_test.go` — end-to-end: OpenAI request
  with `temperature`/`top_p`/`max_completion_tokens` arrives upstream as
  `temperature`/`top_p`/`max_tokens` in the completion body.
