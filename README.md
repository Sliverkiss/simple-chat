<div align="center">

<img src="IMG_8921.png" width="160" alt="simple-chat logo"/>

# simple-chat

**A tiny self-hosted chat gateway that speaks the OpenAI API protocol.**

One binary. One model. No bells and whistles.

[![Telegram](https://img.shields.io/badge/Telegram-sliverkiss__blog-2AABEE?style=flat-square&logo=telegram)](https://t.me/sliverkiss_blog)
[![Go](https://img.shields.io/badge/Go-1.23-00ADD8?style=flat-square&logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-distroless-2496ED?style=flat-square&logo=docker)](Dockerfile)

*Send standard OpenAI-format chat requests, get standard OpenAI-format responses back.*

</div>

---

## ✨ Features

- 🗣️ **`POST /v1/chat/completions`** — stream & non-stream, OpenAI protocol
- 🧠 **Deep thinking by default** — reasoning exposed as `reasoning_content`, opt-out per request
- 🌐 **Web search** — opt-in per request (`"search": {"type": "enabled"}`) with structured citations, plus a standalone `POST /v1/web_search` endpoint
- 🖼️ **Image understanding** — `image_url` content parts (http(s) URLs & base64 data URLs)
- 📋 **`GET /v1/models`** — exactly one model: `deepseek-flash`
- 🔄 **Multi-account rotation** — round-robin pool with in-flight caps & health states
- 🛡️ **Hardened failure paths** — bounded retry ladder, stream idle watchdog, honest error termination
- 🔑 **Optional API key auth** — `DS_API_KEY` (unset = open access)
- 🧹 **Human-paced session cleanup** — a background pass occasionally tidies a random handful of the oldest sessions per account (jittered 1h ±50% wake, batch 1–3, floor 5), like a real user cleaning their chat list
- 🛠️ **Admin account API** — upload / list / remove accounts over HTTP (`/admin/accounts`), no restart needed; the belmo.io deployment model: container + Redis URL + push accounts
- 📦 **Single static binary** — distroless Docker image, zero external Go dependencies

## 🚀 Quick start

```bash
cp .env.example .env          # optional API key
# create accounts.json:
#   {"accounts":[{"mobile":"...","email":"","password":"..."}]}
chmod 600 accounts.json
docker compose up -d          # serves :9879 -> 8080
```

Point any OpenAI client at `http://host:9879/v1` with model `deepseek-flash`:

```bash
curl http://localhost:9879/v1/chat/completions \
  -H "Authorization: Bearer $DS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-flash","messages":[{"role":"user","content":"hi"}]}'
```

## ⚙️ Configuration

| Env | Default | Purpose |
|---|---|---|
| `DS_ADDR` | `:8080` | listen address |
| `DS_ACCOUNTS` | `./accounts.json` | account file (`0600`) |
| `DS_API_KEY` | unset | require `Authorization: Bearer` / `X-Api-Key` when set |
| `DS_MAX_INFLIGHT` | `2` | per-account concurrent request cap |
| `DS_MAX_PROMPT_CHARS` | `2000000` | flattened-prompt cap → clean 400 `context_length_exceeded` (`0` = default, negative = off) |
| `DS_REDIS_HOST` | unset | Redis account store as sole source of truth (e.g. Upstash): portal URL (`https://mydb.upstash.io`), bare host, or a full `rediss://` connection string (used as-is); unset = `accounts.json` (default). When set, the runtime is memory-first — one boot load, then write-through only (admin mutations, park transitions, fresh login tokens); `accounts.json` is never read or written. An empty store is a valid boot state — seed via the admin API or `simple-chat -import-redis accounts.json` |
| `DS_REDIS_TOKEN` | unset | Upstash token for `DS_REDIS_HOST` (only needed for the URL/bare-host forms; ignored when the host is already a full `rediss://` string) |
| `DS_CLEANUP_INTERVAL` | `1h` | base wake interval of the human-paced session cleanup, jittered ±50% per sleep; duration (`90m`) or plain seconds (`3600`); `0` disables |
| `DS_CLEANUP_FLOOR` | `5` | at/below this many upstream sessions a cleanup episode deletes nothing |
| `DS_SESSION_CAP` | unset | hard safety ceiling: `N` = oldest sessions beyond N evicted synchronously on create (emergency valve; the human-paced policy is the normal cleanup path) |
| `DS_PURGE` | `1` | master switch for the weekly purge-all-sessions: `0` disables it entirely |
| `DS_PURGE_WEEKDAY` | `6` | weekly purge day (`0` = Monday … `6` = Sunday); `-1` disables |
| `DS_PURGE_HOUR` | `4` | weekly purge hour (0-23, local time) |

**Mute/ban cooldown is persistent.** When the upstream mutes (biz 5) or
bans (biz 10) an account, the gateway parks it and writes the state back
into `accounts.json` (optional `park_kind` / `park_until` / `park_reason` /
`parked_at` fields). Restarts honor the window: a muted account stays out of
rotation — zero upstream traffic, because every completion attempt against
a muted account *renews* the mute window upstream — and rejoins automatically
once `mute_until` passes. **Banned accounts stay parked forever**; to
manually revive one, delete its `park_*` fields from `accounts.json` and
restart the container. Parked accounts are visible in the logs
(`pool: <mobile> → MUTED until <t> (persisted)`, and
`pool: <mobile> still MUTED until <t> (restored from disk)` on boot).

**Deep thinking** is on by default; opt out per request with `"thinking": {"type": "disabled"}`:

```jsonc
{"model": "deepseek-flash",
 "thinking": {"type": "disabled"},   // omit or "enabled" for thinking (default)
 "messages": [{"role": "user", "content": "hi"}]}
```

Reasoning arrives as `reasoning_content` — streamed as `delta.reasoning_content`
chunks before the `delta.content` chunks, and as `message.reasoning_content` on
non-stream responses. Thinking is best-effort: if the upstream model skips
thinking, you simply get an answer with no reasoning. Malformed switch values
(e.g. `{"type": "banana"}`) are rejected with `400`.

**Web search** is off by default (it slows responses); opt in per request with
`"search": {"type": "enabled"}`:

```jsonc
{"model": "deepseek-flash",
 "thinking": {"type": "disabled"},  // thinking + search composes into a slower tool pipeline — usually disable one
 "search":   {"type": "enabled"},   // default: off
 "messages": [{"role": "user", "content": "today's tech news"}]}
```

When search runs, the answer text carries `[citation:N]` markers, and the
structured hits ride a **non-standard `citations` field** — `message.citations`
on non-stream responses, `citations` on the final stream chunk. Each citation
is `{url, title, snippet, site_name, cite_index, ...}`; `N` in `[citation:N]`
keys into `cite_index`.

Honest caveat: in search mode the upstream injects a stronger system prompt
server-side (tool-call adherence). This gateway never injects anything itself —
the opt-in switch documents what the upstream does with the mode.

**Standalone search** — `POST /v1/web_search` returns the structured results
directly (no chat framing):

```bash
curl http://localhost:9879/v1/web_search \
  -H "Content-Type: application/json" \
  -d '{"query": "latest tech news"}'
```

```jsonc
{"object": "web_search",
 "queries": ["latest tech news October 2026"],   // the model's fan-out queries
 "results": [{"url": "...", "title": "...", "snippet": "...", "cite_index": 1}]}
```

One search = one upstream completion (thinking forced off to stay on the fast
search path). Empty results are an honest empty list. Malformed bodies answer
`400`.

## 🛠️ Admin API (account management)

Manage the account pool over HTTP after deployment — container filesystems
are ephemeral on managed hosts; with `DS_REDIS_HOST` set, the service is
turnkey: deploy the container, point it at Redis, push accounts via API.

All three endpoints sit behind the same optional `DS_API_KEY` as `/v1`
(open access when unset). Responses are **unredacted** (full mobile,
password, device_id) — owner-only surface. Every mutation is store-first:
the record is persisted (JSON file or Redis, whichever is active) before
the running pool changes; a store failure answers `500` with the pool
untouched. Added accounts serve on the very next request — no restart —
with the same first-login startup sequence as file-loaded ones. Removed
accounts stop receiving new requests; in-flight ones finish.

### Upload — `POST /admin/accounts`

One object or a batch (also accepted: `"channel": "android"`):

```bash
curl http://localhost:9879/admin/accounts \
  -H "Authorization: Bearer $DS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"accounts":[
        {"mobile":"15200000001","password":"..."},
        {"email":"acct@example.com","password":"...","channel":"web","device_id":"B-harvested-shumei-id"}
      ]}'
```

Fields: `mobile`/`email` (exactly one), `password` (required), `region`
(`"cn"` or omitted), `channel` (`""`/`"android"`/`"web"`; `web` requires
an explicit `device_id` — a browser-harvested Shumei SMSdk id), `device_id`
(verbatim if set, else minted deterministically). Duplicates — same
mobile/email in the store or within the batch — answer `409`/`400`; the
`409` body carries the existing record. On success the full stored records
are echoed back:

```jsonc
{"accounts": [
  {"mobile": "15200000001", "email": "", "password": "...", "region": "cn",
   "device_id": "yAl20M...", "channel": "", "identity": "15200000001"}]}
```

### List — `GET /admin/accounts`

Full records plus live pool state and a summary:

```bash
curl http://localhost:9879/admin/accounts -H "Authorization: Bearer $DS_API_KEY"
```

```jsonc
{"accounts": [
  {"mobile": "15200000001", "password": "...", "region": "cn", "device_id": "...",
   "channel": "", "state": "ready", "inflight": 0, "max_inflight": 2, "token_warm": true},
  {"mobile": "15200000002", "password": "...", "region": "cn", "device_id": "...",
   "channel": "", "state": "muted", "park_kind": "muted", "park_until": "2026-09-21T09:00:00Z",
   "park_reason": "account muted: ...", "inflight": 0, "max_inflight": 2, "token_warm": false}],
 "summary": {"ready": 1, "parked": 1, "total": 2}}
```

`state` is `ready`/`banned`/`muted`/`risk`; parked accounts additionally
carry `park_kind`/`park_until`/`park_reason`; `token_warm` means the
account holds a cached token (has served traffic).

### Remove — `DELETE /admin/accounts/{id}`

`id` is the mobile (or email, for email-only accounts):

```bash
curl -X DELETE http://localhost:9879/admin/accounts/15200000001 \
  -H "Authorization: Bearer $DS_API_KEY"
```

Responds with the removed record (full echo). The account is deleted from
the store and retired from rotation — in-flight requests finish, no new
ones are routed to it, and no upstream logout happens. Unknown id → `404`.
On restart (or with Redis), whatever remains in the store is what loads.

## ☁️ Deploy to belmo (or any Coolify-based PaaS)

belmo runs [Nixpacks](https://nixpacks.com) builds, and its deployment engine
always scans the **`app/` subdirectory** of the cloned repo — that's why the
Go sources live in `app/` (repo root keeps `Dockerfile`, `docker-compose.yml`
and this README for local use).

**Setup (one time):**

1. Import the GitHub repo into belmo, build pack: **Nixpacks**.
2. **Base Directory: `app`** — critical. Leave it empty and the Go project is
   never detected (`nixpacks plan` gets pointed at a nonexistent `/app` path
   and reports `providers: []`).
3. **Build / Start commands: leave empty** — `app/nixpacks.toml` already
   pins them (`go build -o app .` / `./app`).
4. **Port: `8080`** (or set `DS_ADDR=:3000` and use port `3000` — note the
   leading colon: `DS_ADDR` is a full listen address, not a bare port).
5. **Environment variables** — the belmo deployment model is
   *stateless container + Upstash Redis + admin API*:

   | Env | Value |
   |---|---|
   | `DS_API_KEY` | your gateway key (e.g. `REDACTED_KEY`) |
   | `DS_REDIS_HOST` | Upstash portal URL or host, e.g. `https://mydb.upstash.io` |
   | `DS_REDIS_TOKEN` | Upstash token |

   With `DS_REDIS_HOST` set, the container never touches local files (the
   Nixpacks image filesystem is read-only anyway): it loads all accounts from
   Upstash once at boot and runs memory-first. `accounts.json` is only the
   fallback when no Redis is configured.

**Seeding accounts (optional):** an empty Upstash store boots fine — push
accounts over the admin API (see below). Or seed it from a local
`accounts.json` in one shot:

```bash
DS_REDIS_HOST=https://mydb.upstash.io DS_REDIS_TOKEN=xxx \
  ./simple-chat -import-redis accounts.json
```

**Why not the platform's own Redis?** On belmo the managed Redis endpoints
were not reachable from the app over the Redis protocol during testing;
Upstash (free tier) works over TLS on port 6379 and gives you a shared
account store across *all* your deployments — local, cloud, anywhere.

## 🏗️ Build

```bash
go build -o simple-chat .
go test ./...
```

## 📐 Design

`spec.md` documents the full design and upstream contract; `gap-analysis.md` records
what we absorbed from a survey of 14+ similar projects — and what we deliberately rejected.

> Statelessness is the design: full history passed on every call, a fresh upstream
> session per request. The client owns the conversation. Sessions accumulate
> app-like; a background pass occasionally tidies the oldest handful.

**Session lifecycle.** Every completion creates one upstream session that then
persists, exactly like the mobile app (which never auto-deletes a chat). A
background cleanup goroutine wakes on a jittered interval (`DS_CLEANUP_INTERVAL`,
default 1h ±50%) and, per active account, occasionally (50% per wake) lists the
account's real upstream sessions via `fetch_page` — the same drawer-open GET the
app fires — and deletes a random 1–3 of the oldest, non-pinned ones, but only
when the account holds more than `DS_CLEANUP_FLOOR` (default 5). The result is a
human pattern: burst-then-idle tidying at unlearnable times, never a machine
cadence, never a purge. Because truth is read from the upstream drawer, sessions
orphaned by restarts are healed by the next episode too. `DS_SESSION_CAP`
(optional) remains as a hard synchronous ceiling if you ever need one; `0`
disables cleanup entirely.

**Weekly purge.** Complementing the tidy episodes, one background goroutine
mimics the app's "clear all chat history" setting (profile →
delete-all-chats): once a week (`DS_PURGE_WEEKDAY`/`DS_PURGE_HOUR`, default
Sunday 04:00 ±30m jitter) it fires the app's `chat_session/delete_all` for
every active (healthy, warm-token) account, logs the before/after drawer
counts, and resets the in-memory session registry. A purge never cold-logins
an account — parked/muted/cold accounts simply miss that week. A window
missed while the process was down (within the last 24h) catches up once
shortly after startup, with its own jitter; a failed purge is retried next
week, never in a storm. `DS_PURGE=0` or `DS_PURGE_WEEKDAY=-1` disables it.

## 📄 License

MIT — see [LICENSE](LICENSE).
