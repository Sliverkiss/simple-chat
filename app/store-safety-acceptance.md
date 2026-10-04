# Account store safety — acceptance matrix

| Scenario | Expected | Evidence / boundary |
|---|---|---|
| Two separately booted instances: A parks, B logs in from old cache | B updates token only; mute survives | `TestMemoryFirstCrossInstanceStaleWrites` (RED then GREEN) |
| A deletes account, B parks old cached identity | No Redis SET/SADD by B; no resurrection | Same test (RED on old code, GREEN) |
| A deletes and re-adds identity, B parks old incarnation | New epoch rejects old park | `TestMemoryFirstCrossInstanceReaddDoesNotAcceptOldPark` (RED then GREEN) |
| Park fields changed by another instance since callback's snapshot | Conditional patch rejects stale park; no unpark/ban overwrite | Lua compares previous park fields atomically; fake RESP emulates it |
| Expired park cleanup during boot | Conditional patch; Redis command error aborts boot | `RedisStore.Load` path |
| JSON and fake backing | Existing full-record write-through contract unchanged | `go test ./...` |
| Redis fails during live park write, then process restarts | **Not guaranteed**: if Redis never recorded the mute, no durable signal exists to reconstruct it after crash | Requires durable outbox / upstream authoritative reconciliation, not a fail-closed boot check based only on stale Redis |

Deployment search found no explicit single-instance restriction. The in-memory pool and generations are process-local; this fix fences Redis callback writes only. Concurrent admin full-record upserts and two-step Redis `SET+SADD` / `SREM+DEL` remain outside this narrow change; multiple active replicas must not be treated as generally supported. No live Redis/provider was accessed: RESP fake checks command behavior but does not execute Lua in a real Redis runtime. Lua atomicity relies on Redis EVAL semantics. No mute duration was changed.
