# OP-01/03/04 local vertical tracer (SDD → GREEN baseline)

Source: [pool-stability-spec.md](../pool-stability-spec.md), operational contract OP-01/03/04. The journey is: boot from persistent backing, serve chat and Web search only through ready identities, observe first biz5, write the local T+1h park deadline, reject new leases, reboot a new memory-first store/pool/server over the same backing, and resume only after expiry. Scope is one live server/store writer at a time; all upstream and backing records are fictional/local.

| Guarantee | Evidence |
|---|---|
| OP-01: ready A serves chat and search; initially muted B and permanently banned C receive no login/session/completion, with pool states checked | `TestOperationalMuteFromBackingThroughRestart` |
| OP-03: A first completion returns non-200 biz5; durable `park_until` equals parsed upstream T+1h; neither chat nor search gives A another completion; an extra stale ready slot of A's physical identity cannot bypass the park | `TestOperationalMuteFromBackingThroughRestart` |
| OP-04: new store/server loads the same backing, excludes A until its deadline and C permanently; B's expired park is cleared and serves both routes | `TestOperationalMuteFromBackingThroughRestart` |
| OP-04: post-deadline boot clears A's persisted park; A serves chat and search when made the unique eligible identity, independently of scored pool draws | `TestOperationalMuteFromBackingThroughRestart` |
| OP-04 live expiry: short persisted mute stays excluded until its deadline, then the same running pool lazily clears its backing park and serves both routes; C remains banned | `TestOperationalPersistedMuteNaturallyRecovers` |

Test-only fixture (`app/internal/server/operational_tracer_test.go`) uses `httptest` and an in-memory fake backing that implements the Store.Load expiry semantics of JSON/Redis stores. It does **not** connect to Redis or the production upstream; it does not use `/admin/accounts` or print account records. The distant T+1h recovery in the main tracer is simulated by advancing a fictional backing row after the old server is stopped; the separate short-deadline test exercises live lazy expiry. No production code change was necessary: the new tests passed on the baseline, so there is no invented RED checkpoint or fix commit.

Validation (all actually run in the independent worktree):

- `go test ./internal/server -run 'TestOperational' -count=3 -timeout=60s`: `ok simple-chat/internal/server 11.484s`
- `go test ./... -count=1 -timeout=300s`: all seven packages PASS (server 20.515s)
- `go test -race ./... -count=1 -timeout=300s`: all seven packages PASS on repeat (server 24.319s). One preceding full-race run failed in an unrelated existing `pool_diagnostics_boundary_test.go` bytes.Buffer logger read vs background startup log write, reported on `TestConcurrentRotationAndSessionCleanup`; this is not fixed by the OP-01/03/04 tracer and remains a known race-test instability.
- `go vet ./...`, `go build ./...`, `git diff --check`: exit 0

Not covered by this tracer: real Redis networking/credentials, real upstream, concurrency barrier for in-flight leases, login/session biz5 and missing/invalid mute_until (existing focused tests cover variants), OP-02/05/06. Read-only audit noted RESP commands lack read/write deadlines and park callbacks execute under pool locks, so a stalled backing may hang acquisition; all-parked accounts currently wait QueueWait and surface pool_busy. These are deferred OP-05/06 risks, not claimed fixed. Crash before a successful park write is not recoverable by this fixture or guaranteed by the contract.
