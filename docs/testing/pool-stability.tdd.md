# 账号池稳定性 TDD 证据

规格：[`../pool-stability-spec.md`](../pool-stability-spec.md)。本轮仅本地 HTTP mock，没有调用真实账号或 Upstash。账户身份由 mock 登录 token 反推，完整 token/密码未进入提交；测试内号码为固定虚构 fixture。

- RED `9e9161b`：`go test ./internal/server -run '^TestPlainChat' -count=1 -v` 编译且运行后失败：parallel、HTTP 500、transport cut 的第二次 completion 重用了同一物理 identity；单账号 parallel_limit 也被立即重试。流式首 delta 后不重试的对照用例通过。
- 规格校正 `62e6b1b`：原 RED 把所有单账号重试都判为错误，与既有可重试 transport/5xx/空结果的有限兜底冲突。改为仅 parallel_limit 不自重试，其余允许一次、但必须建新 session；改后重跑 `TestPlainChat` 仍因真实缺口 RED。
- GREEN `fd981a2`：请求级记录前次物理 identity 与故障类别；第二次优先 `AcquireWithWaitExcluding`。无替代账号时，仅安全故障保留一次同账号兜底；parallel_limit 原样映射。修正 SSE 回调：压制的 THINK delta 不再提前 commit，已发出可见 delta 后绝不重试。`go test ./internal/server -run '^TestPlainChat' -count=3` 与 `go test ./... -count=1 -timeout=300s` 通过。
- 监听范围纠正：曾以 `9496a53` / `47557f0` 做全局 loopback RED/GREEN；用户随后明确本地限制**仅针对测试**，部署不需要。已撤回默认监听与 Compose 绑定更改，并删除 `listen_address_test.go`；这些提交保留为可追溯历史，不作为最终验收。测试启动显式传 `127.0.0.1`，部署保留配置自由。
- 总门禁（上述改动合并后）：`go test ./... -count=1 -timeout=300s`、`go test -race ./... -count=1 -timeout=300s`、`go vet ./...`、`go build ./...`、`git diff --check` 均通过。`go test ./internal/upstream ./internal/server -cover -count=1 -timeout=300s` 分别为 85.5%、88.5% statement coverage。

- 重复身份与结束边界 RED `43bd2db`：`go test ./internal/upstream -run '^TestPoolDuplicateIdentityShares' -count=1` 失败于第二个同身份 lease 超容量、ban/mute/risk 只停用一个槽；`go test ./internal/server -run '^TestPoolBoundary' -count=1` 失败于裸 EOF 伪造正常 stop/部分 200 与内容过滤日志终态错误。
- GREEN `fbded79` / `1049b52`：重复身份共享容量、AccountManager 和停用状态；热加加入同身份共享态，删除后重建隔离旧 lease 的迟到 park；SSE 裸 EOF 不再报正常 stop，非流式不返回截断部分 200，content_filter 终态正确。原有 `event: close` 完成约定保留。`go test ./... -count=1 -timeout=300s`、`go test -race ./... -count=1 -timeout=300s`、`go vet ./...`、`go build ./...`、`git diff --check` 复跑通过。此前“不同槽独立容量”的旧测试已改为物理身份语义。

已覆盖：AC-P01/P02/P06 既有池和持久态测试，以及重复身份共享容量/停用与热删重建；AC-P03/P04 同一 prompt、独立 session、身份切换与安全兜底 mock；AC-P05 已可见 SSE 片段后不切换、裸 EOF 不报正常结束；AC-P07 内容过滤诊断。监听范围按用户纠正仅约束测试，不约束部署。限制：未做真实服务请求；AC-P07 的所有诊断细分仍需单独验收。测试不证明两个健康账号严格轮询或固定频率；评分调度本来不承诺这些性质。
