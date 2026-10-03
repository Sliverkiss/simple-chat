# 账号池稳定性 TDD 证据

规格：[`../pool-stability-spec.md`](../pool-stability-spec.md)。本轮仅本地 HTTP mock，没有调用真实账号或 Upstash。账户身份由 mock 登录 token 反推，完整 token/密码未进入提交；测试内号码为固定虚构 fixture。

- RED `9e9161b`：`go test ./internal/server -run '^TestPlainChat' -count=1 -v` 编译且运行后失败：parallel、HTTP 500、transport cut 的第二次 completion 重用了同一物理 identity；单账号 parallel_limit 也被立即重试。流式首 delta 后不重试的对照用例通过。
- 规格校正 `62e6b1b`：原 RED 把所有单账号重试都判为错误，与既有可重试 transport/5xx/空结果的有限兜底冲突。改为仅 parallel_limit 不自重试，其余允许一次、但必须建新 session；改后重跑 `TestPlainChat` 仍因真实缺口 RED。
- GREEN `fd981a2`：请求级记录前次物理 identity 与故障类别；第二次优先 `AcquireWithWaitExcluding`。无替代账号时，仅安全故障保留一次同账号兜底；parallel_limit 原样映射。修正 SSE 回调：压制的 THINK delta 不再提前 commit，已发出可见 delta 后绝不重试。`go test ./internal/server -run '^TestPlainChat' -count=3` 与 `go test ./... -count=1 -timeout=300s` 通过。
- 监听范围纠正：曾以 `9496a53` / `47557f0` 做全局 loopback RED/GREEN；用户随后明确本地限制**仅针对测试**，部署不需要。已撤回默认监听与 Compose 绑定更改，并删除 `listen_address_test.go`；这些提交保留为可追溯历史，不作为最终验收。测试启动显式传 `127.0.0.1`，部署保留配置自由。
- 总门禁（上述改动合并后）：`go test ./... -count=1 -timeout=300s`、`go test -race ./... -count=1 -timeout=300s`、`go vet ./...`、`go build ./...`、`git diff --check` 均通过。`go test ./internal/upstream ./internal/server -cover -count=1 -timeout=300s` 分别为 85.5%、88.5% statement coverage。

已覆盖：AC-P01/P02/P06 既有池和持久态测试；AC-P03/P04 新增同一 prompt、独立 session、真实身份切换/安全兜底 mock；AC-P05 首客户端 SSE 片段后不切换、未输出 THINK 的提交边界。监听范围按用户纠正仅约束测试，不约束部署。限制：未做真实服务请求；重复账号槽位的容量和停用在本段记录时仍以槽位为单位，后续 RED/GREEN 另行记录。完整可测的业务终止原因/usage 缺失场景虽有日志基础，但 AC-P07 的所有诊断细分尚未单独验收。测试不证明两个健康账号严格轮询或固定频率；评分调度本来不承诺这些性质。
