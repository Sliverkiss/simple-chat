# 账号池稳定性 TDD 证据

规格：[`../pool-stability-spec.md`](../pool-stability-spec.md)。本轮仅本地 HTTP mock，没有调用真实账号或 Upstash。账户身份由 mock 登录 token 反推，完整 token/密码未进入提交；测试内号码为固定虚构 fixture。

- RED `9e9161b`：`go test ./internal/server -run '^TestPlainChat' -count=1 -v` 编译且运行后失败：parallel、HTTP 500、transport cut 的第二次 completion 重用了同一物理 identity；单账号 parallel_limit 也被立即重试。流式首 delta 后不重试的对照用例通过。
- 规格校正 `62e6b1b`：原 RED 把所有单账号重试都判为错误，与既有可重试 transport/5xx/空结果的有限兜底冲突。改为仅 parallel_limit 不自重试，其余允许一次、但必须建新 session；改后重跑 `TestPlainChat` 仍因真实缺口 RED。
- GREEN `fd981a2`：请求级记录前次物理 identity 与故障类别；第二次优先 `AcquireWithWaitExcluding`。无替代账号时，仅安全故障保留一次同账号兜底；parallel_limit 原样映射。修正 SSE 回调：压制的 THINK delta 不再提前 commit，已发出可见 delta 后绝不重试。`go test ./internal/server -run '^TestPlainChat' -count=3` 与 `go test ./... -count=1 -timeout=300s` 通过。
- 监听 RED `9496a53`：`go test . -run '^TestDefaultListenAddressIsLoopback$' -count=1` 因 `defaultListenAddress` 缺失编译失败。GREEN `47557f0`：默认 `127.0.0.1:8080`，Compose 主机映射 `127.0.0.1:9879:8080`；保留原有明确传入 `DS_ADDR` 的配置能力。`docker compose config --format json` 显示 host_ip=`127.0.0.1`。
- 总门禁（上述改动合并后）：`go test ./... -count=1 -timeout=300s`、`go test -race ./... -count=1 -timeout=300s`、`go vet ./...`、`go build ./...`、`git diff --check` 均通过。`go test ./internal/upstream ./internal/server -cover -count=1 -timeout=300s` 分别为 85.5%、88.5% statement coverage。

已覆盖：AC-P01/P02/P06 既有池和持久态测试；AC-P03/P04 新增同一 prompt、独立 session、真实身份切换/安全兜底 mock；AC-P05 首客户端 SSE 片段后不切换、未输出 THINK 的提交边界；AC-P08 默认和 Compose loopback。限制：未做真实服务请求；显式 `DS_ADDR` 仍可由部署者指定非 loopback，属于现有配置能力，不能称作“任何配置都强制本地”；重复账号槽位的容量和停用仍以槽位为单位，尚未提升为物理身份聚合。完整可测的业务终止原因/usage 缺失场景虽有日志基础，但 AC-P07 的所有诊断细分尚未单独验收。测试不证明两个健康账号严格轮询或固定频率；评分调度本来不承诺这些性质。
