# 账号池切换与稳定性规格（SDD）

状态：执行中。主分支从工具调用功能回退到 `d80a01a`，再独立恢复账号池评分调度 `041cba1`。旧工具调用历史保留于 `backup/pre-tool-rollback-20261003-2020`；未提交无关改动保留于 stash，不将工具代码恢复到当前分支。

## 目标与边界

仅在 127.0.0.1 本地提供 OpenAI 形状的普通聊天反代；每次请求独立调度已授权账号，不依赖会话粘性。容量、in-flight、EWMA 延迟、账号状态共同决定候选。首字节前可按安全错误分类有限重试；首字节后不换号不重放。上游 429／parallel limit、mute、risk、ban 分别按现有状态处理，不能靠反复换号硬顶。客户端普通消息在每次尝试使用相同 prompt；每次尝试创建独立上游 session，不跨账号复用 session/file ID。当前不支持工具调用，也不设计伪装、风控规避或公网服务。

## 可观测验收矩阵

- AC-P01 调度：健康且有空位候选按评分近优选取；分数考虑 in-flight 占比和 EWMA；禁用/封禁/冷却账户绝不入选；两个相近健康账户无需固定轮流或真均匀随机。用 fake clock/确定性 PRNG 或强制唯一候选做状态断言，不以四次抽样必须覆盖全部账户作断言。
- AC-P02 容量：并发 Acquire/Release 不超出每账号 MaxInflight；排队按 QueueWait 截止；忙碌与全禁用区分错误和 HTTP 映射；热增删不使现有 lease 失效。包级并发与 race 测试。
- AC-P03 首字节前切换：A 的 retryable transport/HTTP 5xx/明确 parallel limit 首字节前失败，第二次优先排除 A 的**物理身份**（含重复池槽）；B 有容量则使用 B 的独立 token/session，同一普通 prompt 只送一次给每个尝试，客户端只得一次响应。B 忙碌时服从排队时限，不偷换回 A。
- AC-P04 单账号回退：没有 B 时，仅对原有明确安全可重试的 transport/5xx/空结果允许同一身份一次重试；parallel limit/429、封禁、risk/mute 不在同账号上立即重试。无可用候选返回明确错误，不无限等待。
- AC-P05 提交边界：任何客户端可见正文、推理或 SSE 事件出现后，后续上游错误只显式终止原流，不换账号/重复内容。非流式在完整上游响应分类前不提交。
- AC-P06 状态持久化：ban/mute/risk 与自然恢复在账户存储和池快照一致；重启后已停用账号不做登录或上游请求，过期冷却重新可选。场景只用本地 mock JSON/Redis fixture，不操作真实账号。
- AC-P07 日志：脱敏 identity、尝试次数、排队时间、终止原因、输入输出/总 tokens 和可测速度；无 usage 标 unknown，不打印密码/token/完整 prompt。日志只声称实际已观察终态。
- AC-P08 配置：监听地址默认/部署配置均限定 loopback；`app/.env` 忽略并保留，任何提交不得包含真实账号或 Redis 凭据。

## TDD 顺序

1. 表征既有 AC-P01/P02/P06，复跑 `go test ./internal/upstream ./internal/server -count=1`；已有通过项作为基线，不制造假 RED。
2. 为 AC-P03/P04 写新 HTTP mock：实际记录 account identity、session id、prompt、completion count；必须先看到 RED（现存 `runAttempt` 每次 AcquireWithWait 可能重选 A）。单账号回退与错误状态分别断言，不依赖调度顺序。
3. 最小 GREEN：`runAttempt` 携带上一身份并用 `AcquireWithWaitExcluding`；只在当前安全重试政策允许时选择单账号兜底，不增加尝试上限，不触碰图像/工具主链。并行状态按提交边界处理；不能拿空输出直接伪造成成功。
4. 新增 AC-P05 的提交前/后 SSE mock，先 RED（若已有测试覆盖则记录 GREEN 基线），最小修复。
5. 最终门禁：`go test ./... -count=1 -timeout=300s`、`go test -race ./... -count=1 -timeout=300s`、`go vet ./...`、`go build ./...`、`git diff --check`；逐项审查提交内容，只提交账号池主题。不得把 mock 通过表述为真实账号通过。

## 风险与未决

预工具基线的 retry ladder 原本允许单账号部分错误重新尝试；账户排除必须不回归安全兜底。评分调度不是严格轮询，旧测试关于“固定第一账号”和“四次必覆盖”的断言必须改成可确定的状态/唯一候选测试。账号池 `pool.go` 中重复身份多槽仍存在，AC-P03 必须按物理身份排除而非槽位。现有工作树还包含 README/admin/main 等与本主题无关的用户改动：保留、不混入池提交。真实 Upstash/上游账号测试已停止；今后只有用户另行要求才恢复。
