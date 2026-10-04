# 账号池切换与稳定性规格（SDD）

状态：账号切换、重复身份聚合和迟到错误边界、流式 EOF/内容过滤诊断均已在本地 mock GREEN；独立末轮审查中，未做真实账号验证。主分支从工具调用功能回退到 `d80a01a`，再独立恢复账号池评分调度 `041cba1`。旧工具调用历史保留于 `backup/pre-tool-rollback-20261003-2020`；未提交无关改动保留于 stash，不将工具代码恢复到当前分支。

## 目标与边界

提供 OpenAI 形状的普通聊天反代；本地测试只绑定 127.0.0.1，部署监听地址保持可配置。每次请求独立调度已授权账号，不依赖会话粘性。容量、in-flight、EWMA 延迟、账号状态共同决定候选。首字节前可按安全错误分类有限重试；首字节后不换号不重放。上游 429／parallel limit、mute、risk、ban 分别按现有状态处理，不能靠反复换号硬顶。客户端普通消息在每次尝试使用相同 prompt；每次尝试创建独立上游 session，不跨账号复用 session/file ID。当前不支持工具调用，也不设计伪装或风控规避。

## 单实例账号池运行合同（本轮 SDD，优先于零散缺陷清单）

**目标**：一个应用进程连接一个 Redis，账号池能持续给聊天与 Web 搜索分配可用账号；已停用账号不被再次分配，停用期限及恢复在重启前后保持一致。不是保证每个账号都能正常完成上游请求，也不是严格轮询。以下为用户明确规则与待验证的技术合同，不把现有代码/单测通过当作验收结果。

**边界**：不设计多应用实例同步、不改 Redis 数据结构、不接真实账号或生产 Redis（除非用户另行授权）。当所有候选停用、容量耗尽或持久化失败时，不伪造成功；要有明确且可观测的响应及健康状态。仅在单实例范围讨论 crash 后未持久化事件：若 Redis 未收到写入而进程已退出，新进程无法恢复这次事件；此项标为**未保证**，不得以普通测试通过宣称解决。

| 合同 | 初始条件与动作 | 必须观察到／禁止的副作用 | 本轮验证证据 |
|---|---|---|---|
| OP-01 启动与候选 | Redis 中有 ready A、期限内 muted B、永久 banned C；启动并发起聊天和搜索 | 仅 A 可以获取新 lease；B/C 不登录、不建会话、不发 completion；重复物理身份不能绕过停用 | 从 backing 加载到真实 HTTP handler 的本地假 store/httptest，请求计数及池快照 |
| OP-02 健康号服务 | ready A/B 均有容量；A 正在服务或遭遇可安全重试的首字节前错误 | 请求有界等待或按现有评分/容量选择；允许安全切到另一物理身份，客户端仅得一次结果；首字节后不重放；不承诺固定轮流 | httptest 记录每身份 lease/session/completion、输出次数、容量与排队时间 |
| OP-03 mute 移出轮转 | A 首次收到 biz5，带有效未来 `mute_until=T`，或无有效截止时间；其余账号仍健康 | A 立刻不再取得**新**聊天/搜索 lease；写 Redis 的本地截止分别为 `T+1h` 或首次观测起 7 天；B 继续服务；在途 lease 不可撤销，但不能引出新的 A lease | HTTP 200/非 200 三入口与并发 barrier，回读 backing 和后续请求身份 |
| OP-04 重启与恢复 | A 已持久 muted，进程重建；随后时间跨过本地截止 | 截止前不对 A 发上游请求；截止后自然进入候选并清除持久停用标记；永久 ban 永不随时间复活；迟到较短错误不缩短期限 | 重建 server/store/pool 的本地持久 fixture、受控时间边界及候选/持久状态断言 |
| OP-05 持久化故障 | A 的停放 backing 写失败，另有看似 ready 账号 | 当前进程关闭新租约，聊天/搜索与健康检查返回 503；热重建不能解除锁存；不得把旧 Redis 行当成功落盘 | 注入失败 backing 后调用真实 handler/healthz；记录重启后无法保证的边界 |
| OP-06 空池与分类 | 全部停用、全部忙碌、永久 ban、限流、transport 故障分别触发 | 有限等待与对应错误，不无限换号或对 mute/ban 立即重试；日志/响应不泄漏凭据或身份 | handler 黑盒响应码、Retry-After 与上游请求计数 |

**执行顺序**：先做 OP-01/03/04 的纵向闭环测试；若现有行为已满足，记录 GREEN 基线，不制造假 RED。若不满足，先见到该合同对应的 RED 再最小修复为 GREEN；再覆盖 OP-02/05/06。每一轮独立重跑完整测试、race、vet、build，逐项填证据与未覆盖项。离线闭环通过仅证明本地协议/状态机，不代替生产 Redis 与真实上游验证。

## 可观测验收矩阵

- AC-P01 调度：健康且有空位候选按评分近优选取；分数考虑 in-flight 占比和 EWMA；禁用/封禁/冷却账户绝不入选；两个相近健康账户无需固定轮流或真均匀随机。用 fake clock/确定性 PRNG 或强制唯一候选做状态断言，不以四次抽样必须覆盖全部账户作断言。
- AC-P02 容量：并发 Acquire/Release 不超出每账号 MaxInflight；排队按 QueueWait 截止；忙碌与全禁用区分错误和 HTTP 映射；热增删不使现有 lease 失效。包级并发与 race 测试。
- AC-P03 首字节前切换：A 的 retryable transport/HTTP 5xx/明确 parallel limit 首字节前失败，第二次优先排除 A 的**物理身份**（含重复池槽）；B 有容量则使用 B 的独立 token/session，同一普通 prompt 只送一次给每个尝试，客户端只得一次响应。B 忙碌时服从排队时限，不偷换回 A。
- AC-P04 单账号回退：没有 B 时，仅对原有明确安全可重试的 transport/5xx/空结果允许同一身份一次重试；parallel limit/429、封禁、risk/mute 不在同账号上立即重试。无可用候选返回明确错误，不无限等待。
- AC-P05 提交边界：任何客户端可见正文、推理或 SSE 事件出现后，后续上游错误只显式终止原流，不换账号/重复内容。非流式在完整上游响应分类前不提交。
- AC-P06 状态持久化：ban/mute/risk 与自然恢复在账户存储和池快照一致；重启后已停用账号不做登录或上游请求，过期冷却重新可选。场景只用本地 mock JSON/Redis fixture，不操作真实账号。
- AC-P07 日志：脱敏 identity、尝试次数、排队时间、终止原因、输入输出/总 tokens 和可测速度；无 usage 标 unknown，不打印密码/token/完整 prompt。日志只声称实际已观察终态。
监听策略：测试启动时显式使用 `127.0.0.1`，不运行真实账号请求；部署环境保留可配置监听地址，不把本地测试约束写成生产限制。`app/.env` 忽略并保留，任何提交不得包含真实账号或 Redis 凭据。

## Mute 切换的独立规格（本地验收）

已知 mute 指账户存储中 `park_kind=muted` 且 `park_until` 在未来，或一次请求已得到 biz_code=5 并由 `NoteError` 停用的身份；首次探测 mute 指此前为 ready、直到本次上游返回 biz_code=5 才知其不可用。不能将“先前 ready、连续首次探测变 mute”的多个身份误认为已标记 mute 被调度。

| 场景 | 同一请求 | 后续请求／重启 | 验收证据 |
|---|---|---|---|
| 启动已知 mute，另有 ready | 只选 ready，不登录 mute | 截止前始终不选 mute | mock 记录每身份全部请求为零 |
| 首次 completion/login/session 返回 biz 5 | 本次报 mute，不把 biz 5 当可重试故障换 B | 已标记身份从候选中排除；配置 store 时写入并重启恢复 | mock 计数、池快照、store 字段 |
| A、B 原先都 ready，先后各自首次命中 mute | 每个请求可分别失败一次 | A 在标记后不再被选；B 也在标记后不再被选 | 按身份区分的请求计数，非固定轮询 |
| 同身份并发 lease，A 先返回 mute | 已发出的 lease 不可撤销；待它完成后不再分配新 lease | 无新的登录/session/completion 发往停用身份；重复槽共享停用态 | barrier 测试并发在途与新 Acquire |
| 过期 mute | 不在过期前试探 | 到期自然恢复并清除持久 park | 受控期限与存储回读 |

排查前提：本地直接 `NewPool` 若未配置 `OnParkPersist`，不能据此断言生产 Redis 未写；生产通过 `main.go → server.NewServer(ParkStore)` 配置写入回调。所有验证仅使用固定虚构凭据、httptest 和本地 store，不触碰真实上游/Redis。若现有行为满足上述矩阵，只补表征测试和证据，不制造假 RED 或改风控参数。

## 非 200 响应分类补充验收（本地 mock）

- AC-P08：completion 返回 HTTP 非 200 且有效 JSON envelope 明确 biz_code=5 和 mute_until 时，原请求返回 mute/Retry-After，不重放，池和配置 store 按上游期限停用该身份；普通非 JSON 5xx 仍是可重试 HTTP 错误，401/403 的既有鉴权刷新不回归。使用 httptest + 本地 store。
- AC-P09：postJSON（包括建 session）收到 HTTP 非 200 的 biz 5 保留 mute_until；HTTP 503 的 JSON 成功空 envelope（无明确业务拒绝）作为服务故障可重试，明确 biz_code 或 outer code 的拒绝不可重试。只用本地 HTTP mock，不接真实服务。
- AC-P10：建 session 失败发生在客户端输出前；安全 transport/HTTP 5xx 可以先在原有客户端预检重试一次，再按请求剩余次数切换另一物理身份；mute/auth/明确业务拒绝不可换号，不发送 completion，不超出请求尝试上限。Web search 和普通聊天均验证。

## biz5 停用期限策略（本地 SDD，覆盖以上“按上游期限”表述）

- 上游明确返回 biz_code=5 且 `mute_until` 可解析、在收到时仍为未来时，**本地** `park_until = mute_until + 1h`，无论登录、建会话、completion 的 HTTP 200／非 200 路径。上游时间戳保留原值供诊断；加成仅在写入池和持久化时做一次，不修改上游错误字段。HTTP 429 的 `Retry-After` 应指示本地恢复剩余时间（不是上游原截止）；缺失、解析失败或已过期的 `mute_until` 一律从收到时本地停用 **7 天**，不允许按旧 6h 过早复选。显式配置的测试冷却可短于生产默认值。
- 账号身份在本地截止前不允许参与聊天或 websearch 的任何候选，配置 store 记录加成后的 `park_until`，重启加载后不登录停用账号；到期自然恢复并可重新入池。永久 ban 无截止也不自动恢复。
- 同一错误可能先由 `AccountManager.markBan` 再由 `Lease.NoteError` 处理；两入口不得重复加成，同一截止重复上报也不得延长一小时。并发迟到的更短截止不得缩短已知期限；持久回调不能将较长期限写回为短期。
- 验收：httptest 的 HTTP 200／非 200 登录、会话、completion biz5；实际 SSE biz5 仅在当前解析器会把它分类成 biz5 时同样执行；解析秒数／字符串／RFC3339 和无效／已过期兜底；并发迟到、持久化重启跳过和到期复选。仅虚构身份、本地 store，不连真实 Redis／上游。

## 永久 ban 写入与迟到 risk 单调期限（独立验收）

- AC-P11：上游 biz 10 在登录／建会话／completion 经 `AccountManager.markBan` 预先置永久 ban 后，lease 的 `NoteError` 必须恰好一次写入 `OnParkPersist`；再次通知不得重复。用本地 mock 走真实 manager 调用和回调，重建池后 ban 仍不可选；迟到 mute/risk 不得降级或覆盖持久 ban。直接 `NoteError` ban 同样保持一次写入。
- AC-P12：同一身份并发在途错误乱序到达时，已有较晚 mute 不得被较短 risk 改写（manager 和 lease 两层均须保持原 kind、期限）；跨 kind 的已知 park 期限只能延长不能缩短，短消息不得发出覆盖持久化；永久 ban 永远优先。只用虚构账户与确定期限，不改变 mute 加 1h／无截止 7 天策略。

本轮 TDD 证据（`app/internal/upstream/pool_park_monotonic_test.go`）：RED 执行 `go test ./internal/upstream -run 'TestManagerBanIsPersistedExactlyOnceByLease|TestParkDeadlineMonotonicAcrossKinds' -count=1`，分别失败于 `ban persistence = []` 与两个跨 kind 的 manager 期限缩短；GREEN 同命令通过，增加 manager 预置 ban 后迟到错误测试通过。`go test ./... -count=1 -timeout=300s`、`go test -race ./... -count=1 -timeout=300s`、`go vet ./...`、`go build ./...`、`git diff --check` 均通过。测试只覆盖 mock，无真实账户／Redis 验证；ban 持久化依赖调用方将业务错误交给 lease `NoteError`。

## mute 原始时间跨入口过期及 429 提示（本地 SDD）

- AC-P13：同一 biz5 错误的 `mute_until=T` 在 manager 观察时有效，至 lease 处理时已过期，仍沿用首次计算的 `T+1h`，不能回退为 7 天；并发期间若已有更长 park，则保持更长期限与 kind，不追加较短持久记录。原始 `BizError.MuteUntil` 保持不变；真正首次收到无效时间仍按 7 天。
- AC-P14：429 `Retry-After` 是**请求重试的保守提示**，不是某个身份可用的承诺或身份信息。响应体维持现有 `account_muted` 业务码，不暴露账户标识。已持有 lease 的请求采用池最终本地 `parkUntil` 作为秒级向上取整提示，不能早于最终 park；没有 lease 的旧错误映射路径维持由原始时间推算的兼容兜底。多个候选可能提前可用，提示并不承诺届时必成功，也不改变 parallel-limit 429 的独立短冷却语义。
- 验收：本地双入口时间跨界及迟到 risk 的测试、保留较长 park 的 HTTP 错误映射测试；无真实上游、Redis 或账号请求。

## TDD 顺序

1. 表征既有 AC-P01/P02/P06，复跑 `go test ./internal/upstream ./internal/server -count=1`；已有通过项作为基线，不制造假 RED。
2. 为 AC-P03/P04 写新 HTTP mock：实际记录 account identity、session id、prompt、completion count；必须先看到 RED（现存 `runAttempt` 每次 AcquireWithWait 可能重选 A）。单账号回退与错误状态分别断言，不依赖调度顺序。
3. 最小 GREEN：`runAttempt` 携带上一身份并用 `AcquireWithWaitExcluding`；只在当前安全重试政策允许时选择单账号兜底，不增加尝试上限，不触碰图像/工具主链。并行状态按提交边界处理；不能拿空输出直接伪造成成功。
4. 新增 AC-P05 的提交前/后 SSE mock，先 RED（若已有测试覆盖则记录 GREEN 基线），最小修复。
5. 最终门禁：`go test ./... -count=1 -timeout=300s`、`go test -race ./... -count=1 -timeout=300s`、`go vet ./...`、`go build ./...`、`git diff --check`；逐项审查提交内容，只提交账号池主题。不得把 mock 通过表述为真实账号通过。

## 风险与未决

### 单应用进程、一个 Redis：停放写失败的安全边界

- AC-P13：`MemoryFirstStore.ApplyPark` 的 backing 返回错误时，本进程永久锁存新 lease 准入关闭；聊天／web search 新 Acquire 返回 503，`/healthz` 返回 503。不得靠热重建账号解除；已在途 lease 不能撤销。持久化成功不锁存。此故障门仅接入 memory-first store，memory-only 池不改变策略。
- 停放先在内存发生再写 Redis；回包丢失亦属结果未知，不能声称期限已持久化。停新租约／不健康信号并非自动治愈；恢复前需人工核对持久状态。
- **无法保证的启动边界**：若写失败后崩溃或重启，内存停放与锁存均丢失；Redis 若仍保留旧 ready 行，下一进程无法凭空恢复失落 mute，可能重新选择。跨此边界保证需先完成可靠落盘／外部持久故障标记或在重新接流量前人工核对。本次不增加 schema、全局阈值或分布式锁；一个 Redis 实例不等于多个应用实例。
- 本地 TDD：`app/internal/server/park_failclosed_test.go` 虚构账号／失败 backing 的 RED 复现热重建后新租约；GREEN 验证同进程拒绝新 lease、聊天 503、health 503，backing 仍为 ready（因此不能推断重启安全）。不连真实 Redis／上游。

预工具基线的 retry ladder 原本允许单账号部分错误重新尝试；账户排除必须不回归安全兜底。评分调度不是严格轮询，旧测试关于“固定第一账号”和“四次必覆盖”的断言必须改成可确定的状态/唯一候选测试。账号池 `pool.go` 中重复身份多槽仍存在，AC-P03 必须按物理身份排除而非槽位。现有工作树还包含 README/admin/main 等与本主题无关的用户改动：保留、不混入池提交。真实 Upstash/上游账号测试已停止；今后只有用户另行要求才恢复。
