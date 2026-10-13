# ModelClaim 多副本（replicas > 1）支持 - 产品需求文档

## Overview
- **Summary**: 将 ModelClaim 从"只允许 1 个副本、手工创建多个 CR"扩展为单 CR 支持 `replicas > 1`：控制器基于 runtime snapshot 在**互不相同的 warm pod** 上调度多个 engine 实例，按安全顺序滚动扩缩、失败/丢 pod 自动重放，status.instances 记录每个实例的 pod/port/phase/ready/restartCount，网关只把 Ready 实例放入 routable 集合，删除时先全部停实例再移除 finalizer。
- **Purpose**: 让一个模型可以统一做多实例放置、滚动扩缩与跨 pod 健康收敛，无需用户手工维护多个 ModelClaim；同时保证扩缩/故障/删除过程中不中断已有可服务实例、网关永不路由到未就绪 engine。
- **Target Users**: 在 warm GPU pool 上通过 ModelClaim 部署 vLLM/SGLang engine 的平台工程师与集群管理员。

## Goals
- `spec.replicas` 接受任意 >= 1 的整数（CRD 校验放开），默认仍为 1。
- 调度器为同一 claim 的多个实例选择**互不相同**的 warm pod，决策使用 runtime snapshot：GPU HBM 余量（硬约束）、KV 使用（排序）、artifact 是否已缓存（优先）、pod 当前模型数与跨 claim 负载（排序）、确定性 tie-break。
- status.instances 每实例记录 `pod`、`port`、`phase`、`ready`、`restartCount`；status 与 warm pod annotation 始终一致。
- 扩缩顺序安全：需要替换时**先在新 pod 激活并等 Ready，再停止多余实例**；任何时候不得先停掉唯一健康实例。
- pod 消失（删除/替换）或 engine 终态失败/消失后，自动重放到可用 pod。
- 网关只路由 Ready（active 且 port > 0）实例；Activating/Sleeping/Failed 不进入 routable 集合；无 Ready 实例时 Sleeping 可触发 wake。
- DeletionTimestamp 下：确认全部实例已停止（或所在 pod 已消失）后才移除 finalizer。
- 测试覆盖：多副本放置、HBM/KV 约束、artifact 缓存优先、扩缩顺序、pod 漂移、失败重放、路由状态、删除。

## Non-Goals
- 不引入新的 autoscaler/HPA 联动（replicas 仍由用户静态设置；自动扩缩是后续工作）。
- 不改变 engine 启动参数协议、kv-limit pool policy、idle sleep 策略本身；多副本下这些既有机制继续逐实例工作。
- 不支持同一 warm pod 上同一 claim 的多个实例（副本即"不同 pod 上的实例"）。
- 不改变 ModelClaim 的 finalizer 名称、annotation key/JSON 既有字段与 routing state 字符串（兼容性表面）。
- 不做跨 namespace 调度、不做抢占（资源不足时 Pending 等待，不驱逐其它 claim 的 engine）。
- 不修改 Python runtime 的进程监督/重启预算逻辑（restart_count、phase=failed 已由 runtime 上报）。

## Background & Context
- 现状证据：
  - `api/model/v1alpha1/modelclaim_types.go` 中 `Replicas` 带 `+kubebuilder:validation:Maximum=1`；`ModelClaimInstance` 只有 Pod/Port/Phase。
  - 控制器 `pkg/controller/modelclaim/modelclaim_controller.go` 已有多实例雏形：`ensureActivated` 按 desired 循环激活、`scaleDown` 缩减、`pruneDeadInstances` 清理 pod 消失实例、`reconcileInstanceHealth` 按 snapshot 升降级并写 annotation。
  - 缺口：(a) API 禁止 >1；(b) Failed 实例永久占位（`recomputeReadiness` 直接置 claim Failed），没有"换新 pod 重放"；(c) scaleDown 只按 slice 尾部删，不区分健康状态、无"等新实例 Ready"门禁；(d) 删除路径 best-effort 调一次 deactivate 后立刻移除 finalizer，不确认是否真停；(e) 实例状态无 ready/restartCount 字段；(f) 调度只有排序没有 HBM 硬可行性过滤。
  - 调度输入已齐备：`RuntimeSnapshot`（Accelerators[].HBMFreeBytes、Models[].KVUsedBytes/KVCapacityBytes/HBMPeakBytes、CachedArtifacts）、snapshot TTL 缓存、`PodPlacementState`、跨 claim `computePodLoad`。
  - 网关侧：`pkg/cache` 仅把 port>0 的 binding 加入 model→pod 映射（天然只路由 Ready）；`ModelClaimBinding` 在无 active 时返回单个 binding 用于 sleep/wake 与重试响应；`targetAddress` 用 per-pod `ModelClaimPortForPod` 决定端口。多 active pod 时缓存已支持多后端，但 wake 回退目前不区分 binding 状态。
  - runtime（Python）已上报 `restart_count`、`phase`（active/sleeping/restarting/failed/...）、`alive`、`ready`、`last_error`。

## Functional Requirements

- **FR-1（API 放开与实例状态）**：`spec.replicas` 允许 >=1 的任意整数，默认 1；`status.instances[]` 在 pod/port/phase 之外新增 `ready:boolean` 与 `restartCount:integer`。CRD manifests、deepcopy、generated clients 同步重新生成并通过 `make verify`。

- **FR-2（多副本调度）**：当 desired > 当前实例数时，控制器在一次或多次 reconcile 内为每个缺口选择一个**尚未承载本 claim 任何实例**的 warm pod 激活；同一 claim 的所有实例 pod 两两不同。候选不足时保持 Pending（Scheduled=False/NoMatchingPods）并周期重试，不报错、不重复激活已有实例。

- **FR-3（snapshot 驱动的确定性调度规则）**：选 pod 按以下确定性全序，全部可在无 snapshot 时安全退化：
  1. artifact 已在该 pod 缓存（CachedArtifacts 命中 spec.artifactURL）优先；
  2. snapshot 已知优先于未知；
  3. **HBM 硬约束**：当可估出本 artifact 的单实例 HBM 需求（取所有候选 pod snapshot 中相同 artifactURL 模型的 `HBMPeakBytes`/`KVCapacityBytes` 最大值；均为 0 视为未知）时，`HBMFreeBytes < 需求` 的 pod 不可选（TP/PP>1 时取各 rank 的最小空闲）；需求未知时不过滤，保持现状行为；
  4. HBM 空闲多者优先；
  5. KV 已用字节少者优先；
  6. pod 上 runtime 模型数少者优先；
  7. 跨 claim 已分配实例数（computePodLoad）少者优先；
  8. locality cost 低者优先（当前 uniformLocality 恒等）；
  9. pod 名字典序小者优先。
  连续挑选时每选定一个 pod 即更新占用集合与负载，保证第二选择不会再选到同 pod。

- **FR-4（实例状态收敛与 annotation 一致）**：每次 reconcile 用各实例所在 pod 的**新鲜 runtime snapshot** 校正 phase/port/ready/restartCount：Ready 且 port>0 → Active 并写真实端口路由；未就绪 → Activating/保持 Sleeping；runtime 报 failed → Failed；snapshot 成功但该 claim engine 已消失 → Failed（触发重放）；snapshot 请求失败 → 保持上一个已知路由状态。每次 phase/port 变化都同步重写 pod annotation（port+state），使 status.instances 与 annotation 一致。

- **FR-5（扩缩顺序与安全门禁）**：
  - 扩容（含因失败/丢 pod 产生的替换）：先激活新实例，新实例保持 Activating/port0 不路由，**等其 Ready 后**才允许移除它所替换的旧实例；
  - 缩容：当实例数 > desired 时按"牺牲品顺序"删除：Failed → Sleeping → Activating → Active，同相位内按 pod 名确定性选择；
  - 健康门禁：只有删除后 Ready 实例数仍 >= desired 时才允许停止 Active 牺牲品；**任何情况下不得把当前唯一的健康（Active/Ready）实例先停掉**；非 Ready 牺牲品不承载流量，可在其替换实例已激活（Activating 及以上）后移除；
  - 纯用户缩容（如 3→1）允许停掉多余 Active 实例，但删除后 ready 数必须等于新的 desired（即按顺序逐个停到目标数）。

- **FR-6（失败重放）**：
  - pod 被删除/不再是候选：实例记录被剪除（现有 prune 语义），随后按 FR-2/FR-3 在可用 pod 上重新激活；
  - engine 在存活 pod 上终态失败或消失：若存在另一个可选的不同 warm pod，先在该 pod 激活替换实例并等 Ready，再 stop+摘除旧失败实例（deactivate + 去 annotation + 删 status 记录）；不存在不同 pod 时，清理失败记录后按普通激活路径重试（允许落回同 pod），由 requeue 节奏限速；
  - 重放不产生重复 engine（同一 pod 上不会因重试出现同一 claim 的两个 engine）。

- **FR-7（网关路由）**：网关 routable 集合只包含 state=active 且 port>0 的实例所在 pod；activating/sleeping/failed/port0 的 pod 不参与模型后端枚举，targetAddress 对 port0 binding 返回空（不回退默认端口）。同一模型有多个 Ready 实例时，ListPodsByModel 返回全部 active pod，路由器在其中选择并使用该 pod annotation 中的实例端口。无任何 Ready 实例时，确定性地优先返回 sleeping binding 以触发 wake，其次 activating，再次 failed。

- **FR-8（删除收尾）**：DeletionTimestamp 非空时，控制器对每个 status 实例：去路由 annotation、调用 deactivate(stop)，并用随后的 snapshot 确认该 claim 的 engine 在该 pod 上已消失（pod 已删除视为已确认）；存在 deactivate 错误或 snapshot 仍观察到 engine 时保留 finalizer 并 requeue；全部实例确认停止后才移除 finalizer。

- **FR-9（可观测与文档）**：既有 desired/ready gauges 继续正确反映多副本；更新 modelclaim 文档中 replicas 的限制说明与行为描述；samples 中给出多副本用法。

## Non-Functional Requirements
- **NFR-1（兼容性）**：不破坏既有单副本行为与全部现有单副本测试；annotation JSON、routing state 常量、CRD 既有字段保持兼容；新增 status 字段均为 optional。
- **NFR-2（可测试性）**：核心决策（可行性过滤、牺牲品顺序、替换门禁）为纯函数/可注入 runtime 的表驱动测试；不依赖真实 GPU/apiserver 的逻辑用 fake client + fake runtime 单测覆盖，端到端控制面用 envtest ginkgo 覆盖。
- **NFR-3（确定性与幂等）**：相同 snapshot/输入下放置与牺牲品选择结果确定；重复 reconcile 不重复激活、不抖动路由。
- **NFR-4（节流）**：无候选/未就绪/失败重放均由 10s requeue 驱动，不产生热循环；snapshot 继续走 TTL 缓存。

## Constraints
- **Technical**: Go 控制器（controller-runtime fake client 测试）、CRD 由 controller-gen 生成（`make manifests`/`make generate`/`make verify`）、runtime 协议字段已存在；测试运行命令 `go test ./pkg/controller/modelclaim/... ./pkg/cache/... ./pkg/types/...` 与 `make test-integration`（envtest）。
- **Business**: 遵循 AGENTS.md 的兼容性表面规则（API 字段、annotation、HTTP 端点语义不得破坏）。
- **Dependencies**: controller-gen 等代码生成工具通过 Makefile 按需安装；envtest 集成测试依赖本地 kubectl binaries（不可用时以单测+构建验证为准）。

## Assumptions
- `restart 状态`采用 runtime snapshot 已有的整数 `restart_count`，映射为 status 字段 `restartCount`（不新增 phase 枚举）。
- 单实例 HBM 需求无法从 spec 静态得知时，用集群内**同 artifactURL 已运行实例的观测峰值**（HBMPeakBytes，退路 KVCapacityBytes）作为估计；观测不到就不做硬过滤（与现状一致，宁可调度后由 runtime 返回 OAM 错误再重试）。
- "副本"语义为一个 claim 在不同 warm pod 上的 engine 实例；同一 pod 不放置两个同 claim 实例。
- snapshot 成功但 engine 条目消失等价于终态失败（进程没了），纳入失败重放；snapshot RPC 失败不做猜测（保持现状）。
- 删除时只需对 status.instances 中记录的实例做停止确认（binding 以 annotation/status 为准，孤儿 engine 由 runtime 自身监督与 claim UID 机制处理，不在本期扩展）。

## Acceptance Criteria

### AC-1: replicas > 1 的 API 与生成产物
- **Type**: `rule`
- **Given**: 仓库中设置 `spec.replicas: 3` 的 ModelClaim
- **When**: 应用到装有新 CRD 的 apiserver（envtest）并检查生成产物
- **Then**: CRD schema 中 replicas 只有 minimum=1（无 maximum=1）；status.instances 条目包含可选 `ready`、`restartCount` 字段；deepcopy/applyconfiguration/clientset 重新生成且 `make verify` 通过
- **Pass Condition**: CRD yaml 无 maximum；新字段存在且类型正确；`make verify`、`make vet` 退出码 0
- **Evidence**: config/crd/model/model.aibrix.ai_modelclaims.yaml 片段；`make verify` 输出；envtest 用例成功

### AC-2: 多副本放置到互不相同的 warm pod
- **Type**: `rule`
- **Given**: replicas=3 与 3 个 Running warm pod（同 selector）
- **When**: reconcile 收敛且 runtime 全部 Ready
- **Then**: 产生 3 次 activate，分别落在 3 个不同 pod；status.instances 有 3 条、pod 两两不同、port 非零、ready=true、phase=Active；desiredReplicas=3、readyReplicas=3；3 个 pod 各带 active+真实端口 annotation
- **Pass Condition**: 单测断言 activate 目标 pod 集合大小=3 且 status/annotation 一致
- **Evidence**: TestReconcileMultiReplicaPlacesOnDistinctPods

### AC-3: 候选不足时 Pending 并在 pod 出现后补齐
- **Type**: `rule`
- **Given**: replicas=3，仅 1 个 warm pod
- **When**: 连续 reconcile，之后再出现 2 个 pod
- **Then**: 先只激活 1 个、Scheduled=False/NoMatchingPods；新 pod 出现后自动补齐到 3，全程不重复激活
- **Pass Condition**: activate 调用总数最终=3；每个 pod 恰好 1 次
- **Evidence**: 控制器单测 + envtest ginkgo 用例

### AC-4: HBM 硬约束过滤
- **Type**: `rule`
- **Given**: 两个候选 pod，snapshot 显示同 artifact 在别处的 HBMPeakBytes=800；pod-A HBMFree=1000，pod-B HBMFree=500
- **When**: 为 claim 选择 pod
- **Then**: pod-B 被判定不可行，实例只可能落在 pod-A；HBM 需求未知（无同 artifact 观测）时两个 pod 都可行（不回归）
- **Pass Condition**: placement 纯函数/表驱动测试断言可行性集合
- **Evidence**: TestSelectPodsRejectsInsufficientHBM

### AC-5: KV 与模型数排序、artifact 缓存优先
- **Type**: `rule`
- **Given**: 多个通过 HBM 硬约束的候选，artifact 缓存状态、KVUsedBytes、ModelCount 不同
- **When**: 调度选 pod
- **Then**: artifact 已缓存者优先；同等条件下 KV 用得少、模型数少、名字小者依次胜出；规则确定且可重复
- **Pass Condition**: placement 表驱动测试覆盖 3 个排序维度各至少一例
- **Evidence**: placement_test.go 新增用例

### AC-6: 扩容先激活并等 Ready、缩容受健康门禁保护
- **Type**: `rule`
- **Given**: replicas 1→3 再 3→1 的场景，以及 [Active, Failed] desired=2 的替换场景
- **When**: 逐轮 reconcile，新实例处于 notReady/Ready 的不同时刻
- **Then**: 新实例 Ready 前不停任何旧实例；替换场景下替换实例 Ready 后才摘除 Failed；整个过程中 ready>=1 时没有任何 Active 实例在 ready 会跌破 desired 的情况下被停；唯一健康实例永不先被停
- **Pass Condition**: 单测按时间线断言 deactivate 调用时刻与实例集合；存在一条断言"仅剩 1 个 Active 时不允许停它"
- **Evidence**: TestScaleOutActivatesBeforeStop / TestScaleDownNeverStopsOnlyHealthy

### AC-7: pod 漂移自动重放
- **Type**: `rule`
- **Given**: 1 个 Active 实例在 warm-old 上
- **When**: 删除 warm-old，随后创建 warm-new
- **Then**: 旧实例记录消失、claim 不路由到旧 pod；warm-new 出现后自动激活并 Ready，activate 调用指向新 pod；status.instances 与新 pod annotation 一致
- **Pass Condition**: 单测+envtest 断言最终 pod=warm-new 且无旧 annotation
- **Evidence**: TestReconcileReplaysAfterPodDeletion（envtest 扩展现有用例为多副本）

### AC-8: engine 失败重放（先换 pod、Ready 后再清理）
- **Type**: `rule`
- **Given**: [Active(pod-a)] desired=1（另有空闲 pod-b），runtime 在 pod-a 上报 phase=failed
- **When**: 多轮 reconcile
- **Then**: 先在 pod-b 激活（pod-a 失败记录仍在、不路由）；pod-b Ready 前不对 pod-a 发 deactivate；Ready 后 stop pod-a 失败 engine、摘除记录与 annotation；最终单实例 Active 在 pod-b。无不同 pod 时清理失败记录后重试同 pod，不产生重复 engine
- **Pass Condition**: 时间线断言 activate/deactivate 顺序与最终实例集合
- **Evidence**: TestReconcileFailedInstanceReplaysToDifferentPod

### AC-9: status 与 annotation 一致且含 ready/restartCount
- **Type**: `rule`
- **Given**: runtime snapshot 对实例报告 ready=true、restart_count=2
- **When**: reconcile 后读取 claim status 与 pod annotation
- **Then**: 对应 instance.Ready=true、RestartCount=2、Phase=Active、Port=实际端口；annotation state=active、port 与 status 一致；未就绪时 ready=false、annotation port=0/state=activating
- **Pass Condition**: 单测对 ready/restartCount/annotation 三者一致性断言
- **Evidence**: TestReconcileInstanceStatusReflectsReadyAndRestarts

### AC-10: 网关只路由 Ready 的多副本实例
- **Type**: `rule`
- **Given**: 同模型 2 个 active(port>0) pod、1 个 activating(port0)、1 个 sleeping(port0)
- **When**: 查询缓存模型后端并计算目标地址
- **Then**: HasModel=true、ListPodsByModel 恰好返回 2 个 active pod；activating/sleeping pod 不在其中且 targetAddress 为空；无 active 时 ModelClaimBinding 优先返回 sleeping 以触发 wake
- **Pass Condition**: pkg/cache 与 pkg/types 单测覆盖四态混合
- **Evidence**: cache/gateway 新增单测

### AC-11: 删除时先停全部实例再移除 finalizer
- **Type**: `rule`
- **Given**: replicas=2，2 个 Active 实例分布在 2 pod，claim 带 DeletionTimestamp
- **When**: 多轮 reconcile；模拟首轮某 deactivate 失败/engine 仍可见，之后成功且 snapshot 确认消失
- **Then**: finalizer 在两个实例都确认停止前不移除（首轮后仍在）；全部确认后移除 finalizer，且两个 pod 的 annotation 都被删除
- **Pass Condition**: 单测断言（a）失败时 finalizer 留存+requeue；（b）成功后 finalizer 消失、deactivate 次数=2、annotation 清空
- **Evidence**: TestReconcileDeletionStopsAllInstancesBeforeFinalizer

### AC-12: 既有单副本行为不回归
- **Type**: `rule`
- **Given**: 现有全部 modelclaim/cache/types/gateway 单测与 envtest 用例
- **When**: 运行 `go test ./pkg/controller/modelclaim/... ./pkg/cache/... ./pkg/types/... ./pkg/plugins/gateway/...` 与 `make test`
- **Then**: 全部通过（必要时仅适配因实现演进必需的断言，不弱化原意图）
- **Pass Condition**: 命令退出码 0
- **Evidence**: CI 命令输出

### AC-13: 实现质量与仓库约定
- **Type**: `rubric`
- **Dimension**: 代码贴合现有模式（reconcile 结构、事件、condition、日志、表驱动测试、注释解释不变量）、变更聚焦、无无关重构
- **Scale**: 1-5
- **Anchors**: 1 = 另起炉灶/大面积无关改动；3 = 功能正确但风格游离；5 = 像原有代码自然延伸，测试与实现同构
- **Pass Threshold**: >= 4
- **Evidence**: 独立审查阅读 diff

## Open Questions
- 无（设计决定见 Assumptions；审批阶段可调整）。
