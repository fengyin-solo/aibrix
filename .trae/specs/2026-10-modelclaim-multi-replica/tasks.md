# ModelClaim 多副本（replicas > 1）支持 - 实施计划

## Task 1: API 放开 replicas 限制并扩展 status.instances 字段
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - TR-1.1 pass：CRD diff 仅删除 replicas 的 maximum、instances 新增 ready(boolean)/restartCount(integer)；config/crd/model 与 dist/chart/crds 副本均与 controller-gen v0.16.1（CI pinned）bases 字节一致（逐文件 diff 全 OK，无无关漂移）。
  - TR-1.2 pass：`go build ./...`、`go vet ./api/... ./pkg/client/...` 退出码 0；deepcopy 用 v0.16.1 object-gen 复核无 diff（新字段为值类型）；applyconfiguration 用 pinned code-generator v0.31.8（go1.24.9 工具链，经硬链接 GOPATH 镜像运行）重新生成，全仓唯一差异即 modelclaiminstance.go 的 WithReady/WithRestartCount。
  - 变更文件：api/model/v1alpha1/modelclaim_types.go、config/crd/model/model.aibrix.ai_modelclaims.yaml、dist/chart/crds/model.aibrix.ai_modelclaims.yaml、pkg/client/applyconfiguration/model/v1alpha1/modelclaiminstance.go。
- **Description**:
  - 修改 `api/model/v1alpha1/modelclaim_types.go`：删除 Replicas 的 `+kubebuilder:validation:Maximum=1`（保留 Minimum=1/default=1，更新注释说明多副本语义：每副本位于不同 warm pod）。
  - `ModelClaimInstance` 新增 `Ready bool`（json `ready,omitempty`）与 `RestartCount int32`（json `restartCount,omitempty`），注释说明来自 runtime snapshot。
  - 运行 `make manifests` 与 `make generate` 重新生成 CRD（config/crd/bases 及同步后的 config/crd/model/...）、deepcopy、applyconfiguration/clientset；若工具无法下载则手工等价更新并用 `make verify` 复核。
- **Acceptance Criteria Addressed**: AC-1
- **Test Requirements**:
  - `rule` TR-1.1: config/crd/model/model.aibrix.ai_modelclaims.yaml 的 replicas schema 不含 maximum、仍含 minimum=1/default 1；instances item schema 含 ready(boolean) 与 restartCount(integer)；证据为 grep 片段。
  - `rule` TR-1.2: `make verify` 与 `go build ./...` 退出码 0；证据为命令输出。
- **Notes**: 不新增枚举值、不改 annotation/routing state 常量。

## Task 2: 调度器支持 HBM 硬约束、KV/负载排序与多副本确定性挑选
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - TR-2.1 pass：TestSelectPodForActivationWithStateFiltersPodsBelowHBMRequirement（需求 800：500 被过滤、1000 选中；全不足时错误含 HBM）；UnknownMemoryBypassesHBMRequirement 覆盖未知不过滤；TestHbmRequirementFromSnapshots 覆盖峰值/KVCapacity 取 max 与同 artifact 过滤。
  - TR-2.2 pass：RanksKVThenModelCount（KV→模型数排序）、PrefersLiveRuntimeState（artifact 优先于 HBM）、TieBreakByName、MultipleReplicasUseDistinctPods（连续 3 选不同 pod、第 4 次失败）全部通过。
  - TR-2.3 rubric 自评 5/5：可行性与排序均为纯函数表驱动测试，与 placement_test.go 既有风格同构；控制器仅在 ensureActivated 循环内增量 ModelCount/load。`go test ./pkg/controller/modelclaim/... -run 'TestSelectPod|TestHbmRequirement'` 通过。
- **Description**:
  - 在 `snapshot.go`/`placement.go` 中增加单实例 HBM 需求估计：扫描候选 pod snapshot，取相同 artifactURL 模型的 `max(HBMPeakBytes, KVCapacityBytes)`；0 = 未知。
  - `selectPodForActivationWithState`（或新签名）增加 feasibility：snapshot/MemoryKnown 且 `HBMFreeBytes < 需求` 的 pod 不可选；需求未知不过滤；TP/PP>1 继续用最小 rank 空闲。
  - 明确确定性全序：artifact 缓存 → snapshot 已知 → memory 已知 → HBM 空闲降序 → KVUsed 升序 → runtime ModelCount 升序 → 跨 claim load 升序 → locality → pod 名升序（把 computePodLoad 作为 placementState 之后的稳定 tie-break 保留）。
  - `ensureActivated` 循环内每选定一个 pod 即更新 alreadyOn/load/ModelCount 视图，保证同一 claim 后续副本不再选到同 pod；controller 层 collectPlacementStates 一次性取快照，连续挑选基于内存视图增量。
  - 表驱动单测覆盖：不足 HBM 被过滤、需求未知不过滤、KV 少者胜、模型数少者胜、artifact 缓存优先于 HBM、name 兜底、两副本选到不同 pod 且负载增量可见。
- **Acceptance Criteria Addressed**: AC-2, AC-4, AC-5
- **Test Requirements**:
  - `rule` TR-2.1: HBM 需求=800 时 free=500 的 pod 不可选、free=1000 可选；需求未知时两 pod 均可选。
  - `rule` TR-2.2: 排序用例（KV/模型数/artifact/name）全部断言唯一胜出 pod；连续两次挑选返回两个不同 pod。
  - `rubric` TR-2.3: 选择逻辑保持纯函数/表驱动风格，与现有 placement_test.go 同构；scale 1-5；anchors 1=逻辑散落在 reconcile 内难单测，3=可测但耦合，5=纯函数+完整表驱动；threshold >=4；证据为 diff 与测试代码。

## Task 3: 重写 reconcile 扩缩收敛：替换优先、牺牲品顺序与健康门禁
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Completion Evidence**:
  - TR-3.1/3.2/3.3 pass：新增 scaling.go 纯函数 selectScaleDownVictim（相位序 Failed→Sleeping→Activating→Active；Active 门禁 ready-1>=desired；Failed 门禁 ready>=desired；替换 Activating 受 nonFailed-1>=desired 保护），scaling_test.go 表驱动 10 例全过；时间线见 TestReconcileFailedInstanceReplaysToDifferentPod（替换 Ready 前 deactivate=0）、TestReconcileScaleDownThreeToOne（恰停 2 个、保留 warm-1）。
  - TR-3.4 pass：TestSelectScaleDownVictimOrderingAndGuards 覆盖四相位/name tie-break/替换保护/健康门禁。
  - 控制器重构：convergeInstances（activateToTarget→releaseFailedForSamePodRetry→scaleDownGuarded）替换旧 switch；stopInstance 统一去路由+deactivate；`go build ./...`、包内全部测试通过。

## Task 4: 实例健康收敛与 status/annotation 一致（ready/restartCount/engine-gone）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - TR-4.1/4.2/4.3 pass：TestReconcileInstanceStatusReflectsReadyAndRestarts（ready/restartCount/真实端口与 active annotation 一致；降级后 ready=false、restartCount 推进、port0 activating）；TestReconcileEngineVanishedMarksFailed（snapshot 成功但 engine 消失→Failed/EngineGone/port0 failed annotation）；snapshot RPC 失败维持旧状态（既有 TestReconcileSnapshotPortZero 等路径）。
  - recomputeReadiness 新聚合：全 Active→Active；部分可用→Activating(InstanceFailed/EngineStarting) 或 Sleeping（混合休眠）；全无可用且有 failed→Failed；replay 后可恢复。既有全部单测不回归。

## Task 5: engine 失败重放与 pod 漂移（含 fake runtime 按 pod 建模）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3, Task 4
- **Completion Evidence**:
  - TR-5.1 pass：TestReconcileFailedInstanceReplaysToDifferentPod 三时间线（失败标记→异 pod 激活且旧实例不动→Ready 后 deactivate+摘除，断言 deactivatePodIPs/annotation）。
  - TR-5.2 pass：TestReconcileFailedInstanceRetriesSamePod（无不同 pod 时 stop+摘除后同 pod 重新激活，全程单 engine）。
  - TR-5.3 pass：TestReconcileMultiReplicaReplaysAfterPodDeletion（删 warm-2 后补到 warm-3，warm-1 零 activate/deactivate，ready 恢复 2）。
  - fakeRuntime 改造为 models 按 podIP 维护、Activate/Deactivate/Snapshot/ListModels 均按 pod 作用域，并记录 activatePodIPs/deactivatePodIPs；既有测试全部适配通过。

## Task 6: DeletionTimestamp 下先停全部实例并确认后移除 finalizer
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3
- **Completion Evidence**:
  - TR-6.1 pass：TestReconcileDeletionWaitsForAllInstancesStopped 注入 deactivate 失败：RequeueAfter>0、finalizer 留存、2 条实例保留；TestReconcileDeletionWaitsForSnapshotConfirmation 覆盖 snapshot 仍见 engine 时保留。
  - TR-6.2 pass：恢复后 2 个实例全部 deactivate+snapshot 确认消失，finalizer 移除，两 pod annotation 均删除（deactivate 4 次=2 失败+2 成功，IP 集合断言）。
  - 实现 reconcileDeletion：未确认实例持久化到 status（重启续跑）、pod 消失视为确认、clearClaimMetrics 在最终移除时执行。
- **Description**:
  - 在 `modelclaim_controller.go` 中以"目标实例集合"重构主循环（保持函数小而可测）：
    - pruneDeadInstances 后计算 desired、failed 集合 F；effectiveTarget = desired + len(F)。
    - 激活阶段：len(instances) < effectiveTarget 时按 Task 2 选择器激活（排除所有已占用 pod，含 failed 实例所在 pod）；无不同 pod 且缺口仅来自 failed 时，走失败清理+同 pod 重试路径（Task 5 细化，本任务先实现计数与门禁骨架）。
    - 收敛阶段：每次 reconcile 先 health 校正再决定 scaleDown；牺牲品顺序 Failed→Sleeping→Activating→Active（同相位 pod 名升序取后者）。
    - 健康门禁：仅当移除后 readyReplicas >= desired 时允许停 Active；Failed 牺牲品仅在 ready>=desired 或当前无 Activating 替换实例且无不同候选时移除；保证"唯一健康实例不先停"。
  - 纯函数化牺牲品选择（如 `selectScaleDownVictim(instances, ready, desired, distinctCandidateAvailable)`）便于表驱动测试。
  - 新增控制器时间线单测：1→3 扩容（notReady 期间无 deactivate）、3→1 缩容（逐个停、ready 最终=1）、[Active,Failed] desired=2 在替换 Ready 前不动 Failed、Ready 后摘除、只剩 1 Active 时任何分支都不发 deactivate。
- **Acceptance Criteria Addressed**: AC-6
- **Test Requirements**:
  - `rule` TR-3.1: 扩容时间线：新实例 notReady 时 deactivateCalls 为空；全 Ready 前不摘除任何旧实例。
  - `rule` TR-3.2: 缩容时间线：3 Ready→desired1 后恰停 2 个、留 1 个 Active；每次停止后 ready 不低于当时目标。
  - `rule` TR-3.3: [Active, Failed]+替换场景：替换实例 Ready 前对 Failed pod 的 deactivate 次数=0；Ready 后=1 且 Failed 记录消失。
  - `rule` TR-3.4: 牺牲品选择纯函数表驱动测试覆盖四种相位顺序与 name tie-break。

## Task 4: 实例健康收敛与 status/annotation 一致（ready/restartCount/engine-gone）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - `reconcileInstanceHealth` 中用 snapshot 观测填充 `inst.Ready` 与 `inst.RestartCount`（来自 RuntimeSnapshotModel.Ready/RestartCount）；port/phase 切换时重写 annotation（沿用 annotateWarmPodWithState），保证 status 与 annotation 同源同值。
  - snapshot 成功但 `snapshotModelForClaim` 返回 nil（engine 在存活 pod 上消失）时，将实例置 Failed（reason EngineGone/事件），进入 Task 5 的重放流程；snapshot RPC 失败继续保持上一个已知状态。
  - recomputeReadiness 聚合多实例：ReadyReplicas=Active 计数；存在 Failed 不再永久把 claim 钉死为 Failed 而阻塞重放（Failed 仍体现在 phase/condition，但激活替换后可回到 Activating/Active）——定义聚合：有 Failed 且 ready==0 → Failed；有 Failed 且 ready>0 → 保持可服务（Active/Activating）并在 condition 中带 Degraded 信息（沿用 Ready=False+reason InstanceFailed，不新增 condition 类型）。
  - 单测：ready=true/restart_count=2 时 status 与 active annotation 一致；notReady 时 ready=false/port0/activating；engine 消失置 Failed；snapshot 报错时路由不抖动。
- **Acceptance Criteria Addressed**: AC-9
- **Test Requirements**:
  - `rule` TR-4.1: Active 实例 status.Ready=true、RestartCount=2、Port=真实端口且 annotation state=active/port 一致。
  - `rule` TR-4.2: engine 从 snapshot 消失（snapshot 成功）后实例 Phase=Failed、annotation port0/failed；snapshot RPC error 时维持原 Active+原端口。
  - `rule` TR-4.3: [Active, Failed] 聚合时 ReadyReplicas=1 且仍可路由，替换 Ready 后整体 Active；全部失败时 claim Phase=Failed。

## Task 5: engine 失败重放与 pod 漂移（含 fake runtime 按 pod 建模）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 3, Task 4
- **Description**:
  - 终态失败（runtime phase=failed 或 engine-gone）重放：存在不同候选 pod 时，激活替换实例（Task 3 effectiveTarget），等其 Ready 后 deactivate(stop) 旧实例、去 annotation、删记录；不存在不同候选时，deactivate 清理 + 删除 failed 记录，使普通激活路径在下一/后续 reconcile 重试（允许同 pod），依赖 10s requeue 限速；不产生重复 engine（激活前确认该 pod 无本 claim 存活实例：选择器排除 + activate 由 runtime 按 ClaimRef 幂等）。
  - pod 漂移：确认 pruneDeadInstances 覆盖 terminating/删除 pod；新 pod 出现由现有 pod watch enqueue 触发补齐；多副本下逐个补齐。
  - 改造单测 `fakeRuntime`：models 按 podIP 维护（Activate 记 podIP，Snapshot 只合并该 pod 的模型），snapshots 已按 IP；保证现有单 pod 测试不回归。
  - 新增单测：failed→不同 pod 重放顺序（AC-8 时间线）；无不同 pod 时同 pod 重试且无重复 engine；2 副本中删 1 pod 后在新 pod 补齐，另一实例全程 Active 不被扰动。
- **Acceptance Criteria Addressed**: AC-7, AC-8
- **Test Requirements**:
  - `rule` TR-5.1: failed 重放：替换 Ready 前旧 pod 无 deactivate；Ready 后旧 pod deactivate=1、annotation 删除、最终实例在新 pod 且为 Active。
  - `rule` TR-5.2: 仅有同 pod 可选时：failed 记录被清理并重新 activate 同 pod，任意时刻该 pod 上本 claim 的在途 engine 记录不超过 1 个（activate/deactivate 配对可验证）。
  - `rule` TR-5.3: pod 删除漂移：实例记录剪除、新 pod 激活后 ReadyReplicas 恢复、未受影响副本无 activate/deactivate 调用。

## Task 6: DeletionTimestamp 下先停全部实例并确认后移除 finalizer
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - 删除路径改为两阶段：对每个 instance 执行 deannotate + Deactivate(stop)；随后用该 pod snapshot 验证 `snapshotModelForClaim` 已不存在（pod 不存在/无 IP 视为已确认）；确认一个摘除一个记录（通过 status 更新持久化，保证重启续跑）。
  - 任一 deactivate 返回错误或 snapshot 仍观察到 engine：保留 finalizer，记录事件/condition，RequeueAfter 重试；全部确认后 RemoveFinalizer 并 Update。
  - 单测：2 副本删除——首轮注入 deactivate 失败，finalizer 仍在且 requeue；恢复后两个 deactivate 各 1 次、两个 annotation 均删除、finalizer 移除；pod 已删除的实例视为已确认不阻塞。
- **Acceptance Criteria Addressed**: AC-11
- **Test Requirements**:
  - `rule` TR-6.1: 注入一个 deactivate 错误时 finalizer 留存且结果含 RequeueAfter/Requeue。
  - `rule` TR-6.2: 成功路径：2 个实例全部确认后 finalizer 才消失；deactivate 总数=2；两 pod annotation 均不存在；snapshot 仍见 engine 时不移除。

## Task 7: 网关多副本路由集合与 wake 回退偏好
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - TR-7.1 pass：TestModelClaimMultiReplicaOnlyActiveBindingsAreRoutable（2 active 进 ListPodsByModel，activating/sleeping 不进）。
  - TR-7.2 pass：TestModelClaimBindingWakePreference（无 active 时 sleeping 优先于 activating/failed，确定性）。
  - TR-7.3 pass：TestTargetAddressMultiReplicaUsesPerPodPort（两个 active pod 分别 10.0.0.1:9001/10.0.0.2:9002）；既有 port0/label 回退测试不回归。pkg/cache、pkg/types、pkg/plugins/gateway 全部测试通过。
- **Description**:
  - `pkg/cache/modelclaim_state.go`：`ModelClaimBinding(model)` 的确定性回退改为按 state 偏好 sleeping→activating→failed→active，同 state 内按 podKey 排序，使无 Ready 实例时网关对 sleeping 副本发 wake（多副本下唤醒任一 sleeping 即可）。
  - 核对并以测试锁定：仅 port>0/active binding 进入 model→pod 映射（多 active pod = 多后端）；ListPodsByModel 返回全部 active pod；TargetAddress 用 per-pod 端口，activating/sleeping pod 返回空。
  - 新增 pkg/cache 测试：2 active+1 activating+1 sleeping 混合 → ListPodsByModel=2；无 active 时回退 binding 为 sleeping；pkg/types 测试：两个 active pod 各自 annotation 端口分别正确路由。
  - gateway 层无需改路由算法；如 MockCache/helper 需要适配则一并更新。
- **Acceptance Criteria Addressed**: AC-10
- **Test Requirements**:
  - `rule` TR-7.1: 混合四态下 ListPodsByModel 仅含 2 个 active pod；HasModel=true；sleeping/activating 不在后端列表。
  - `rule` TR-7.2: 无 active 时 ModelClaimBinding 返回 sleeping binding（触发 wake）；全 activating 时返回 activating。
  - `rule` TR-7.3: TargetAddress 对两个 active pod 分别返回各自端口；对 port0 binding 返回 ""。

## Task 8: envtest 集成测试：fixture 多副本能力与端到端用例
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 5, Task 6, Task 7
- **Description**:
  - 扩展 `test/utils/controller/modelclaim.go` 的 FakeModelClaimRuntime：models 按 (claimUID, podIP) 存储，snapshot 按请求 pod IP 只返回该 pod 的模型；SetClaimState 支持按 pod 定位；新增注入 deactivate 失败/engine 残留能力（如需要）。
  - 在 `test/integration/controller/modelclaim_test.go` 新增 ginkgo 用例：replicas=3 放置到 3 个不同 pod（状态/annotation/ready 计数）；扩容 1→3；删 pod 漂移到新 pod；engine failed 重放到不同 pod（顺序）；replicas 3→1 缩容；删除时多实例全部 stop 后才 NotFound；replicas>1 CRD 校验接受（创建即成功）。
  - envtest 二进制不可用（bin/k8s 缺失）时，至少保证编译通过（`go test -tags integration -run XXX ./...` 编译或 `go vet` 覆盖），并在证据中记录；以包内单测作为行为主证据。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-7, AC-8, AC-11
- **Test Requirements**:
  - `rule` TR-8.1: `go build ./test/...` 与带 integration tag 的测试包编译通过。
  - `rule` TR-8.2: 若 envtest 可运行：新增 ginkgo 用例全部通过；证据为命令输出。若环境不可用：记录阻塞证据，不标记 blocked（行为已由 TR-2~TR-7 单测覆盖），Review 时以单测证据验收。

## Task 9: 文档、sample 与指标核对
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 1, Task 3
- **Description**:
  - 更新 `docs/source/features/modelclaim.rst`：移除"仅 1 副本"警告措辞，replicas 行说明支持多副本、每副本位于不同 warm pod、扩缩/重放/路由语义；status.instances 补充 ready/restartCount。
  - 更新 `samples/modelclaim/modelclaims.yaml`（新增多副本示例或注释），不改部署清单。
  - 核对 metrics：desired/ready gauges 对多副本取值正确（读代码+现有 metrics_test 扩展一例 replicas=3）。
- **Acceptance Criteria Addressed**: AC-13（部分）, FR-9
- **Test Requirements**:
  - `rule` TR-9.1: 文档中不再出现"only accepts 1"类表述且描述与实现一致；sample 含 replicas>1 示例。
  - `rule` TR-9.2: metrics 单测覆盖 replicas=3 时 desired=3、ready 随实例数变化。

## Task 10: 全量验证与收尾
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 1-9
- **Description**:
  - 运行 `make fmt`、`make vet`、`make verify`、`go test ./pkg/controller/modelclaim/... ./pkg/cache/... ./pkg/types/... ./pkg/plugins/gateway/...`，再 `make test`（排除 e2e/integration 的主单元集）；修复发现的问题；不提交不推送。
- **Acceptance Criteria Addressed**: AC-12, AC-1
- **Test Requirements**:
  - `rule` TR-10.1: 上述命令全部退出码 0；证据为完整命令输出。
  - `rubric` TR-10.2: 变更聚焦度；scale 1-5；anchors 1=夹带无关重构/格式化，3=少量可接受噪声，5=diff 仅含本特性相关文件与意图内测试/文档；threshold >=4；证据为 `git status`/diff 概览。
