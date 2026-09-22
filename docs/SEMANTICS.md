# 语义说明：状态机、指纹、血缘、保留与回收

本服务把实验追踪与制品血缘的全部结论建立在**本地 SQLite + 内容寻址 blob
存储**之上。所有变更操作在进程内通过同一把可重入连接锁与
`BEGIN IMMEDIATE` 事务串行化；不依赖任何外部服务或凭据。

## 1. 运行状态机

```
PENDING ──▶ RUNNING ──▶ SUCCEEDED
   │           │
   │           ├──────▶ FAILED
   │           └──────▶ CANCELED
   ├──────────────────▶ FAILED
   └──────────────────▶ CANCELED
```

* 合法迁移：`PENDING→{RUNNING, FAILED, CANCELED}`，
  `RUNNING→{SUCCEEDED, FAILED, CANCELED}`。
* `SUCCEEDED / FAILED / CANCELED` 为**终态**，不可离开。
* 非法请求被拒绝为 `invalid_state_transition`，并在 `details.reason` 中
  区分原因：
  * `terminal`：当前已是终态（含从终态再迁移）；
  * `not_allowed`：迁移不在允许表内（`details.allowed` 给出允许集合）；
  * 未知目标状态直接在消息中指出。
* **关键字段冻结**：离开 PENDING 后，env / config / 制品清单是指纹输入，
  任何改写返回 `terminal_field_write`（`details.field` 指明字段）。
  PENDING 阶段允许整体替换清单，并同步重算指纹。指标/参数写入仅在
  PENDING / RUNNING 接受，终态后返回 `terminal_field_write`。
* 每次迁移写一条 `audit_log`，日志打印 `run_id`、`from → to` 与判定依据。

## 2. 幂等提交

`idempotency_key` 在库内有唯一索引。

* 同 key + **规范化后相同的输入** → 返回**同一运行**（同 id、同指纹），
  不产生第二条运行；
* 同 key + 不同 env/config/制品依赖 → 拒绝为 `idempotency_conflict`，
  `details` 同时给出 `existing_run_id`、新旧指纹与逐项 `divergences`，
  **绝不静默覆盖**，也不会新建运行。

## 3. 指纹与可复现性

运行指纹 = 对三元组 `(env, config, artifacts)` 的规范化 JSON 做 sha256：

* 对象键按字典序输出 —— **键顺序不影响指纹**；
* 字符串做 NFKC 归一化并去除首尾**无害空白**（内部空白保留）；
* 封闭的时间字段集合在所有层级被剔除，**时间字段变化不导致漂移**：
  `created_at, updated_at, ended_at, submitted_at, started_at,
  finished_at, timestamp, time, date, datetime, ts`；
* 制品清单只覆盖 `name / content_hash / role`（version_id 是派生值）。

重跑校验 `POST /api/v1/runs/{id}/verify` 用同一套规范化重算：

* 完全一致且所有 blob 在盘 → `reproducible=true`；
* 任一差异 → `reproducible=false`，`divergences` 用 JSON 指针风格路径
  区分「rerun 缺失 / rerun 多出 / 值不一致」；blob 丢失单列
  `missing_blobs[].reason=artifact_blob_missing`。
* 结论持久化在 `repro_checks`，无法复现的运行被显式标记，不会判为通过。

## 4. 制品内容寻址与版本化

* 逻辑名（`artifacts`）下挂不可变版本（`artifact_versions`），
  `(artifact_id, version)` 与 `(artifact_id, content_hash)` 均唯一；
* 字节存于 `<ELR_ARTIFACT_STORE>/ab/<sha256>`，临时文件 + 原子 rename，
  同内容去重（同一条版本行、同一份 blob），不同内容分配新单调版本号，
  永远不会复用既有版本标识；
* 容量按版本字节求和。超限行为：
  * `reject`（默认）：登记直接失败 `capacity_limit_reached`，
    `reason=over_limit`；
  * `defer`：先尝试回收待删版本再复查，仍超限才失败，
    `reason=capacity_after_reclaim`（可区分）。

## 5. 血缘语义

* 血缘**只来自显式登记表** `lineage`，不从请求内容隐式推断。
* run→run 边表示「src 依赖 dst」，图必须无环：src==dst 或 dst 已能到达
  src 时拒绝为 `lineage_cycle`（`basis=path_dst_to_src_already_registered`）。
* 引用不存在的运行 / 制品版本拒绝为 `lineage_reference`（`details.role`
  区分 src / dst / artifact_version）。
* 相同边的重复登记是幂等的，返回同一 `edge_id`。
* 查询返回上游运行、下游运行与制品链接，**每条结论带 edge_id、登记时间、
  relation、registered_by**，可回溯到具体登记记录。
* 运行清单中的制品依赖（`run_artifacts`）与显式 `run_artifact` 血缘边
  共同构成制品引用集合。

## 6. 指标与参数

* 写入追加到 `run_metrics / run_params`，带每个 (run,name) 的单调序号；
  读取取**最大序号**点（最后提交的写入可见，历史点保留）。
* 比较 `POST /runs/{id}/metrics/{name}/compare`：
  * 基线运行不存在 → `missing_baseline / baseline_run_missing`；
  * 基线缺该指标 → `missing_baseline / baseline_metric_missing`；
  * 候选缺该指标 → `missing_metric / candidate_metric_missing`
    （**缺失绝不按 0 参与比较**）；
  * 口径（caliber）不同 → `inconsistent_caliber`（给出两侧口径）；
  * 其余返回 delta、ratio 与 `basis=latest_seq_visible_value`。

## 7. 保留与回收规则

* **被引用的版本永不删除**。每次删除都在串行写事务内重新核对引用集合
  （`run_artifacts` ∪ run_artifact 血缘边），因此回收与登记/引用并发时
  不会出现半删除或悬空引用。
* **两阶段（`ELR_CAPACITY_POLICY=defer`）**：首次 `collect` 只把无引用
  版本标记到 `gc_deletions`；下一次 `collect`（或登记时容量驱动的回收）
  重新核对后再真正删除。仍被引用的待删标记会保留，待其无引用后的下一轮
  回收。`reject` 策略为单阶段直接回收。
* **保留窗口** `ELR_RETENTION_RUNS=N`：保留「晚于第 N 近已结束运行的
  ended_at」创建的未引用版本；已结束运行不足 N 条时保守保留全部。
* **悬空引用显式暴露**：版本行存在但 blob 不在盘时，
  `GET /artifacts/gc/dangling` 逐条列出 run/version/hash/来源/原因；复现
  校验也会因此失败。绝不静默忽略。

## 8. 并发一致性

* 单一共享 SQLite 连接由进程内可重入锁保护，写事务全程持锁，杜绝多线程
  在同一连接上交叉 BEGIN/COMMIT 造成的状态越位、重复计数或部分提交；
* 同 key 并发提交恰好落一条运行；并发状态迁移恰好一次成功，其余按
  `terminal`/`not_allowed` 被拒；并发指标写序号稠密无丢失；
* 回收与引用并发：已提交的引用必然保护对应版本；登记与引用分两步时若回收
  先赢，引用方收到可区分的 `artifact_version_not_registered`（不是悬空
  引用）。多进程共享同一库文件时，`BEGIN IMMEDIATE` 同样串行化。
