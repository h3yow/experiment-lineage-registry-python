# Experiment Lineage Registry

本地可复现的**实验追踪与制品血缘注册服务**。使用 FastAPI + SQLite(WAL) +
内容寻址文件存储，无外部服务、无真实凭据。所有数据保存在本地目录，可离线
运行、可重复验证。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload          # 与原始骨架相同的启动方式
# 或
uv run pytest                              # 运行全部测试（132 个用例）
```

服务默认把数据写入 `.elreg-data/`（`registry.db` + `artifacts/`）。
根路由 `GET /` 保持原有响应 `{"Hello": "World"}`（附加 `service` 字段）。

## 配置（环境变量，均有默认值）

| 变量 | 含义 | 默认值 |
| --- | --- | --- |
| `ELR_DATA_DIR` | 数据目录 | `.elreg-data` |
| `ELR_DB_PATH` | SQLite 路径 | `<data>/registry.db` |
| `ELR_ARTIFACT_DIR` | blob 存储目录 | `<data>/artifacts` |
| `ELR_RETENTION_MAX_AGE_SECONDS` | 未引用制品保留期（秒） | `604800`（7 天） |
| `ELR_CAPACITY_MAX_BYTES` | 存活制品总容量硬上限 | `1000000000` |
| `ELR_CAPACITY_POLICY` | 超限策略：`reject` / `evict-oldest` / `delay` | `reject` |
| `ELR_GC_ON_OVERFLOW` | 登记新制品前先尝试回收 | `true` |
| `ELR_CAPACITY_WAIT_TIMEOUT_SEC` | `delay` 策略最长等待秒数 | `5.0` |
| `ELR_DERIVATION_PAGE_LIMIT` | 派生后代查询默认分页大小 | `100` |
| `ELR_DERIVATION_MAX_LIMIT` | 单次后代查询允许的最大 `limit` | `1000` |
| `ELR_DERIVATION_SCAN_CAP` | 后代遍历的硬上限（边数） | `100000` |
| `ELR_LOG_LEVEL` | 日志级别 | `INFO` |

不支持的取值（未知策略、负数容量、非法布尔值、非法日志级别，以及
`PAGE_LIMIT > MAX_LIMIT`、`MAX_LIMIT > SCAN_CAP` 等）在
**进程启动加载配置时立即失败**（`ConfigError code=invalid_config`），
不会拖到首次使用才暴露。

## 运行状态机

```
created ──► queued ──► running ──► succeeded  (终态)
   │           │          ├──► failed        (终态)
   │           │          └──► canceled      (终态)
   └───────────┴──────────► canceled         (终态)
```

* 合法转移仅上表列出的边；`succeeded / failed / canceled` 为**终态，不可逆**。
* 拒绝原因均有**可区分错误码**：

  | 情形 | code | HTTP |
  | --- | --- | --- |
  | 目标状态名未知 | `unknown_state` | 400 |
  | 当前状态不允许转到目标 | `illegal_transition` | 409 |
  | 目标与当前相同 | `already_in_state` | 409 |
  | 终态后再转移/写指标/挂制品 | `terminal_state` | 409 |
  | 运行不存在 | `run_not_found` | 404 |

* **终态关键字段不可改写**：终态后状态转移、指标写入、制品挂载一律拒绝；
  幂等重放只返回原运行，指纹与配置永不被覆盖。

## 幂等提交

`POST /runs` 带 `idempotency_key` 时：

* 同一 `(experiment_id, idempotency_key)` 的重复请求**落到同一运行**
  （HTTP 200，响应体 `"created": false`）。
* 键相同但 **env / config / metadata / 制品依赖** 任一实质不同，返回
  `409 idempotency_payload_mismatch`，消息与日志列出分歧字段；
  **不会产生第二个运行，也不会静默覆盖**。
* 幂等键按实验隔离；不同实验可复用同一键。
* 比较口径与指纹一致：键顺序、JSON 空白、易失时间字段不影响判定。

## 派生（Derivation）：以终态运行为起点迭代

派生让一次**已经进入终态**（`succeeded / failed / canceled`）的运行成为新
运行的起点：显式声明要换掉的输入，其余全部沿用来源运行。来源运行**只读、
永不被派生改动**，新运行从 `created` 开始走自己的状态机。

`POST /runs/{source_run_id}/derivations`：

```json
{
  "artifact_replacements": {"a@v1": "a@v2"},
  "remove_artifacts": ["b@v1"],
  "env": {"cuda": "12.4"},
  "config": {"lr": 0.2, "model": {"hidden": 512}},
  "idempotency_key": "iter-7"
}
```

* `artifact_replacements`：**键必须是来源运行已有的 input 制品**，值是替换
  目标；键不在来源依赖里 → `409 replacement_source_not_found`。
* `remove_artifacts`：从继承集合中移除的已有 input；同一项既替换又移除 →
  `400 invalid_replacement`。
* `env` / `config`：在来源值上做**深度合并（deep-merge）**，未提到的路径
  原样继承；不是整体覆盖。
* 不支持对非终态运行派生 → `409 source_not_terminal`。

### 派生身份与幂等

派生的"身份"取决于是否携带提交标识（`idempotency_key`）。两条规则都由
**数据库级唯一约束**兜底，因此在多服务实例、多进程共享同一份本地数据
（同一个 SQLite 文件）时同样成立，不依赖单个进程内的锁：

* **带 `idempotency_key`**：身份是 `(source_run_id, idempotency_key)`。
  * 同一来源 + 同一键 + 相同替换内容 → 永远落到同一个派生运行
    （首次 `201 created=true`，重放 `200 created=false`），并发下也
    只创建一个；
  * 同一来源 + 同一键 + **不同**替换内容 → 恰好保留先提交的一个，另一
    侧得到 `409 derivation_idempotency_mismatch`，错误消息与日志指明
    冲突的提交标识、来源运行与已存在的派生运行，**绝不悄悄多算一个**；
  * 不同键互不干扰：即使替换内容完全相同，也是各自独立的派生运行
    （与 `POST /runs` 的幂等键语义一致）。
* **不带键**：身份是 `(source_run_id, replacement_digest)`——规范化后的
  替换集合摘要。等价请求（键序、无害空白、易失时间字段、remove 列表的
  顺序与重复不同）收敛到同一个派生运行。
* 冲突或被拒绝的一侧**不留半成品**：派生运行、输入引用、派生记录与血缘
  边在同一事务内写入，失败即整体回滚；已成功的派生结果与来源运行不被
  改动或覆盖。
* 幂等作用域是"来源运行"：不同来源可复用同一键。
* 每个派生都持久化一条 `run_derivations` 记录（来源、派生运行、替换摘要、
  逐项变更、时间），任何派生都能反查"从哪来、换了什么"：
  `GET /runs/{derived_run_id}/derivation`。普通运行查派生记录 →
  `404 derivation_not_found`。`GET /runs/{id}` 的 `derived_from` 字段给出
  直接来源（非派生运行为 `null`）。
* 旧数据目录在启动时自动迁移：`run_derivations` 上把不同提交标识错误
  折叠为同一身份的旧表级唯一约束会被重建（既有记录原样保留），唯一性
  改由 `(source_run_id, idempotency_key)` 唯一索引与"仅无键记录"的
  `(source_run_id, replacement_digest)` 部分唯一索引共同保证。

### 替换、指纹与逐项变更

* 派生运行的指纹按**替换后的实际输入**重新计算，规则与提交时完全一致：
  JSON 键顺序/无害空白、易失时间字段、制品依赖的书写顺序与重复都不造成
  指纹漂移；制品贡献的仍是**内容 sha256**。
* 响应 `changes` 对每一处真实差异逐项标注，不给笼统“不一致”：

  | category | op | key 含义 |
  | --- | --- | --- |
  | `env` / `config` | `added` / `removed` / `replaced` | 点分叶子路径 |
  | `artifact` | `replaced` | 被替换掉的来源制品 id（值给出新 id） |
  | `artifact` | `removed` | 被移除的来源制品 id |
  | `artifact` | `added` | 不在替换映射里、真正新增的制品 id |

  每项带 `source_value` / `derived_value` 双方取值。

### 父子谱系查询

* 每次派生都会登记一条 `relation=derived_from` 的血缘边
  （source=upstream → derived=downstream），因此既可以用既有的
  `GET /runs/{id}/lineage/upstream|downstream` 看直接父子，也可以用专用
  接口遍历整棵派生树。
* `GET /runs/{id}/descendants?limit=&cursor=`：按 **BFS** 遍历所有传递后代
  （菱形谱系会去重），每项含 `run_id / source_run_id / depth /
  derivation_id / created_at`。
* **容量与分页是显式的、会如实标注**：
  * `limit` 默认 `ELR_DERIVATION_PAGE_LIMIT`，必须 ≥1 且 ≤
    `ELR_DERIVATION_MAX_LIMIT`，否则 `400 invalid_limit / limit_exceeded`；
  * 遍历边数有硬上限 `ELR_DERIVATION_SCAN_CAP`；
  * 还有下一页时返回 `truncated=true, truncation_reason=page_limit` 与
    `next_cursor / next_offset`；撞上硬上限时
    `truncation_reason=scan_cap`。**绝不把截断结果当完整结果返回**；
  * `cursor` 不透明，非法游标 → `400 invalid_cursor`。

### 并发、回收与边界

* 所有派生与制品登记/回收共用同一把服务锁并在单事务内落库
  （新运行 + 制品引用 + 派生记录 + 血缘边原子提交），因此：
  * 同一来源被并发派生、且替换集合相同 → **只产生一个运行**；
  * 派生**不会引用不存在或已被/正被回收的制品**：替换目标在写入前统一校验，
    不存在 → `404 artifact_not_found`，已回收 → `409 artifact_deleted`，
    消息点名具体是哪一项；失败时事务回滚，**不留下半个派生运行**，也不会
    悄悄换另一个制品顶替。
* 派生新运行不登记新制品内容，因此不受容量策略限制；容量策略仍只作用于
  制品上传。

## 制品：内容寻址、版本化与回收

* 制品以请求体原始字节上传：`POST /artifacts?id=<版本ID>`，服务端计算
  `sha256`，blob 存于 `artifacts/<sha前两位>/<sha>`，写入采用
  **临时文件 + 原子 rename**，不会出现半个文件。
* **内容相同即去重**：相同字节再次登记返回同一制品（`created=false`）。
  相同内容想换一个版本 ID → `409 content_already_registered`；
  同一版本 ID 上传不同字节 → `409 artifact_version_exists`
  （不同内容绝不复用同一标识）。
* 运行通过 `run_artifacts` 引用制品（提交时声明 input，或
  `POST /runs/{id}/artifacts` 挂载）。
* **回收规则**（`POST /artifacts/gc`，或容量探测时自动触发）：
  * 被**任意运行引用**的制品永不删除（`retained` 中给出
    `reason=referenced_by_run` 与 run 列表）；
  * 未引用但仍在保留窗口内的不删除（`within_retention_window`）；
  * 每个制品在独立事务中先落删除标记（tombstone），提交后再删 blob 文件，
    并发回收同一制品只有一方生效，不会半删除、不会重复计数。
* **悬空引用显式暴露**：提交/挂载/校验引用不存在或已回收的制品 →
  `409 artifact_not_found / artifact_deleted`；`GET /integrity`
  主动巡检“元数据存活但 blob 缺失”等悬空情况，`healthy=false` 时列出明细。

### 容量策略

登记前若 `现有总字节 + 新字节 > ELR_CAPACITY_MAX_BYTES`：

* `reject`（默认）：`507 capacity_exceeded`，元数据与 blob 均不落盘；
* `evict-oldest`：先按回收规则清理可回收项后重试，仍不足则
  `507 capacity_exceeded_after_eviction`；被引用/未过期制品受保护；
* `delay`：在条件变量上等待其他请求完成回收释放空间，被唤醒后重验；
  超时返回 `503 capacity_wait_timeout`（可区分于拒绝）。

## 血缘语义

`POST /lineage {"upstream_run_id","downstream_run_id","relation"}`：

* 登记前校验两端运行存在，否则分别返回 `upstream_run_not_found` /
  `downstream_run_not_found`。
* **成环拒绝**：自环，或新边会使 downstream 沿已登记边重新到达 upstream
  （传递成环），返回 `409 cycle_detected`，边不会落库。
* 相同三元组（upstream, downstream, relation）重复登记是幂等的，返回同一
  edge id。
* 血缘结论**只来自登记记录**：查询上游/下游（`GET /runs/{id}/lineage/*`）
  就是遍历 `lineage_edges` 表中的边，绝不由请求输入隐式推断；每条边带
  `registered_at` 与自增 id，可逐条回溯。

## 指纹与可复现性

运行提交时对 `(env, config, 制品内容摘要集合)` 计算 SHA-256 指纹并冻结。

规范化规则：

* JSON 递归按键排序、紧凑序列化——**键顺序与无害空白不造成漂移**；
* 易失字段（`created_at/updated_at/timestamp/ts/submitted_at/started_at/
  finished_at/recorded_at/wall_time/wall_clock/time`）在哈希前剥离——
  **无关时间字段不造成漂移**；
* 制品依赖贡献的是**内容 sha256**而非版本 ID，集合排序去重——依赖书写
  顺序、重复列出不造成漂移；
* env/config/artifacts 三段带域分隔前缀，避免跨段碰撞。

`POST /runs/{id}/verify` 按重跑提供的 env/config/artifacts 重算指纹：

* 一致 → 200，`reproducible=true`；
* 不一致 → **不判通过**：`422 fingerprint_mismatch`，响应体 `reproduction`
  中给出 `divergence` 明细（`category=env|config|artifact`、点分路径
  `key`、原因 `value_mismatch / missing_in_actual /
  unexpected_in_actual / artifact_missing_in_rerun /
  artifact_not_in_registration` 及双方取值）；
* 每次校验都作为 `reproduction_attempts` 记录持久化（含是否可复现），
  历史可查。

## 兼容性范围

派生能力是**纯增量**，不改变任何既有行为：

* 启动方式（`uvicorn main:app`）、配置读取方式（环境变量 + 启动期 fail-fast）
  不变；根路由 `GET /`、`/health` 与所有既有路径、请求/响应字段保持兼容。
* `RunView` 仅**新增**一个可空字段 `derived_from`，旧调用方无需改动。
* 新增的三个环境变量都有确定默认值；非法取值在启动期直接报错
  （见“配置”一节）。
* 派生血缘使用新 relation 值 `derived_from`，不与用户自定义 relation 冲突；
  既有 `/lineage` 与上下游查询语义不变。
* 派生唯一性语义按提交标识修正（见"派生身份与幂等"）：不同
  `idempotency_key` 的派生请求现在是各自独立的命名空间，不再因替换内容
  相同而被折叠或判冲突；同键不同内容仍在任何部署形态下被拒绝。旧数据
  目录启动时自动迁移，无需手工干预。

## 指标与参数

* `POST /runs/{id}/metrics {"name","value","step?"}`：
  * 同一 `(run,name,step)` 同值重复写 → 幂等返回同一记录；
  * 同点不同值 → `409 metric_value_conflict`，不允许改写历史；
  * 多 step 多次写全部保留；比较时未指定 step 按**确定性的最新值规则**
    （最大 step，`step=null` 视为 -1）取值；
  * 终态运行拒绝写入（`terminal_state`）。
* `POST /metrics/compare`：
  * 必须显式给出每个指标的 `direction=maximize|minimize`（非法值
    `invalid_direction`），可带非负 `tolerance`（`|Δ|≤tol` 记 equal）；
  * **缺失绝不当零**：基线缺、候选缺、两边都缺分别返回
    `metric_missing_baseline` / `metric_missing_candidate` /
    `metric_missing_both`（HTTP 422）；
  * 返回逐指标 verdict（better/worse/equal）、delta 与汇总
    `{total, better, worse, equal, regressed}`。

## 并发一致性

* 服务内一把可重入锁串行化所有读-改-写序列；SQLite `BEGIN IMMEDIATE`
  事务保证多行原子性，WAL + `busy_timeout` 提供持久层兜底。
* 同一运行被多个请求并发转移：每一步**恰好一个成功**，其余得到
  `already_in_state / illegal_transition / terminal_state`，不会状态越位。
* 并发同幂等键提交只产生一个运行；并发同内容登记只计一次字节；
  回收与挂载并发不会产生悬空引用或半删除（见
  `tests/test_concurrency.py`）。
* 派生的提交标识唯一性由数据库唯一约束保证，跨实例/跨进程同样成立：
  两个服务实例并发地对同一来源使用同一 `idempotency_key` 派生时，替换
  内容不同则**恰好一个成功**，另一个得到可区分的
  `409 derivation_idempotency_mismatch` 且不留下任何运行、输入引用或
  谱系记录；内容相同则两边收敛到同一个派生运行（见
  `tests/test_derivation_multi_instance.py`）。

## HTTP 接口一览

```
GET    /                                  健康/兼容性根路由
GET    /health
POST   /experiments        GET /experiments   GET /experiments/{id}
POST   /runs              GET /runs[?experiment_id=]   GET /runs/{id}
POST   /runs/{id}/transitions             状态流转
POST   /runs/{id}/artifacts               挂载制品引用
POST   /runs/{id}/derivations             从终态运行派生新运行
GET    /runs/{id}/derivation              反查某运行的派生来源与替换明细
GET    /runs/{id}/descendants[?limit=&cursor=]   分页遍历传递后代
POST   /artifacts (raw bytes, ?id=&filename=&media_type=)
GET    /artifacts        GET /artifacts/{id}   GET /artifacts/{id}/content
POST   /lineage
GET    /runs/{id}/lineage/upstream|downstream
POST   /runs/{id}/metrics     GET /runs/{id}/metrics[?name=]
POST   /metrics/compare
POST   /runs/{id}/verify                   可复现性校验
POST   /artifacts/gc                       回收
GET    /integrity                          悬空引用巡检
```

错误响应统一为：

```json
{"error": {"code": "illegal_transition", "message": "..."}}
```

## 本地验证方法

```bash
uv run pytest                      # 全量（含并发、竞态、成环、重跑校验）
uv run pytest -k concurrency       # 仅并发竞态（含并发派生 / 派生撞回收）
uv run pytest -k derivation        # 派生、幂等、逐项变更、后代分页
uv run pytest -k multi_instance    # 多实例/多进程下的派生唯一性与旧库迁移
uv run pytest -k "illegal or cycle or mismatch"
```

测试以非法状态转移、幂等冲突、血缘成环、不可复现重跑、并发更新与回收竞态
为主；失败一律断言具体 `code` 以保证原因可区分。单测日志以
`key=value` 形式打印运行标识、状态迁移与判定依据，例如：

```
event=run_transition run_id=run_... from=created to=queued reason=accepted
event=run_transition_denied run_id=run_... from=queued to=queued reason=already_in_state
event=lineage_cycle_rejected upstream=run_a downstream=run_b relation=depends_on
event=reproduction_checked run_id=run_... reproducible=False divergence_count=1
event=run_derived source_run_id=run_... derived_run_id=run_... basis=source+replacement_digest changes=2
event=derivation_idempotent_replay source_run_id=run_... derived_run_id=run_... basis=source+replacement_digest
event=descendants_listed root_run_id=run_... returned=100 truncated=True basis=page_limit
```

手工验证（服务启动后）：

```bash
curl -s localhost:8000/health
curl -s -XPOST localhost:8000/experiments -H 'content-type: application/json' -d '{"name":"demo"}'
# 等来源运行进入 succeeded 后派生：改 config 的 lr，其余沿用
curl -s -XPOST localhost:8000/runs/run_xxx/derivations \
  -H 'content-type: application/json' \
  -d '{"config":{"lr":0.2},"idempotency_key":"iter-7"}'
curl -s "localhost:8000/runs/run_xxx/descendants?limit=50"
```

## 存储布局

```
.elreg-data/
├── registry.db            # SQLite 元数据（实验/运行/制品/血缘/指标/重跑记录）
├── registry.db-wal/-shm   # WAL 模式文件
└── artifacts/<sha[:2]>/<sha>   # 内容寻址 blob
```
