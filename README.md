# Experiment Lineage Registry

本地可复现的**实验追踪与制品血缘注册服务**。使用 FastAPI + SQLite(WAL) +
内容寻址文件存储，无外部服务、无真实凭据。所有数据保存在本地目录，可离线
运行、可重复验证。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload          # 与原始骨架相同的启动方式
# 或
uv run pytest                              # 运行全部测试（149 个用例）
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
| `ELR_DERIVATION_DEFAULT_LIMIT` | 后代/祖先查询默认每页条数 | `100` |
| `ELR_DERIVATION_MAX_LIMIT` | 单次谱系查询条数硬上限 | `1000` |
| `ELR_LOG_LEVEL` | 日志级别 | `INFO` |

不支持的取值（未知策略、负数容量、非法布尔值、非法日志级别等）在
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

## 运行派生（Derivation）：从已完成的运行分支迭代

把一次**已进入终态**的运行当作起点，显式换掉一部分输入，派生一条新的
运行；来源运行永不被修改，整条派生链持久保留、可双向反查。

`POST /runs/{id}/derive`，请求体（所有字段都可省略，省略即沿用来源）：

```json
{
  "idempotency_key": "rerun-2026-09-28-tune-lr",
  "experiment_id": null,
  "env_overrides": {"python": "3.13"},
  "config_overrides": {"train": {"lr": 0.02}},
  "env_remove": ["old_debug_flag"],
  "config_remove": ["train.weight_decay"],
  "add_artifacts": ["dataset@v2"],
  "remove_artifacts": ["legacy_notes@v1"],
  "replace_artifacts": [
    {"artifact_id": "model@v3", "replacement_id": "model@v4"}
  ],
  "metadata": {"reason": "tune learning rate"}
}
```

### 派生语义

* **来源必须终态**：只有 `succeeded/failed/canceled` 可作为起点，否则
  `409 source_run_not_terminal`；来源运行不存在 → `404 run_not_found`。
* **未提及即沿用**：env/config 递归深合并（RFC 7386 风格），未提到的键、
  嵌套兄弟字段与未提到的制品依赖原样继承；`*_overrides` 只负责覆盖/新增，
  `*_remove` 用点分路径显式删除（被删路径必须在来源中存在，否则
  `400 invalid_derivation`）。子运行默认落在来源运行所在实验，也可用
  `experiment_id` 显式指定（实验必须存在）。
* **来源运行不可变**：派生只新增行，从不回写来源运行；来源的状态、env、
  config、指纹、制品引用在派生前后逐字节一致（见
  `test_source_run_is_never_mutated`）。子运行从 `created` 开始按自己的
  状态机流转，终态规则照常适用。
* **原子落库**：子运行、它的全部 input 引用与派生记录在同一个
  `BEGIN IMMEDIATE` 事务里提交。校验失败（目标制品不存在/已回收、结构
  非法、幂等冲突）时**不会留下半个派生运行**。
* **派生记录**持久化到 `run_derivations`，保存规范化后的请求体、请求
  摘要与逐项变更，任何一次派生都能反查“从哪来、换了什么”。

### 标识与幂等

| 情形 | 结果 |
| --- | --- |
| 同一来源 + 同一套替换内容（无论是否带 `idempotency_key`） | **同一个子运行**；第二次起 HTTP 200、`created=false` |
| 同一来源 + 同一 `idempotency_key` 但替换内容不同 | `409 derivation_payload_mismatch`，**绝不静默多建一个运行** |
| 同一 `idempotency_key` 用在**不同**来源运行上 | 互不影响（键按来源隔离），各自产生自己的子运行 |

判定口径与指纹一致：键顺序、JSON 空白、易失时间字段、依赖书写顺序与
重复列出都不会让两份“等价请求”变成两次派生。

### 替换与指纹规则

* 子运行指纹按**替换后的实际输入**（env、config、生效制品的内容 sha256
  集合）重新计算，规则与 [指纹与可复现性](#指纹与可复现性)完全一致：
  键顺序、无害空白、`created_at/ts/...` 等时间字段、依赖顺序/重复都不
  造成漂移。
* 制品替换是显式的三类操作：
  * `replace_artifacts`：把来源依赖 `artifact_id` 换成活的
    `replacement_id`（两者都必须写明；不能拿已经继承的制品当替换目标；
    同一来源制品在一次派生里最多被替换一次；两个来源不能换成同一个
    目标）；
  * `add_artifacts`：新增当前来源没有的制品（已继承的制品不允许再 add）；
  * `remove_artifacts`：移除来源依赖（不存在于来源的不允许 remove）；
  * 同一制品不能同时出现在 remove 与 replace、add 与替换目标中，否则
    `400 invalid_derivation`。
* 响应的 `derivation.changes` **逐项**列出生效变更，而不是笼统的“不一致”：

  | category | change | 含义 |
  | --- | --- | --- |
  | `env` / `config` | `added` / `removed` / `replaced` | 点分叶子路径级别的新增/移除/改值，带 `from`/`to` |
  | `artifact` | `added` / `removed` / `replaced` | 制品维度新增、移除、替换（替换给出 from→to 的版本 ID） |

### 父子谱系怎么查

```
GET /runs/{id}/derivation     # 反查：该运行由谁派生、当时的请求与变更
GET /runs/{id}/children       # 直接子派生（完整列表，按派生次序）
GET /runs/{id}/descendants?limit=&cursor=   # 全部后代，按层（BFS）分页
GET /runs/{id}/ancestors?limit=             # 祖先链（每个运行至多一个父）
```

* 后代遍历为稳定的**逐层遍历**：同层按派生记录 id（即提交先后）排序；
  游标为不透明 token（base64 JSON），只用于继续翻页，非法游标
  `400 invalid_cursor`。
* **容量与截断如实标注**：`limit` 缺省取 `ELR_DERIVATION_DEFAULT_LIMIT`，
  上限为 `ELR_DERIVATION_MAX_LIMIT`（超限 `400 pagination_limit_exceeded`，
  非正整数 `400 invalid_pagination_limit`）。只要还有未返回的节点，
  `truncated=true` 且带 `next_cursor`；**绝不会把截断结果当完整结果返回**。
  祖先查询同理，超长链只返回前 `limit` 个并置 `truncated=true`。
* 派生关系只记录在 `run_derivations` 中；它**不会**自动写入
  `lineage_edges`——血缘结论仍只来自显式登记的血缘边（如需同时出现在
  血缘图里，照常 `POST /lineage`）。

### 并发、回收与登记碰撞

所有派生仍在服务内同一把可重入锁内完成读-判-写，并由
`UNIQUE(parent_run_id, request_hash)` 与
`UNIQUE(parent_run_id, idempotency_key)` 在持久层兜底：

* 同一来源被多个请求并发派生：等价请求**只产生一个子运行**（恰好一个
  `created=true`）；同键不同内容只有一方可能成功，其余得到
  `derivation_payload_mismatch`。
* 派生与制品回收（GC）并发：二者串行决出一个确定结果——派生先提交引用，
  制品即被运行引用、GC 必须保留；GC 先落 tombstone，派生得到
  `404 artifact_deleted`（或登记未完成时 `404 artifact_not_found`），
  **不允许悄悄换另一个制品顶上**，也不会留下指向已回收/回收中制品的
  引用。目标制品的存在性与存活状态在同一临界区内、同一事务前校验。
* 派生与新制品登记并发：登记先完成则派生成功；登记尚未完成则明确
  `artifact_not_found`，二者必居其一，`GET /integrity` 保持 healthy。

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
* 并发派生同一来源（等价请求）只产生一个子运行；派生与回收/登记并发时
  结果确定：要么引用已建立、制品受保护，要么明确拒绝（`artifact_deleted /
  artifact_not_found`），不会指向回收中的制品（见
  `tests/test_derivation_boundaries.py`）。

## HTTP 接口一览

```
GET    /                                  健康/兼容性根路由
GET    /health
POST   /experiments        GET /experiments   GET /experiments/{id}
POST   /runs              GET /runs[?experiment_id=]   GET /runs/{id}
POST   /runs/{id}/transitions             状态流转
POST   /runs/{id}/artifacts               挂载制品引用
POST   /runs/{id}/derive                  从终态运行派生新运行
GET    /runs/{id}/derivation              反查派生来源与替换内容
GET    /runs/{id}/children                直接子派生
GET    /runs/{id}/descendants?limit=&cursor=   后代分页（带截断标注）
GET    /runs/{id}/ancestors?limit=        祖先链（带截断标注）
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
uv run pytest                      # 全量（含并发、竞态、成环、重跑校验、派生）
uv run pytest -k concurrency       # 仅并发竞态
uv run pytest -k derivation        # 派生语义/并发/GC 竞态/分页/HTTP
uv run pytest -k "illegal or cycle or mismatch"
```

测试以非法状态转移、幂等冲突、血缘成环、不可复现重跑、并发更新与回收竞态
为主；派生部分额外覆盖：并发派生同一来源只产生一个运行、同键不同内容被
拒绝、替换目标不存在或已回收被拒且不留半个派生运行、等价输入指纹不漂移、
后代超上限分页与截断标注、派生不改动来源运行。失败一律断言具体 `code`
以保证原因可区分。单测日志以 `key=value` 形式打印运行标识、来源标识与
判定依据，例如：

```
event=run_transition run_id=run_... from=created to=queued reason=accepted
event=run_transition_denied run_id=run_... from=queued to=queued reason=already_in_state
event=lineage_cycle_rejected upstream=run_a downstream=run_b relation=depends_on
event=reproduction_checked run_id=run_... reproducible=False divergence_count=1
event=run_derived parent_run_id=run_... child_run_id=run_... request_hash=... fingerprint=... change_count=2
event=run_derivation_conflict parent_run_id=run_... child_run_id=run_... idempotency_key=... reason=payload_mismatch
event=run_derivation_idempotent_replay parent_run_id=run_... child_run_id=run_... request_hash=...
```

手工验证（服务启动后）：

```bash
curl -s localhost:8000/health
curl -s -XPOST localhost:8000/experiments -H 'content-type: application/json' -d '{"name":"demo"}'
```

手工验证一次派生（先把来源运行推到终态）：

```bash
# 1) 两个制品 + 一次终态运行
curl -s -XPOST 'localhost:8000/artifacts?id=model@v3' --data-binary @model_v3.bin
curl -s -XPOST 'localhost:8000/artifacts?id=model@v4' --data-binary @model_v4.bin
SRC=$(curl -s -XPOST localhost:8000/runs -H 'content-type: application/json' \
  -d '{"experiment_id":"'$EXP'","idempotency_key":"src","artifacts":["model@v3"],"config":{"lr":0.1}}' | jq -r .id)
for s in queued running succeeded; do
  curl -s -XPOST localhost:8000/runs/$SRC/transitions -H 'content-type: application/json' -d "{\"state\":\"$s\"}"
done
# 2) 派生：换模型、调学习率
curl -s -XPOST localhost:8000/runs/$SRC/derive -H 'content-type: application/json' -d '{
  "idempotency_key":"tune-lr",
  "config_overrides":{"lr":0.02},
  "replace_artifacts":[{"artifact_id":"model@v3","replacement_id":"model@v4"}]}'
# 3) 反查与谱系
curl -s localhost:8000/runs/$SRC/children
curl -s localhost:8000/runs/$SRC/descendants?limit=100
```

## 存储布局

```
.elreg-data/
├── registry.db            # SQLite 元数据（实验/运行/制品/血缘/指标/重跑/派生记录）
├── registry.db-wal/-shm   # WAL 模式文件
└── artifacts/<sha[:2]>/<sha>   # 内容寻址 blob
```

## 兼容范围

* 启动方式（`uv run uvicorn main:app --reload`）、根路由 `GET /`
  （`{"Hello":"World", ...}`）、`GET /health`、既有全部接口与请求/响应
  结构保持不变；本次仅**新增** `/runs/{id}/derive`、`/derivation`、
  `/children`、`/descendants`、`/ancestors` 五个只读/新增接口与
  `run_derivations` 一张新表（`CREATE TABLE IF NOT EXISTS`，旧库直接
  启动即可用，无需迁移）。
* 配置全部沿用环境变量读取方式；新增的 `ELR_DERIVATION_DEFAULT_LIMIT` /
  `ELR_DERIVATION_MAX_LIMIT` 均有确定默认值，非法取值（非正整数、
  default > max）在**进程启动加载配置时**即 `ConfigError
  code=invalid_config` 失败。
* 派生不会隐式改动血缘图、制品回收规则或既有幂等提交语义。
