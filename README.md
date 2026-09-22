# Experiment Lineage Registry (Python)

本地可复现的实验追踪与制品血缘注册服务。数据完全保存在本地
SQLite 文件与内容寻址 blob 目录，零外部服务、零真实凭据。

## 快速开始

```bash
uv sync
# 启动（与原骨架一致：uvicorn main:app / fastapi run，GET / 保持兼容）
uv run uvicorn main:app --reload
```

默认库文件 `data/elr.db`、blob 目录 `data/artifacts`（首次使用自动创建，
已被 .gitignore 排除）。`GET /` 仍返回 `{"Hello": "World"}`，新增接口统一
前缀 `/api/v1`，交互文档见 `/docs`。

## 配置（环境变量 ELR_*，缺失取文档化默认值，非法取值启动即失败）

| 变量 | 默认 | 说明 |
|---|---|---|
| `ELR_DB_PATH` | `data/elr.db` | SQLite 文件路径 |
| `ELR_ARTIFACT_STORE` | `data/artifacts` | 内容寻址 blob 目录 |
| `ELR_CAPACITY_POLICY` | `reject` | 超限时 `reject` 拒绝 / `defer` 两阶段延迟 |
| `ELR_MAX_ARTIFACT_BYTES` | `0` | 已登记版本字节总和上限，0 为不限 |
| `ELR_RETENTION_RUNS` | `0` | 保留最近 N 个已结束运行时间窗内的未引用版本 |
| `ELR_SERIALIZE_WRITES` | `1` | 进程内串行化所有变更 |

语义（状态机 / 指纹 / 血缘 / 保留与回收）见 [docs/SEMANTICS.md](docs/SEMANTICS.md)。

## 端到端示例

```bash
# 1. 实验
curl -s -X POST localhost:8000/api/v1/experiments \
  -H 'Content-Type: application/json' -d '{"name":"exp1"}'

# 2. 制品（body 为 base64）
curl -s -X POST localhost:8000/api/v1/artifacts \
  -H 'Content-Type: application/json' \
  -d '{"name":"model","content_b64":"aGVsbG8="}'

# 3. 幂等提交运行（重复且同输入 -> 同一运行；不同输入 -> 409 冲突）
curl -s -X POST localhost:8000/api/v1/runs \
  -H 'Content-Type: application/json' \
  -d '{"experiment_id":"<id>","idempotency_key":"job-1",
       "env":{"python":"3.14"},"config":{"lr":0.1},
       "artifacts":[{"name":"model","content_hash":"<sha256>"}]}'

# 4. 状态流转（终态不可逆）
curl -s -X POST localhost:8000/api/v1/runs/<run_id>/transitions \
  -H 'Content-Type: application/json' -d '{"status":"RUNNING"}'

# 5. 血缘（成环 / 不存在对象 -> 409，原因码可区分）
curl -s -X POST localhost:8000/api/v1/lineage/run-deps \
  -H 'Content-Type: application/json' \
  -d '{"src_run_id":"<a>","dst_run_id":"<b>"}'

# 6. 可复现性校验（不一致返回 reproducible=false + divergences）
curl -s -X POST localhost:8000/api/v1/runs/<run_id>/verify \
  -H 'Content-Type: application/json' \
  -d '{"env":{"python":"3.14"},"config":{"lr":0.1},
       "artifacts":[{"name":"model","content_hash":"<sha256>"}]}'

# 7. 回收 / 悬空检查 / 用量
curl -s -X POST localhost:8000/api/v1/artifacts/gc/collect
curl -s localhost:8000/api/v1/artifacts/gc/dangling
curl -s localhost:8000/api/v1/artifacts/gc/usage
```

## 错误归类（HTTP body: `{error,message,details}`）

| 错误码 | HTTP | 触发场景 |
|---|---|---|
| `not_found` | 404 | 实验/运行/版本不存在；引用未登记制品（`reason` 区分） |
| `already_exists` | 409 | 重名实验 |
| `invalid_state_transition` | 409 | 终态迁移/非法跳转（`terminal`/`not_allowed`） |
| `terminal_field_write` | 409 | 终态或非 PENDING 后改指纹输入/写指标 |
| `idempotency_conflict` | 409 | 同 key 不同输入（含逐项 divergences） |
| `fingerprint_mismatch` | 422 | 预留：指纹不一致 |
| `lineage_cycle` | 409 | run 依赖成环 |
| `lineage_reference` | 409 | 血缘端点不存在 |
| `artifact_referenced` | 409 | 删除仍被运行引用的版本（附引用 run 列表） |
| `capacity_limit_reached` | 507 | 容量超限（`over_limit`/`capacity_after_reclaim`） |
| `missing_baseline` | 409 | 基线运行或基线指标缺失 |
| `inconsistent_caliber` | 422 | 指标口径不一致 |
| `missing_metric` | 409 | 候选指标从未写入（不按 0） |
| `invalid_config` | 400 | 入参或环境变量非法 |
| `dangling_reference` | 409 | 预留：悬空引用操作 |

## 本地验证

```bash
uv run pytest -q
```

测试重点：非法状态转移、幂等冲突、血缘成环、不可复现重跑、并发更新与
回收竞态；所有失败用例断言到**错误码与 `details.reason`**，日志打印
运行标识、状态迁移与判定依据（`run_id` / `from → to` / `basis` /
`decision` / 指纹）。并发用例在多线程（线程异常按测试失败处理）与多进程
共享同一库文件两种场景下验证一致性。
