# 任务 1：Airflow 架构与编排机制学习笔记

> 对应工程：`docker-compose.yaml`、`dags/`。所有结论都能在本工程代码与运行界面中找到对应物。

## 1. 整体架构（Airflow 3.3.2，CeleryExecutor）

```
                 ┌──────────────────────────────────────────────┐
                 │              浏览器 / LLM 网关（宿主机）         │
                 └───────────────┬──────────────┬────────────────┘
                        HTTP/REST│ JWT Bearer   │
                 ┌───────────────▼──────────────▼────────────────┐
                 │  airflow-apiserver  (容器内 8080，宿主机 18080) │
                 └───────┬───────────────┬───────────────┬────────┘
                         │ 读写元数据      │ 提交任务消息     │ 事件触发
                 ┌───────▼───────┐  ┌─────▼──────┐   ┌─────▼──────┐
                 │   postgres 16  │  │   redis    │   │ triggerer  │
                 │  元数据库       │  │ Celery  Broker│  (延迟/可 defer 任务) │
                 └───────▲───────┘  └─────▲──────┘   └────────────┘
                         │                │
        ┌────────────────┼────────────────┼────────────────────┐
        │                │                │                    │
  ┌─────┴─────┐   ┌──────┴──────┐  ┌─────┴──────┐         ┌─────┴──────┐
  │  scheduler│   │ dag-processor│  │  celery   │         │  celery    │
  │  调度决策  │   │ 解析 DAG 文件│  │  worker 1 │  …      │  worker n  │
  └───────────┘   └─────────────┘  └────────────┘         └────────────┘
```

| 组件 | 职责 | 本工程服务名 |
| --- | --- | --- |
| API Server | 提供 Web UI 与 REST API，处理认证、DAG Run 创建、任务实例查询 | `airflow-apiserver` |
| Scheduler | 扫描 DAG、判断依赖与调度时间、把待执行任务分发到 Executor | `airflow-scheduler` |
| Dag Processor | 独立进程解析 `dags/` 下的 Python 文件、生成 DAG 结构写入元数据库（Airflow 3 从 scheduler 拆出） | `airflow-dag-processor` |
| Triggerer | 运行 deferrable（可挂起）任务的事件循环，事件就绪后再唤醒 Worker | `airflow-triggerer` |
| Celery Worker | 真正执行 PythonOperator 等任务的进程池，由 Broker 拉取任务 | `airflow-worker` |
| Postgres | 元数据库：DAG 定义、任务实例状态、XCom、变量、连接、审计记录 | `postgres` |
| Redis | Celery Broker：scheduler → worker 之间的任务消息队列；结果后端用 Postgres | `redis` |
| Flower | Celery 集群监控面板，查看队列、任务耗时、失败重试 | `flower`（profile 启用，5555 端口） |
| Init | 一次性初始化：数据库迁移、创建管理员账号、目录与权限 | `airflow-init` |

**数据流**：dag-processor 解析 DAG → 元数据库存 DAG 结构；scheduler 按 schedule 与依赖判断触发点 → 通过 Celery 把任务投递到 Redis → worker 取任务执行 → 状态回写 Postgres；apiserver 从 Postgres 读取状态渲染 UI 与 REST 响应。

## 2. DAG 定义机制

- **DAG（有向无环图）**：在 Python 文件中实例化 `DAG(dag_id="email_daily_report", schedule="0 9 * * *")`，每个文件可包含一个或多个 DAG；`dag_id` 全局唯一。
- **Task（任务节点）**：本工程统一使用 `PythonOperator`，`python_callable` 指向 Skill 函数；任务之间通过 `>>` 声明依赖。
- **XCom（跨任务通信）**：`ti.xcom_push / xcom_pull` 在任务间传递可序列化的字典（本工程关闭 pickle，只传 JSON 原生类型）。
- **参数注入**：手动触发或 REST 触发时传入 `conf`，DAG 内通过 `dag_run.conf` 读取（见 `dags/_common.py::get_conf`）。
- **doc_md**：DAG 页面直接展示 Markdown 说明，录屏时可点开讲解流程。

对应代码：
- 线性依赖：`dags/01_email_daily_report_dag.py`
- 并行分支汇合：`dags/02_skill_pipeline_dag.py`（`[t_attach, t_render] >> t_send`）
- API 触发 DAG：`dags/03_llm_skill_dispatch_dag.py`（`schedule=None`）

## 3. 任务依赖

1. **线性链**：`prepare_report_data >> render_email_draft >> send_report_email >> archive_and_verify`，
   下游只有上游成功才会进入 `queued/running`。
2. **并行分支 + 汇合（join）**：`t_prepare >> [t_attach, t_render] >> t_send`；
   `t_send` 隐式 `trigger_rule=ALL_SUCCESS`，两条分支都成功才执行。
3. **兜底汇总**：`final_summary` 设置 `trigger_rule="all_done"`，无论上游成功、失败、跳过后都执行，
   用于记录任务状态，保证异常路径也有监控输出。
4. **`depends_on_past=False`**：任务不依赖上一次 DAG Run 同名任务的状态，适合独立日报任务。

## 4. 定时调度

| 配置 | 本工程取值 | 含义 |
| --- | --- | --- |
| `schedule` | `0 9 * * *` / `0 18 * * *` / `None` | cron 每天 09:00、18:00；None 表示仅手动/API 触发 |
| `start_date` | 2026-11-01 00:00（CST） | 调度起点，不会追跑起点之前的周期 |
| `catchup=False` | 关闭回填 | 启动时不补跑历史漏跑的周期 |
| `max_active_runs=1` | 单并发 | 同一 DAG 同时只允许一个 Run，避免日报重入 |
| `DEFAULT_TIMEZONE` | `Asia/Shanghai` | cron 按北京时间解释（compose 环境变量注入） |
| `DAGS_ARE_PAUSED_AT_CREATION=true` | 新建即暂停 | 由 `scripts/start.ps1` 启动后统一 unpause，避免脚本刚写完就被调度 |

> cron 五段：`分 时 日 月 周`。`0 9 * * *` = 每天 09:00:00。

## 5. 执行监控

### 5.1 Web UI（http://localhost:18080）
- **Grid / Graph / Tree**：查看每个任务实例的成功（绿）、失败（红）、重试（橙）、运行中（蓝）状态；
- 点开任务实例可看 **Logs**、**XCom**、**重试次数**、起止时间；
- DAG 详情页展示调度时间、下次运行时间、历史 Run 列表。

### 5.2 Flower（http://localhost:15555）
查看 Celery 队列、worker 在线状态、每个任务的执行耗时与重试历史。

### 5.3 状态快照落盘（本工程增强）
`dags/_common.py` 的 `on_success_callback / on_failure_callback` 把 DAG Run 元信息、
conf、各任务状态、异常摘要写入 `output/status/<dag_id>__<run_id>.json`；并行任务回调写入前
重读并合并旧快照、用临时文件原子替换，避免后写覆盖先写，即使不看 UI 也能在宿主机复盘。

### 5.4 REST API 监控（任务3 使用）
`llm_bridge/airflow_rest_client.py` 封装：JWT 认证、DAG 列表、DAG Run 创建/查询、
任务实例列表、Airflow 3 结构化任务日志拉取；网关轮询到终态后汇总状态与异常。
认证路由为根命名空间 `/auth/token`，DAG/Run/任务等业务接口位于 `/api/v2`。
意图层默认 DeepSeek 在线解析，网络异常或输出非法时自动切换规则兜底。

## 6. 异常处理机制（本工程落地清单）

| 机制 | 位置 | 作用 |
| --- | --- | --- |
| `retries=2` | `default_args` | 首次执行失败后再自动重试 2 次（共 3 次尝试） |
| `retry_delay=30s` + `retry_exponential_backoff=True` + `max_retry_delay=2min` | `default_args` | 指数退避：30s → 60s → …，封顶 2 分钟，避免风暴式重试 |
| `execution_timeout=10min` | `default_args` | 单任务超时强制失败，防止任务挂死占用 worker |
| `on_failure_callback` | `default_args` | 任务/DAG 失败写失败快照，记录异常类型与堆栈摘要 |
| `on_success_callback` | `default_args` | 成功写成功快照 |
| `trigger_rule=all_done` | `final_summary` | 异常路径下仍记录任务状态 |
| Skill 内部异常分类 | `skills/email_sender/sender.py` | 参数非法抛 `ValueError`（不重试）；SMTP 阶段抛 `EmailSkillError`（可重试）；凭据缺失自动降级 DRY_RUN |
| 归档交付校验 | `skills/archiver/archiver.py` | 上一条发送记录非成功状态直接 `RuntimeError`，防止“假成功”归档 |
| 网关侧异常回传 | `llm_bridge/orchestrator.py` | 轮询终态、识别失败任务、解析 Airflow 3 结构化日志并抽取异常类型、出错位置形成汇报 |

### 可现场演示的异常场景（录屏备用）
1. 故意填错误 SMTP 授权码 → `send_report_email` 先重试 2 次后变红，回调快照写入 `output/status/`；
2. 调用网关触发未注册 Skill → `route_execute` 抛 `SkillNotFoundError`，失败任务日志被网关回传；
3. 观察 Flower 中任务的重试记录与耗时变化。

## 7. Executor 选型说明

本工程使用 **CeleryExecutor**（官方 quick-start 默认）：scheduler 把任务分发给多台/多个
Celery worker 并发执行，适合演示分布式监控；单机课程场景也可换 LocalExecutor /
SequentialExecutor，区别在于并发度与是否需要 Redis/独立 worker。
