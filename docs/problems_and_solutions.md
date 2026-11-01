# 部署调试记录：已解决问题、关键决策与待解决难点

> 对应作业要求“自由探索，找出目前难以解决的问题”。第一部分是本次实际踩到并修复的问题；
> 第二部分是工程当前局限与后续可探索方向，录屏收尾时讲解。

## 一、已解决问题（实机调试记录）

### 1. 宿主机 8080 / 5555 端口无法绑定（Windows 动态端口保留区间）
- **现象**：容器内 api-server 健康检查正常、`docker port` 显示 `0.0.0.0:8080->8080`，
  但宿主机 `curl http://localhost:8080` 连接被拒绝（exit 7），`netstat` 无对应 LISTENING。
- **排查**：`netsh interface ipv4 show excludedportrange protocol=tcp` 发现 8080 落在
  Windows 动态保留区间 `8027–8126`；Hyper-V/WSL 端口代理无法把该区间端口发布到宿主机。
  另起一个 nginx 探针映射到 18080 可正常访问，证明 Docker 端口代理本身正常。
- **解决**：`docker-compose.yaml` 把宿主机端口改为 `18080:8080`、`15555:5555`；
  容器内部服务端口保持 8080/5555 不变，健康检查路径不受影响；
  网关 `AIRFLOW_BASE_URL`、README、演示脚本同步更新。

### 2. 本机已有 Redis 占用宿主机 6379
- **现象**：宿主机 6379 已被本地 Redis 监听。
- **解决**：compose 中 Redis 只使用 `expose: 6379`（Compose 内部网络互通），
  不发布到宿主机，避免端口冲突。

### 3. Airflow 3 的 `DagRun.get_task_instances()` 已移除
- **现象**：`final_summary` 任务重试 3 次后失败，异常
  `AttributeError: 'DagRun' object has no attribute 'get_task_instances'`。
- **原因**：Airflow 3 改用 pydantic SDK 模型，旧版 ORM DagRun 的实例遍历方法不再可用。
- **解决**：汇总任务改为通过上游任务 XCom 是否存在推断成功状态，不再跨模型查询实例列表；
  `_common.py` 的状态快照回调原本就把该查询包在 try/except 中，兼容不同版本上下文。

### 4. PythonOperator 导入路径废弃告警
- **现象**：日志出现 UserWarning：`airflow.operators.python.PythonOperator` 已废弃，
  建议使用 `airflow.providers.standard.operators.python.PythonOperator`。
- **解决**：三个 DAG 统一切换到新导入路径，保持任务日志干净。

### 5. 缺少 SMTP 凭据导致演示链路中断风险
- **决策**：邮件 Skill 在凭据为空时强制 DRY_RUN，渲染 `.eml` 落盘、写模拟发送记录，
  不触达网络；配置完整且 `EMAIL_DRY_RUN=false` 才走真实 SMTP。当前本地已填入 QQ SMTP
  完成宿主机 `SENT` 自测，DRY_RUN 保留为离线兜底。参数校验类错误直接抛 `ValueError`
  （不重试），SMTP 阶段错误包装为 `EmailSkillError` 交给 Airflow 重试。

### 6. 大模型在线优先与无 Key/异常兜底
- **决策**：`IntentParser` 大模型优先；本地 `llm_bridge/.env` 配置真实 DeepSeek Key 后默认走
  在线 `deepseek-chat`。无 Key、网络超时、Key 失效或输出不合法时自动切换
  `rule_fallback` 中文规则引擎，保证离线环境仍能完成“NL → DAG → 触发 → 监控”演示。
- **实测**：2026-11-01 在线 DeepSeek 完成邮件、并行、采集、归档、DAG03 `send_email` 五轮
  触发并执行成功，无关需求在 REST 调用前被拒绝；证据见 `docs/evidence/online-deepseek/`。

### 7. Airflow 3 认证路由与业务接口分属不同命名空间
- **现象**：网关 POST 令牌到 `/api/v2/auth/token` 返回 405 Method Not Allowed。
- **排查**：逐路径探测 `GET/POST` 状态码后确认：换取 JWT 的路由是根命名空间
  `/auth/token`（POST 返回 201 与 `access_token`）；DAG、DagRun、任务实例等业务接口
  才在 `/api/v2` 下，两个前缀不能混用。
- **解决**：`airflow_rest_client._authenticate` 固定请求 `/auth/token`，其余调用统一
  `/api/v2/dags`、`/api/v2/dags/{dag_id}/dagRuns` 等业务前缀；401 时清空本地 token 自动重认证并重试一次。

### 8. Airflow 3 创建 DAG Run 必填 `logical_date`（422 校验）
- **现象**：网关触发 `POST /api/v2/dags/{dag_id}/dagRuns` 只提交 `{"conf": {"to": "demo@example.com"}}`，
  返回 422，校验信息指明 `body.logical_date` 字段缺失。
- **原因**：Airflow 3 的 DagRun 创建 schema 强制 `logical_date`（UTC ISO8601），
  不再像 2.x 那样由服务端自动生成。
- **解决**：`trigger_dag` 在 payload 中带上当前 UTC 时间的 `logical_date`，`conf` 作为
  业务参数一并提交；触发后从响应读取 `dag_run_id` 进入轮询。

### 9. Windows 下 `localhost` 优先 IPv6，导致每次 REST 调用等待约 20 秒
- **现象**：接口本身可用，但网关日志显示认证后到触发之间固定间隔约 40 秒、每次调用
  约 20 秒；用 curl 直连却很快。
- **排查**：在宿主机分别计时 `localhost` 与 `127.0.0.1` 直连 `GET /api/v2/dags`：
  `localhost` 每次约 20 秒，`127.0.0.1` 约 0.02–0.1 秒；根因是 urllib 解析 `localhost`
  时优先尝试 `::1`（IPv6），Windows/Docker Desktop 该地址不可达后等待回退到 IPv4。
- **解决**：网关 `AIRFLOW_BASE_URL` 默认与配置示例改为 `http://127.0.0.1:18080`；
  浏览器访问 UI 仍可用 `localhost`。最近一次重建后实测：单次邮件耗时 9.54 秒、
  并行流水线 6.43 秒、单 Skill 调度约 3.37–3.44 秒。

### 10. 规则兜底不再“默认发邮件”，未识别需求直接拒绝触发
- **现象**：早期规则引擎在关键词全部未命中时兜底选择邮件 DAG，“订一张去火星票”
  这类无关需求也会被误触发。
- **解决**：`_choose_dag` 改为按归档 / 采集 / 并行 / 邮件四组信号显式匹配；都未命中时
  返回 `valid=False` 的拒绝意图，编排器在触发 REST 前就终止，从源头避免误触发未知工作流。
  新增对应单元测试 `test_unmatched_request_is_rejected`。

### 11. Airflow 3 任务日志路由改为路径参数，且日志是结构化事件
- **现象**：网关拉取失败任务日志时 `GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs?try_number=2`
  返回 422，报错把 `logs` 当成 `map_index` 路径段解析。
- **排查**：直接读取容器内 `api_fastapi/core_api/routes/public/log.py` 源码，确认 Airflow 3
  路由为 `/{task_id}/logs/{try_number}`，`try_number` 位于路径；JSON 响应的 `content`
  是日志事件数组，异常存于 `error_detail[0].exc_type / exc_value / frames`，
  不再是 2.x 纯文本 Traceback。
- **解决**：`get_task_log` 改用路径段传 `try_number` 并解析 `content`；
  `orchestrator._extract_traceback_tail` 同时兼容 JSON 与 Python dict 字面量，
  抽取异常类型、异常信息与出错文件行号，附加失败回调快照说明；新增
  `tests/test_failure_summary.py` 回归覆盖。

### 12. 运行中的调度器执行 CLI 删除历史 Run 导致调度挂起
- **现象**：在 scheduler 运行期间用 CLI `airflow dags delete` 清理历史 Run 后，
  scheduler 日志停在 “Adopting or resetting orphaned tasks for active dag runs”，
  后续周期不再调度，手动触发的 Run 一直 queued；重启 scheduler 未恢复。
- **解决**：使用 `scripts/reset.ps1`（`down --volumes` 清空元数据卷）配合
  `scripts/start.ps1` 全量重建后恢复。后续约定：需要清历史时优先全量重建或走 REST，
  不对运行中的 scheduler 执行破坏性删除 CLI。

### 13. 并发补跑时归档任务串用全局最后一条邮件记录
- **现象**：两个邮件 DAG 在重建后几乎同时补跑，`archiver` 原先读取 `send_records.jsonl`
  的最后一条作为当前 Run 的交付记录；并发场景下最后一条可能属于另一个 DAG，导致归档目录
  复制到不匹配的 `.eml` 草稿。
- **解决**：发送记录新增 `run_id`；邮件 DAG 归档时先按 `run_id / message_id` 精确匹配当前
  Run，匹配不到再以上游 XCom 中的 `send_result` 为准；DAG03 纯归档 Skill 保留校验最近一条
  发送记录的语义。新增 `tests/test_archiver_record_match.py` 回归覆盖并发匹配。

### 14. 并行任务状态快照回调发生读-改-写竞态
- **现象**：DAG02 的 `render_email_draft` 与 `prepare_attachment` 并行执行，两个成功回调同时
  读取旧状态快照后分别写回，后写覆盖先写，最终 `output/status/` 中只保留部分并行任务状态。
- **解决**：`_write_snapshot` 在落盘前重新读取已有快照并合并任务状态，再用临时文件 +
  `os.replace` 原子替换；已有 `failed / upstream_failed` 状态不会被后续成功任务覆盖。
  新增 `tests/test_status_snapshot_merge.py` 回归覆盖并行合并与失败保留。

## 二、架构与配置关键决策

| 决策点 | 选择 | 理由 |
| --- | --- | --- |
| 部署形态 | Docker Compose + CeleryExecutor（官方镜像 apache/airflow:3.3.2） | Windows 原生不支持 Airflow Scheduler；容器化可完整演示多组件架构 |
| 示例 DAG | `LOAD_EXAMPLES=false` | 界面只保留课程 DAG，演示清晰 |
| 时区 | `Asia/Shanghai` | cron 按北京时间解释，与口播一致 |
| 网关依赖 | 纯标准库 urllib | 宿主机免装第三方包，跨机器可直接运行 |
| 意图安全 | 注册表白名单校验 + 规则显式匹配拒绝 | 大模型只能选择已存在的 DAG/Skill；规则引擎未命中也不默认兜底，避免选择不存在的工作流或误触发 |
| 归档一致性 | 按 `run_id / message_id` 匹配当前发送记录 | 避免多个 DAG 并发运行时归档目录串用其他 Run 的草稿 |
| 状态快照合并 | 写入前重读快照并原子替换 | 避免并行任务回调互相覆盖，失败状态可长期保留 |
| 网关地址 | REST 使用 127.0.0.1，浏览器 UI 使用 localhost | 规避 Windows 下 localhost IPv6 不可达导致的每次连接约 20 秒回退等待 |

## 三、当前局限与尚未完全解决的难点（探索方向）

### 1. 自然语言修改定时规则还没有实现
现状：调度时间写在 DAG 的 `schedule` 参数里；网关只能触发“立即执行一次”。
难点：DAG schedule 属于拓扑定义，运行时动态修改需要走 DAG 版本管理 / REST 改参数 /
动态 DAG 生成，涉及调度器重解析与历史 Run 一致性。后续可扩展：大模型解析 cron 后，
调用 PATCH `/api/v2/dags/{dag_id}` 更新 schedule，或生成参数化动态 DAG。

### 2. 中文自然语言到 cron 的泛化
规则引擎目前只覆盖“每天 X 点”等简单表达；“每周一三五早上九点”“每月一号”“节假日除外”
等复杂周期需要 cron 语义库（如 cron-converter / 自然语言排程 DSL）配合大模型生成并校验。

### 3. 真实邮件内容个性化与附件能力
当前 HTML 模板为固定结构；附件只做了文本摘要占位。真实场景需要：大模型根据业务数据
生成个性化正文、多附件 MIME 组装、内嵌图片、抄送/密送、退信与收件人有效性校验。

### 4. 异常诊断的深度
网关目前已能解析 Airflow 3 结构化日志，抽取异常类型、异常信息与出错文件行号；尚未做：
错误自动归类（配置类/网络类/代码类）、修复建议生成、失败自动重放与幂等键去重、
跨 DAG 的级联失败根因定位。

### 5. 生产化差距
- 当前单机单 worker、管理员弱口令、无 RBAC 细分、密钥写在 .env；
- 缺少 DAG 版本发布评审、单元测试进 CI、指标接入 Prometheus/Grafana、告警推送（邮件/IM）；
- Celery 队列未按任务类型拆分，长任务与短任务共用队列存在互相阻塞风险。

### 6. 容器内文件权限
挂载目录由容器 UID 50000 写入，宿主机编辑偶发属主限制；课程演示不影响，
生产环境应使用命名卷或统一 UID 方案。

## 四、复现与排障命令
```powershell
docker compose ps                                   # 组件健康状态
docker compose logs -f airflow-scheduler            # 调度器日志
docker compose logs -f airflow-worker               # 任务执行日志
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags unpause <dag_id>
docker compose --profile flower down --volumes     # 清空重来

# 网关（宿主机；Python 仅依赖标准库；默认 DeepSeek 在线，--no-llm 为离线兜底）
python llm_bridge/cli.py --once "给 zhang@whu.edu.cn 张老师发一封进度汇报邮件"
python llm_bridge/cli.py                           # 交互模式；网络异常时可用 --no-llm 规则兜底
```

> 排障提示：若网关每个请求耗时约 20 秒，先检查 `AIRFLOW_BASE_URL` 是否写成
> `localhost`，改为 `127.0.0.1` 即可；认证 405 检查是否把 `/auth/token` 误写成
> `/api/v2/auth/token`；触发 422 检查 payload 是否缺少 `logical_date`；拉日志 422
> 检查 `try_number` 是否放在路径段 `/logs/{try_number}`，而不是查询参数。
