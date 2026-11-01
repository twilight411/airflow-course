# 运行验证报告（录屏前自查留档）

> 验证时间：2026-11-01（北京时间）
> 工程路径：`D:\DoubaoWork\airflow-course`
> 运行形态：Docker Compose + CeleryExecutor，Apache Airflow 3.3.2；LLM 网关运行在宿主机，仅使用 Python 标准库。

## 1. 环境基线

| 项目 | 取值 / 结果 |
| --- | --- |
| 宿主系统 | Windows 11 + Docker Desktop 4.38，Docker Engine 27.5.1，Compose v2.32.4 |
| Airflow 镜像 | `apache/airflow:3.3.2` |
| 元数据库 / Broker | `postgres:16`、`redis:7.2-bookworm` |
| Web UI | `http://localhost:18080`，账号 `airflow / airflow` |
| Flower | `http://localhost:15555` |
| 网关 REST 地址 | `http://127.0.0.1:18080`（不用 `localhost`，规避 Windows IPv6 回退等待） |
| 邮件模式 | 根目录 `.env` 已配置 QQ SMTP 并完成宿主机独立自测，结果 `SENT`；未填凭据或显式 `EMAIL_DRY_RUN=true` 时自动落盘模拟发送兜底 |
| LLM 模式 | 本地 `llm_bridge/.env` 已配置真实 `DEEPSEEK_API_KEY`；DeepSeek 在线优先，无 Key、超时或输出非法时自动降级规则兜底；密钥仅存于被忽略的本地配置文件 |

## 2. 离线单元测试（不依赖容器）

运行脚本：`./scripts/run_tests.ps1`（Python：`D:\ProgramData\anaconda3\python.exe`）。

| 测试文件 | 覆盖内容 | 结果 |
| --- | --- | --- |
| `tests/test_email_skill.py` | DRY_RUN 草稿落盘、发送记录追加、非法邮箱抛 `ValueError`、缺凭据强制降级 | 通过 |
| `tests/test_archiver_record_match.py` | 邮件 DAG 按 `run_id / message_id` 匹配当前记录；DAG03 归档分支保留最近记录兜底语义 | 通过 |
| `tests/test_status_snapshot_merge.py` | 并行任务状态快照写入前重读合并，互不覆盖；失败状态不被后续成功覆盖 | 通过 |
| `tests/test_rule_parser.py` | 邮件 / 并行 / 归档 / 采集 / 重点事项 / 定时 cron 解析；未匹配需求拒绝 | 通过 |
| `tests/test_registry_consistency.py` | 注册表 DAG 与 `dags/` 文件一致；DAG03 注册 Skill 与网关注册表一致 | 通过 |
| `tests/test_failure_summary.py` | Airflow 3 结构化异常事件解析；纯文本 Traceback 回退 | 通过 |

最终命令输出：

```text
test_email_skill.py 全部通过
test_archiver_record_match.py 全部通过
test_status_snapshot_merge.py 全部通过
test_rule_parser.py 全部通过
test_registry_consistency.py 全部通过
test_failure_summary.py 全部通过
```

## 3. DeepSeek 在线联调结果（2026-11-01 补测）

在线配置：`DEEPSEEK_BASE_URL=https://api.deepseek.com`，`DEEPSEEK_MODEL=deepseek-chat`，
走 OpenAI 兼容 `/chat/completions` 并强制 `response_format=json_object`。下列需求均由
`llm_bridge/cli.py` 默认在线模式发起，未加 `--no-llm`；日志原件保存在
`docs/evidence/online-deepseek/`。录屏前执行 `reset.ps1` 会清空容器历史 Run，但不删除本目录证据。

| 自然语言需求 | 意图来源 | 目标 DAG / Skill | Run ID | 状态 | 耗时 | 证据日志 |
| --- | --- | --- | --- | --- | --- | --- |
| 给 zhang@whu.edu.cn 张老师发一封项目进度汇报邮件，主题是 Airflow 课程作业进展 | `deepseek`，置信度 0.95 | `email_daily_report` / `send_email` | `manual__2026-11-01T11:12:08.379078+00:00` | 5 任务 success | 7.77s | `online-email.txt` |
| 走并行流水线，准备附件摘要后给 team@example.com 发晚间汇总 | `deepseek`，置信度 0.95 | `skill_pipeline_evening` | `manual__2026-11-01T11:12:22.522365+00:00` | 5 任务 success | 7.57s | `online-parallel.txt` |
| 采集今天运行数据 | `deepseek`，置信度 0.90 | `llm_skill_dispatch` / `collect_data` | `manual__2026-11-01T11:12:36.347670+00:00` | `route_execute` success | 4.93s | `online-collect.txt` |
| 把刚才的发送记录归档整理一下 | `deepseek`，置信度 0.85 | `llm_skill_dispatch` / `archive` | `manual__2026-11-01T11:12:48.193075+00:00` | `route_execute` success | 4.62s | `online-archive.txt` |
| 调用 `send_email` Skill 给 ops@example.com 发一条网关在线联调通知 | `deepseek`，置信度 0.90 | `llm_skill_dispatch` / `send_email` | `manual__2026-11-01T11:13:25.098891+00:00` | `route_execute` success | 4.54s | `online-dispatch-send-email.txt` |

在线链路验证结论：

1. DeepSeek 能完成邮箱、称呼、主题、Skill 名称与 DAG 路由抽取；
2. 输出先经过 `intent_registry.json` 白名单交叉校验，不允许选择未注册 DAG/Skill；
3. 五轮在线触发均被 Airflow 正常调度到终态 success；DAG03 三个注册 Skill 均已在线覆盖；
4. 无关需求“帮我订一张去火星的火箭票”未得到可注册 DAG，解析器切换规则兜底，规则未命中后
   在发起 Airflow REST 请求前拒绝，未创建 DAG Run，证据为 `online-rejection.txt`；
5. 本轮在线验证覆盖 **LLM → 意图校验 → Airflow 触发 → 状态轮询回传** 链路；当时邮件仍处于
   `DRY_RUN`。后续已补充 QQ SMTP 宿主机独立自测，发件人与收件人均为本地配置邮箱，结果为 `SENT`，
   自测记录保存在被 `.gitignore` 排除的 `logs/smtp-selftest.json`。

## 4. Airflow 集群规则模式验证结果（录屏前已重置）

> 下列 Run ID 是正式录屏前在重建集群上完成规则模式验证时产生的历史记录；验证结束后已执行
> `scripts/reset.ps1` 清空元数据与产物，录屏现场会重新触发新的演示 Run。

### 4.1 邮件意图 → `email_daily_report`

需求：

> 给 zhang@whu.edu.cn 张老师发一封项目进度汇报邮件，主题是 Airflow 课程作业进展

- 目标 DAG：`email_daily_report`
- 抽取参数：`to=zhang@whu.edu.cn`、`recipient_name=张老师`、`subject=Airflow 课程作业进展`
- Run ID：`manual__2026-11-01T10:55:42.586245+00:00`
- 最终状态：`success`
- 任务链：`prepare_report_data → render_email_draft → send_report_email → archive_and_verify → final_summary`，5 个任务全部 `success`
- 网关耗时：9.54 秒

### 4.2 并行流水线意图 → `skill_pipeline_evening`

需求：

> 走并行流水线，准备附件摘要后给 team@example.com 发晚间汇总

- 目标 DAG：`skill_pipeline_evening`
- Run ID：`manual__2026-11-01T10:55:52.609566+00:00`
- 最终状态：`success`
- 任务状态：`prepare_report_data`、`prepare_attachment`、`render_email_draft`、`send_report_email`、`archive_and_verify` 全部 `success`；两个并行分支均成功后汇合发送
- 网关耗时：6.43 秒

### 4.3 采集意图 → `llm_skill_dispatch / collect_data`

需求：

> 采集今天运行数据

- 目标 DAG：`llm_skill_dispatch`
- Skill：`collect_data`
- Run ID：`manual__2026-11-01T10:55:59.508943+00:00`
- 最终状态：`success`，`route_execute` 成功
- 网关耗时：3.37 秒

### 4.4 归档意图 → `llm_skill_dispatch / archive`

需求：

> 把刚才的发送记录归档整理一下

- 目标 DAG：`llm_skill_dispatch`
- Skill：`archive`
- Run ID：`manual__2026-11-01T10:56:03.435549+00:00`
- 最终状态：`success`，`route_execute` 成功
- 网关耗时：3.44 秒

### 4.5 DAG03 `send_email` 分支自检

除规则演示使用的 `collect_data`、`archive` 外，另通过 REST 直接触发 `llm_skill_dispatch`，`conf.skill=send_email`：

- Run ID：`manual__2026-11-01T10:56:22.971883+00:00`
- 最终状态：`success`，`route_execute` 成功。

至此 DAG03 注册表三个 Skill 分支（`send_email`、`collect_data`、`archive`）均已在重建后的集群实机验证。

### 4.6 未注册 Skill 异常处理验证

通过 REST 触发 `llm_skill_dispatch`，传入 `skill=not_registered_skill`：

- Run ID：`manual__2026-11-01T10:56:36.003711+00:00`
- Airflow 状态：`failed`，`route_execute` 第 2 次尝试后失败（DAG03 `retries=1`，共两次尝试）
- 失败回调写入 `output/status/` 状态快照；
- 网关拉取 Airflow 3 结构化日志并解析出：

```text
SkillNotFoundError: 未注册的 Skill: 'not_registered_skill'；可选: ['archive', 'collect_data', 'send_email']
  出错位置: /opt/airflow/dags/03_llm_skill_dispatch_dag.py:60 (route_execute)
```

### 4.7 无关需求拒绝

需求：

> 帮我订一张去火星的火箭票

- 规则引擎未命中任何注册能力，返回 `valid=false`；
- 编排器在发起 Airflow REST 请求前终止，未创建 DAG Run；
- 错误信息：`未识别到与已注册 DAG/Skill 匹配的需求；可输入 list 查看当前支持的任务范围`。

### 4.8 内置查询命令

- `list`：正常列出三张能力目录卡片；
- `status email_daily_report scheduled__2026-11-01T01:00:00+00:00`：返回 Run 状态 `success` 与 5 个任务状态列表。

## 5. 录屏前干净基线

正式录屏前执行：

```powershell
cd D:\DoubaoWork\airflow-course
./scripts/reset.ps1
./scripts/start.ps1
```

`reset.ps1` 会停止容器、删除元数据卷、清空 `output/` 与 `logs/` 中的历史运行产物；`start.ps1` 重新初始化数据库、启动八个服务组件并 unpause 三个课程 DAG。清空重建后，调度器会自动补出当日两个定时 Run：

- `email_daily_report / scheduled__2026-11-01T01:00:00+00:00`：success，5 任务 success；
- `skill_pipeline_evening / scheduled__2026-11-01T10:00:00+00:00`：success，5 任务 success；
- `llm_skill_dispatch` 无定时 Run，仅接受 API / 手动触发；
- 两个基线归档目录均按当前 `run_id / message_id` 匹配发送记录，并发补跑时不会串用其他 DAG 的草稿；
- 两个基线邮件发送记录均为 `SENT`、`mode=smtp`，SMTP 账号、授权码和收件人只存本地被忽略的 `.env`；
- 两个基线状态快照均保留全部任务状态，DAG02 的并行分支回调不会互相覆盖状态记录。

录屏现场再通过网关依次触发邮件、并行、采集、归档四个需求，即可形成新的演示 Run；失败探针可作为可选异常演示。

## 6. 当前口径与剩余局限（录屏口播需如实说明）

1. **DeepSeek 在线路径已完成真实联调**：本地配置真实 Key 后，默认不加 `--no-llm` 即走在线大模型；
   若现场网络超时、Key 失效或模型返回非法 JSON，解析器会自动切换规则引擎，不中断演示。
2. **QQ SMTP 已完成宿主机自测与容器基线验证**：根目录 `.env` 已填发件邮箱、SMTP 授权码和
   默认收件人，`EMAIL_DRY_RUN=false`；独立 Skill 调用与重建后的两个容器基线邮件均返回 `SENT`。
   授权码是邮箱后台生成的 SMTP 专用密码，不是邮箱登录密码。未配置凭据或显式开启 DRY_RUN 时，
   Skill 仍会自动落盘模拟发送，保证链路可离线演示。

## 7. 录屏对应材料

- 口播稿：`docs/recording_script.md`
- 操作清单：`docs/demo_checklist.md`
- 架构图：`docs/architecture.svg`
- 任务 1 学习笔记：`docs/airflow_notes.md`
- 调试问题与解决记录：`docs/problems_and_solutions.md`
- DeepSeek 在线联调证据：`docs/evidence/online-deepseek/`
- 最终干净基线证据：`docs/evidence/clean-baseline/`
