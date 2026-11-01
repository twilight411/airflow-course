# Airflow 课程作业

本仓库对应课程三项实践：部署 Airflow 工作流平台、把邮件 Skill 封装成定时 DAG、接入 DeepSeek 后用自然语言触发 Airflow 任务。

- 运行环境：Windows 11 + Docker Desktop + Apache Airflow 3.3.2
- 执行器：CeleryExecutor，数据库 Postgres，任务队列 Redis，监控面板 Flower
- 大模型：DeepSeek，走 OpenAI 兼容接口；网络或密钥异常时切换本地规则匹配
- 邮件：配置 QQ SMTP 后真实发送；未配置凭据时保存草稿并记录模拟发送结果
- 仓库地址：https://github.com/twilight411/airflow-course

## 一、目录结构

```text
airflow-course/
├── docker-compose.yaml              # Airflow 集群编排配置
├── docker-compose.orig.yaml         # 官方原始配置备份
├── .env / .env.example              # 本地配置与模板，真实 .env 不入库
├── dags/                            # Airflow DAG 定义
│   ├── _common.py                   # 公共函数、状态快照回调
│   ├── 01_email_daily_report_dag.py
│   ├── 02_skill_pipeline_dag.py
│   └── 03_llm_skill_dispatch_dag.py
├── skills/                          # 邮件、采集、归档 Skill
│   ├── email_sender/
│   ├── data_collector/
│   └── archiver/
├── llm_bridge/                      # 宿主机上的自然语言网关
│   ├── intent_registry.json         # 允许调用的 DAG/Skill 注册表
│   ├── config.py
│   ├── deepseek_client.py
│   ├── airflow_rest_client.py
│   ├── intent.py
│   ├── rule_fallback.py
│   ├── intent_parser.py
│   ├── orchestrator.py
│   └── cli.py
├── output/                          # 草稿、发送记录、归档、状态快照
├── logs/                            # 运行日志
├── tests/                           # 离线单元测试
├── scripts/                         # 启停、重置、测试和 SMTP 自测脚本
└── docs/                            # 学习笔记、演示清单、问题记录、架构图和验证材料
```

## 二、快速开始

### 1. 前置条件

先启动 Docker Desktop。本工程端口如下：

- Airflow UI：http://localhost:18080 ，账号 `airflow` / `airflow`
- Flower：http://localhost:15555
- 网关内部 REST 地址使用 `http://127.0.0.1:18080`，避免 Windows 下 `localhost` 优先走 IPv6 造成等待。

本机 6379 已被其他 Redis 占用，所以本工程 Redis 只在 Compose 内部网络暴露，不映射到宿主机。

### 2. 配置

- `llm_bridge/.env` 保存本地 `DEEPSEEK_API_KEY`，该文件已被 `.gitignore` 排除；`.env.example` 只保留空模板。
- 根目录 `.env` 填写 SMTP 账号、授权码和收件人，`EMAIL_DRY_RUN=false` 时真实发送；不填凭据或开启 DRY_RUN 时，邮件保存为 `.eml` 草稿并写入模拟记录。
- 仓库中的示例文件不包含真实密钥、授权码或邮箱密码。

### 3. 启动平台

```powershell
cd D:\airflow-course
./scripts/start.ps1
```

### 4. 运行离线测试

```powershell
./scripts/run_tests.ps1
```

### 5. SMTP 自测（可选）

```powershell
python scripts/smtp_selftest.py
```

脚本读取本地 `.env` 调用邮件 Skill 发送一封自测邮件，输出中不显示 SMTP 授权码。

### 6. 手动触发邮件 DAG

在 UI 中打开 `email_daily_report`，使用 Trigger DAG w/ config，填入：

```json
{
  "to": "teacher@example.com",
  "subject": "课程项目进度汇报",
  "recipient_name": "张老师"
}
```

### 7. 启动自然语言网关

```powershell
# 默认 DeepSeek 在线模式
python llm_bridge/cli.py

# 离线规则兜底模式
python llm_bridge/cli.py --no-llm

# 单次执行
python llm_bridge/cli.py --once "给张老师发一封项目进度汇报邮件"
```

可输入的例子：

- 给老师发一封项目进度汇报邮件，主题是 Airflow 课程作业进展
- 走并行流水线，准备附件摘要后发送晚间汇总
- 采集今天运行数据
- 把刚才的发送记录归档

和已注册能力无关的需求会被拒绝，不会创建 DAG Run。内置命令：`list` 查看能力目录，`status <dag_id> <run_id>` 查询运行状态，`exit` 退出。

## 三、作业要求与工程对应关系

| 作业要求 | 本工程实现 |
| --- | --- |
| 部署 Airflow，理解架构、DAG、依赖、调度、监控和异常处理 | `docker-compose.yaml` 启动 8 个组件；`docs/airflow_notes.md` 记录组件分工；三个 DAG 分别覆盖线性依赖、并行分支汇合和 API 触发；配置重试、退避、超时、回调和状态快照 |
| 集成定时邮件 Skill 并封装为 Airflow 任务 | `skills/email_sender/` 负责渲染、草稿、SMTP 发送和记录；采集、邮件、归档 Skill 分别封装成 `PythonOperator`；配置 cron、状态监控、失败回调和归档校验 |
| 接入大模型，完成自然语言到任务选择和工作流触发 | `llm_bridge/` 完成意图解析、注册表校验、REST 触发、状态轮询和失败日志收集；在线模型不可用时由规则匹配兜底 |

## 四、实现要点（录屏时可以重点讲）

1. **Skill 拆分和复用**：参考 SkillFabri 的输入、输出和关系说明方式编写 `SKILL.md`；邮件、采集、归档分别封装，不同 DAG 可以复用同一套 Skill。
2. **邮件 Skill 两种运行状态**：配置 SMTP 后真实发送；缺少凭据时自动进入 DRY_RUN，保存草稿和模拟记录，保证课堂演示不会因为缺少密钥中断。
3. **注册表限制可选范围**：`intent_registry.json` 列出允许使用的 DAG、Skill、参数和示例；模型只能选择注册表中已经存在的能力，未注册需求不会被触发。
4. **在线模型加规则兜底**：DeepSeek 负责日常自然语言理解；无 Key、超时或返回格式不合法时，中文规则引擎继续完成演示。
5. **异常处理**：Airflow 层配置重试、退避、超时和成功/失败回调；网关轮询到结束状态后，读取失败任务日志并整理成可读汇报。
6. **网关依赖较少**：大模型客户端和 REST 客户端只使用 Python 标准库，宿主机不需要额外安装 requests、openai 等第三方包。

`skill_pipeline_evening` 是拓展 DAG：数据准备后，邮件渲染和附件摘要并行执行，两条分支都完成后再发送和归档。

## 五、常用命令

```powershell
docker compose ps                                   # 查看组件状态
docker compose logs -f airflow-scheduler            # 查看调度器日志
docker compose exec airflow-scheduler airflow dags list
./scripts/reset.ps1                                 # 清空元数据和运行产物后重建
```
