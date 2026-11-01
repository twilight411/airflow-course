# Airflow + Skill + 大模型自然语言编排 —— 课程实践工程

> 作业三选一/全做：① 部署 Airflow 工作流平台；② 集成 Skill 编排自动化邮件工作流；
> ③ 接入大模型，自然语言驱动 DAG 触发、监控与异常回传。
>
> 运行环境：Windows 11 + Docker Desktop 4.38（Docker Compose v2.32）+ Apache Airflow 3.3.2；
> LLM：DeepSeek（OpenAI 兼容协议）；邮件：SMTP（QQ/163/Gmail 均可），支持 DRY_RUN 离线演示。

---

## 一、目录结构

```
airflow-course/
├── docker-compose.yaml          # 裁剪定制的官方 Airflow 集群编排（CeleryExecutor）
├── docker-compose.orig.yaml      # 官方原始文件备份，便于对照
├── .env / .env.example          # 平台、邮件、LLM 配置（.env 私有不入库）
├── dags/                         # Airflow DAG 定义（挂载进容器 /opt/airflow/dags）
│   ├── _common.py                # 路径引导、成功/失败状态快照回调
│   ├── 01_email_daily_report_dag.py    # 任务2 主 DAG：三 Skill 线性邮件流水线
│   ├── 02_skill_pipeline_dag.py        # 拓展 DAG：并行分支 + 汇合依赖
│   └── 03_llm_skill_dispatch_dag.py    # 任务3 执行侧：LLM 选择 Skill 的通用调度 DAG
├── skills/                       # 可复用 Skill 代码包（挂载进容器 /opt/airflow/skills）
│   ├── email_sender/             # 邮件 Skill：SKILL.md 建模 + 渲染/SMTP/落盘/记录
│   ├── data_collector/           # 数据采集 Skill：生成日报上下文
│   └── archiver/                 # 归档校验 Skill：交付校验 + 产物归档 + 索引
├── llm_bridge/                   # 任务3 宿主机网关：大模型 ↔ Airflow REST API
│   ├── intent_registry.json       # 可调度 DAG/Skill 能力注册表（LLM 只能选本表项）
│   ├── config.py                  # .env 加载与配置对象
│   ├── deepseek_client.py         # DeepSeek OpenAI 兼容客户端（stdlib urllib）
│   ├── airflow_rest_client.py     # JWT 认证、触发、轮询、任务日志采集
│   ├── intent.py                  # Intent 数据结构与注册表工具
│   ├── rule_fallback.py           # 无 Key/解析失败时的中文规则兜底引擎
│   ├── intent_parser.py           # 大模型优先、规则兜底的意图解析器
│   ├── orchestrator.py            # 编排：解析→校验→触发→监控→异常汇总
│   └── cli.py                     # 交互式自然语言命令行入口
├── output/                       # 邮件草稿、发送记录、归档、状态快照（容器挂载）
├── logs/                         # Airflow 调度器/任务运行日志
├── tests/                        # 宿主机离线单元测试
├── scripts/                      # 一键启停 / 重置 / 测试 / SMTP 自测脚本
└── docs/                         # 学习笔记、演示清单、录屏口播稿、问题记录、架构图、验证报告与证据留档
```

## 二、快速开始

### 1. 前置条件
- Docker Desktop 已启动（WSL2 后端）；
- 浏览器访问 UI：`http://localhost:18080`、`http://localhost:15555`；
  网关 REST 默认走 `http://127.0.0.1:18080`（规避 Windows 下 localhost 优先 IPv6 导致的
  连接等待）；本机已有本地 Redis 占用 6379，本工程 Redis 仅暴露在 Compose 内部网络，
  不占用宿主机端口，无冲突。

### 2. 配置
- 本地 `llm_bridge/.env` 已填入 `DEEPSEEK_API_KEY`；该文件被 `.gitignore` 排除，
  密钥不会写入 `.env.example`、文档或提交包。网关默认 DeepSeek 在线优先，无 Key、
  超时或输出非法时自动切换规则兜底引擎，链路照常演示。
- 邮件真实发送：根目录 `.env` 填写 `EMAIL_SMTP_USER / EMAIL_SMTP_PASSWORD /
  EMAIL_SMTP_FROM / EMAIL_DEFAULT_TO`，并把 `EMAIL_DRY_RUN` 改为 `false`；本工程本地已完成
  QQ SMTP 本人自测。模板 `.env.example` 只保留空字段，不包含授权码；未填凭据或显式开启
  DRY_RUN 时，Skill 自动把邮件渲染为 `.eml` 落盘并写模拟发送记录。

### 3. 启动平台
```powershell
cd D:\DoubaoWork\airflow-course
./scripts/start.ps1
```
- Airflow UI：<http://localhost:18080> （airflow / airflow）
- Flower 任务监控：<http://localhost:15555>

### 4. 运行单元测试（离线，不依赖容器）
```powershell
./scripts/run_tests.ps1
```

### 5. 独立 SMTP 自测（可选，不启动 Airflow）
```powershell
python scripts/smtp_selftest.py
```
脚本读取本地私有 `.env`，直接调用邮件 Skill 完成一封真实自测邮件；输出和 `logs/smtp-selftest.json` 不包含授权码。

### 6. 手动触发邮件 DAG
UI 中打开 `email_daily_report` → 右侧 ▷ Trigger DAG w/ config，填入：
```json
{ "to": "teacher@example.com", "subject": "课程项目进度汇报", "recipient_name": "张老师" }
```

### 7. 启动 LLM 自然语言网关
```powershell
python llm_bridge/cli.py
# 在线 DeepSeek 模式（默认）；离线规则兜底：python llm_bridge/cli.py --no-llm
# 单句一次性：python llm_bridge/cli.py --once "给张老师发一封项目进度汇报邮件"
```
对话示例：
- “给 zhang@whu.edu.cn 张老师发一封项目进度汇报邮件，主题是 Airflow 课程作业进展”
- “走并行流水线，准备附件摘要后给 team@example.com 发晚间汇总”
- “采集今天运行数据”
- “把刚才的发送记录归档”
- 与已注册能力无关的需求会被直接拒绝，不会触发任何 DAG；
  内置命令：`list` 查看能力目录；`status <dag_id> <run_id>` 查询运行状态；`exit` 退出。

## 三、三个作业任务对应关系

| 作业要求 | 本工程落地 |
| --- | --- |
| ① 部署 Airflow、理解架构/DAG/依赖/调度/监控/异常 | `docker-compose.yaml` 八组件集群；`docs/airflow_notes.md` 知识点闭环；三套 DAG 覆盖线性依赖、并行分支汇合、API 触发；重试/退避/超时/回调/`trigger_rule=all_done`/状态快照 |
| ② 集成定时邮件 Skill 并封装为 Airflow Task 编排 DAG | `skills/email_sender/SKILL.md` 按 SkillFabri 规范建模；三 Skill 封装为 `PythonOperator`；定时 cron、状态监控、失败回调、归档校验 |
| ③ Python 接入大模型、建立 LLM↔Airflow 接口、NL→任务选择→工作流触发全链路 | `llm_bridge/`：DeepSeek 结构化意图解析 → 注册表校验 → REST 触发 → 轮询监控 → 失败任务日志回传；规则兜底保证离线可用 |

## 四、创新点（录屏重点讲，详见 docs/recording_script.md）
1. **Skill 标准化建模 + 可复用编排**：参照 SkillFabri 的“输入/输出契约 + 技能关系图谱”
   方式定义 SKILL.md，三个 Skill 与 DAG 解耦，可被多个 DAG 与 LLM 调度器复用。
2. **双模式邮件 Skill**：真实 SMTP 与 DRY_RUN 自动降级，凭据缺失不炸演示；
   草稿 `.eml`、发送记录 jsonl、归档目录全程可追溯，并发归档按 `run_id / message_id` 匹配当前 Run。
3. **注册表白名单约束，防止大模型幻觉**：`intent_registry.json` 限定 LLM 只能选择已存在的
   DAG/Skill、参数 schema 与示例；规则引擎未命中时直接拒绝触发，不创建未知 DAG Run。
4. **大模型 + 规则双引擎意图层**：LLM 负责开放语言理解，规则引擎在无 Key、超时或输出
   不合法时兜底，离线环境仍可完整演示。
5. **完整闭环的异常处理**：Airflow 侧重试/退避/超时/失败回调；网关侧轮询终态、
   自动解析 Airflow 3 结构化日志，抽取异常类型、出错位置并形成人类可读异常汇报。
6. **零第三方依赖网关**：LLM 与 REST 客户端全部使用 Python 标准库，跨机器免装包运行；
   与容器内标准库 Skill 保持同一运行时语义。

**拓展探索**：`skill_pipeline_evening` 在主 DAG 线性依赖之外，演示
“渲染 ∥ 附件准备 → 汇合发送”的并行分支依赖与 join 语义。

## 五、常用命令
```powershell
docker compose ps                                   # 查看组件健康状态
docker compose logs -f airflow-scheduler            # 调度器日志
docker compose exec airflow-scheduler airflow dags list
docker compose --profile flower down                # 停止（保留数据）
./scripts/reset.ps1                                 # 清空元数据与产物重来
```
