# 录屏演示清单（按顺序操作，每一步对应口播稿段落）

> 环境前置：Docker Desktop 已启动；`D:\DoubaoWork\airflow-course`；已执行 `./scripts/start.ps1`。
> 建议录屏工具：OBS / Win11 自带 Xbox Game Bar（Win+G），浏览器固定 125% 缩放，关闭无关弹窗。

## 阶段 0：开场（0.5 分钟）
- [ ] 展示项目目录树（`README.md`、`dags/`、`skills/`、`llm_bridge/`、`docs/`）
- [ ] 一句话说明三个作业任务与整体链路图（docs/architecture.svg）

## 阶段 1：Airflow 平台部署与架构讲解（3 分钟）—— 任务 1
- [ ] `docker compose ps`：逐个指出 api-server、scheduler、dag-processor、triggerer、worker、postgres、redis、flower 八个组件状态 healthy
- [ ] 浏览器打开 http://localhost:18080 ，用 airflow/airflow 登录
- [ ] 展示 DAG 列表中三个课程 DAG（无官方示例 DAG，说明 `LOAD_EXAMPLES=false`）
- [ ] 打开 `email_daily_report` 的 **Graph** 视图，指认四个业务任务节点与 `final_summary` 兜底节点、依赖箭头
- [ ] 打开 **Code** 标签，对照讲解 DAG 定义、cron 调度、retries/回调配置
- [ ] 打开 Flower（http://localhost:15555）展示 worker 与队列监控
- [ ] 打开任意任务历史实例的 Logs、XCom 标签，说明监控手段

## 阶段 2：Skill 集成与定时邮件工作流（4 分钟）—— 任务 2
- [ ] 打开 `skills/email_sender/SKILL.md`，讲输入输出字段和 Skill 之间的关系（参考 SkillFabri 写法）
- [ ] 打开 `skills/email_sender/sender.py`，讲渲染、保存草稿、SMTP 发送、追加记录；未配置凭据或显式 DRY_RUN 时的离线降级逻辑
- [ ] 打开 `dags/01_email_daily_report_dag.py`，讲三个 Skill 如何被封装为 PythonOperator
- [ ] UI 手动 Trigger DAG w/ config，输入：
  ```json
  {"to": "demo-recipient@example.com", "subject": "课程项目进度汇报", "recipient_name": "张老师"}
  ```
- [ ] 在 Grid/Graph 视图观察任务依次变绿；点开 `send_report_email` 日志展示 `SENT`（离线兜底时为 `DRY_RUN_OK`）
- [ ] 宿主机打开 `output/drafts/` 查看 `.eml` 草稿；`output/send_records.jsonl` 查看发送记录；
      `output/archive/` 查看按当前 `run_id` 归档的记录；`output/status/` 查看包含全部任务状态的成功快照
- [ ] 切换到 `skill_pipeline_evening` Graph 视图，讲并行分支与汇合依赖
- [ ] （可选异常演示）触发 DAG03 并传入未注册 Skill，观察 Airflow 重试两次后失败、失败回调写入状态快照；网关侧拉取结构化日志并回传异常摘要

## 阶段 3：大模型自然语言驱动编排（4 分钟）—— 任务 3
- [ ] 打开 `llm_bridge/intent_registry.json`，讲“注册表约束大模型只能选择已有 DAG/Skill”
- [ ] 打开 `llm_bridge/` 各文件：`deepseek_client.py`、`intent_parser.py`、`airflow_rest_client.py`、
      `orchestrator.py`、`cli.py`，讲各文件分工
- [ ] 终端运行 `python llm_bridge/cli.py`（默认 DeepSeek 在线；备用离线兜底命令为 `python llm_bridge/cli.py --no-llm`）
- [ ] 依次输入：
  1. `给 zhang@whu.edu.cn 张老师发一封项目进度汇报邮件，主题是 Airflow 课程作业进展`
  2. `走并行流水线，准备附件摘要后给 team@example.com 发晚间汇总`
  3. `采集今天运行数据`
  4. `把刚才的发送记录归档整理一下`
- [ ] 每次展示网关输出：意图来源、置信度、目标 DAG、调度参数、run_id、任务状态时间线、耗时
- [ ] 再输入 `帮我订一张去火星的火箭票`，展示未匹配注册能力时被直接拒绝、未生成 DAG Run
- [ ] 回到 Airflow UI 刷新，展示网关注入触发的 DAG Run 与任务状态
- [ ] 终端 `list` 展示能力目录；`status email_daily_report <run_id>` 演示状态查询
- [ ] （可选异常演示）触发 DAG03 并传入 `{"skill": "not_registered_skill"}`，观察失败任务、重试记录与网关回传的异常类型、出错文件行号和失败快照说明

## 阶段 4：实现要点与问题总结（2 分钟）
- [ ] 对照 README“实现要点”逐条讲
- [ ] 打开 `docs/problems_and_solutions.md`，讲部署中遇到的问题与解决方式、当前局限与后续方向
- [ ] 收尾：按自然语言、意图识别、REST 触发、DAG、Skill 执行、状态监控和异常回传的顺序回放一遍

## 录屏前自检
- [ ] 三个 DAG 均已 unpause；`output/` 目录干净或已有成功产物
- [ ] 终端字号调大；浏览器书签栏收起；系统时间显示北京时间
- [ ] 本地 QQ SMTP 已先做本人自测；录屏前确认 `.env` 模式，真实发送讲 `SENT`，离线兜底讲 `DRY_RUN_OK`
- [ ] 准备好离线兜底命令：`python llm_bridge/cli.py --no-llm`
