# 录屏口播稿（约 12–15 分钟，可按现场情况裁剪）

> 左屏放代码和目录，右屏放 Airflow UI 与终端；【画面】是操作提示，【口播】是要讲的内容。

## 0. 开场（0:00–0:45）

【画面】打开项目根目录 `D:\DoubaoWork\airflow-course`，依次展开 `dags/`、`skills/`、`llm_bridge/`、`docs/`。

【口播】
“老师好，这次课程作业我完成了三部分：第一，基于 Apache Airflow 3.3.2，用 Docker Compose
部署 CeleryExecutor 集群；第二，把邮件发送整理成独立 Skill，再封装成定时 DAG；第三，接入
DeepSeek，让自然语言经过网关解析后触发 Airflow 工作流，并查看执行状态和异常信息。整体过程是：
用户输入一句话，网关识别意图，通过 REST API 触发 DAG，Worker 执行 Skill，网关再轮询运行结果。”

## 1. 任务一：Airflow 平台部署与架构（0:45–3:30）

【画面】终端执行 `docker compose ps`，逐个指出组件状态 healthy。

【口播】
“现在运行的有八个组件：api-server 提供 Web UI 和 REST API；scheduler 负责调度判断；
dag-processor 解析 DAG 文件；triggerer 处理可延迟任务；celery worker 真正执行任务；
postgres 保存元数据；redis 是 Celery 消息队列；flower 用于监控任务队列和耗时。
Scheduler 不直接执行任务，它把任务投递到 Redis，Worker 取走执行，状态再写回 Postgres，
api-server 统一对外提供查询。”

【画面】浏览器打开 http://localhost:18080 ，用 airflow/airflow 登录；DAG 列表展示三个课程 DAG。

【口播】
“我关闭了官方示例 DAG，界面只保留本工程的三个工作流。平台配置是在官方 docker-compose
基础上裁剪的，里面固定了北京时间时区、关闭历史回填，并限制同一个 DAG 的并发运行数。”

【画面】打开 `email_daily_report` 的 Graph，再切到 Code 标签。

【口播】
“这个 DAG 的业务任务用 `>>` 串成线性依赖，另外有一个 `final_summary` 兜底任务，
设置 `trigger_rule=all_done`，上游失败后它也会执行。`schedule='0 9 * * *'` 表示每天早上九点
定时运行；`retries=2` 配合退避重试；`execution_timeout` 防止任务长时间挂死；成功和失败都会
回调写状态快照到 output 目录。”

【画面】打开 Flower http://localhost:15555 ，点开历史任务实例的 Logs、XCom 标签。

【口播】
“监控主要看三处：UI 的 Grid/Graph 看任务状态，Flower 看 Worker 队列和耗时，REST API 供网关
外部查询；任务之间通过 XCom 传递结构化数据。”

## 2. 任务二：Skill 集成与邮件工作流（3:30–7:30）

【画面】打开 `skills/email_sender/SKILL.md`。

【口播】
“第一个实现要点是把 Skill 单独整理出来。我参考 SkillFabri 的写法，在 SKILL.md 里说明这个 Skill
做什么、输入字段有哪些、返回什么结果，以及它和数据采集、归档 Skill 的关系。这样 Skill 和 DAG
分开，不同工作流都可以复用。”

【画面】打开 `skills/email_sender/sender.py`，定位 `render_report`、`save_draft`、`send_message`、`run_email_task`。

【口播】
“邮件 Skill 先渲染纯文本和 HTML 两种正文，保存 .eml 草稿，再走 SMTP 真实发送；如果授权码
没有配置，或者显式开启 DRY_RUN，就自动改成离线模拟发送，把邮件落盘并写模拟记录，避免课堂上
因为缺少密钥中断演示。异常分成两类：参数格式不对直接失败，不重试；SMTP 阶段失败包装成可重试
异常，交给 Airflow 的重试机制处理。”

【画面】打开 `dags/01_email_daily_report_dag.py` 讲依赖链；再回到 UI 手动 Trigger DAG w/ config：

```json
{"to": "demo-recipient@example.com", "subject": "课程项目进度汇报", "recipient_name": "张老师"}
```

【口播】
“三个 Skill 分别被封装成 PythonOperator：data_collector 准备数据，email_sender 渲染并发送，
archiver 做归档校验。现在手动触发一次，传入收件人和主题。”

【画面】观察 Graph 任务依次变绿；点开 `send_report_email` 日志；在宿主机依次打开
`output/drafts/`、`output/send_records.jsonl`、`output/archive/`、`output/status/`。

【口播】
“任务按依赖顺序执行。当前配置 QQ SMTP 时日志状态是 `SENT`；系统同时保留草稿、发送记录、
归档目录和状态快照。archiver 会按本次 `run_id / message_id` 匹配发送记录，多个 DAG 并发补跑时
不会串用其他 Run 的草稿。如果现场切到离线模式，状态会显示为 `DRY_RUN_OK`。”

【画面】打开 `skill_pipeline_evening` Graph。

【口播】
“拓展 DAG 演示并行分支：邮件渲染和附件摘要同时进行，两条分支都完成后再发送，对应 Airflow
的 join 依赖语义。”

## 3. 任务三：大模型自然语言驱动编排（7:30–11:30）

【画面】打开 `llm_bridge/intent_registry.json`。

【口播】
“这里用注册表限制可选范围。大模型只能从注册表里选择已经存在的 DAG 和 Skill，不能自己编造
一个不存在的工作流；注册表同时写清参数格式和示例，作为给大模型的上下文。”

【画面】依次打开 `deepseek_client.py`、`intent_parser.py`、`airflow_rest_client.py`、`orchestrator.py`、`cli.py`，各说明十几秒职责。

【口播】
“网关分成几个部分：DeepSeek 客户端走 OpenAI 兼容协议并要求返回 JSON；意图解析器优先调用
大模型，失败时使用规则匹配；REST 客户端负责登录、触发、轮询和拉取日志；编排器把这些步骤串起来；
CLI 是对话入口。在线模型不可用时，规则引擎继续识别邮件、并行、采集、归档等常见需求，离线环境
也能完成演示。”

【画面】终端运行 `python llm_bridge/cli.py`（默认 DeepSeek 在线；备用命令为
`python llm_bridge/cli.py --no-llm`），依次输入：

1. `给 zhang@whu.edu.cn 张老师发一封项目进度汇报邮件，主题是 Airflow 课程作业进展`
2. `走并行流水线，准备附件摘要后给 team@example.com 发晚间汇总`
3. `采集今天运行数据`
4. `把刚才的发送记录归档整理一下`

【口播】
“第一句会识别出邮件汇报意图，选中 `email_daily_report`，把邮箱、称呼、主题作为参数传给
REST API；网关轮询到 success 后回传任务状态和耗时。第二句识别并行流水线，选中第二个 DAG。
第三句识别采集意图，路由到通用调度 DAG 的 collect_data Skill。第四句识别归档意图，路由到
archive Skill。”

【画面】再输入：`帮我订一张去火星的火箭票`。

【口播】
“这句需求没有匹配到任何已注册 DAG，网关在发起 REST 请求之前就拒绝触发，不会在 Airflow 里生成
未知运行记录。这就是注册表白名单和规则匹配一起做的保护。”

【画面】回到 Airflow UI 刷新，展示网关触发产生的 DAG Run；终端执行 `list` 和
`status email_daily_report <run_id>`。

【口播】
“UI 里可以看到网关触发的运行记录；`list` 查看支持的能力目录，`status` 命令可以查询任意一次
运行状态。”

【画面】可选：触发一个会失败的任务，展示网关读取失败 Traceback 并汇总。

【口播】
“异常处理分成两层：Airflow 负责重试、退避、超时和失败回调；网关轮询到失败状态后，定位失败
任务、读取日志尾部 Traceback，整理成可读的异常说明，而不是只返回一个 failed 状态。”

## 4. 实现要点总结与问题展望（11:30–13:00）

【画面】打开 README 的实现要点章节和 `docs/problems_and_solutions.md`。

【口播】
“总结一下这次作业的主要实现：第一，Skill 单独拆分后可以复用；第二，邮件 Skill 支持真实 SMTP
和离线草稿两种状态；第三，注册表限制大模型只能选择已有能力；第四，在线模型和规则引擎互相兜底；
第五，Airflow 和网关分别处理执行异常与结果汇总；第六，网关只使用 Python 标准库，不需要额外安装
第三方依赖。此外我还拓展了一个并行分支 DAG。

目前还存在一些局限：定时规则变更现在需要修改 DAG 文件，后续可以考虑通过 REST 更新 schedule；
邮件正文可以继续接入大模型生成；监控结果也可以接入告警渠道推送。”

【画面】最后回到架构图 `docs/architecture.svg`，按整体流程回放一遍，结束录制。

## 录屏提醒

- 全程中文口播，术语第一次出现时给出英文原名；
- 每段操作先说明要做什么，再说明这样设计的原因；
- 本地已配置 QQ 真实 SMTP；现场讲清楚真实发送和 DRY_RUN 离线兜底两种状态；
- 默认走 DeepSeek 在线；如果现场网络超时或密钥异常，切换 `--no-llm` 规则模式继续演示。
