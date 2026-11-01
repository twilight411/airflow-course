# 录屏口播稿（约 12–15 分钟，可按时间裁剪）

> 用法：左屏代码/目录，右屏 Airflow UI 与终端；【画面】是操作提示，【口播】是要说的话。
> 加粗部分是评委关注的“创新点”，务必讲到。

## 0. 开场（0:00–0:45）

【画面】打开项目根目录 `D:\DoubaoWork\airflow-course`，依次展开 `dags/`、`skills/`、`llm_bridge/`、`docs/`。

【口播】
“老师好，我完成的是 Airflow 课程三项实践的完整工程：第一，基于官方 Apache Airflow 3.3.2
用 Docker Compose 部署了一套 CeleryExecutor 集群；第二，参照 SkillFabri 的技能建模方式，
把邮件发送封装成独立 Skill，编排成定时 DAG；第三，接入 DeepSeek 大模型，实现自然语言
直接选择并触发 Airflow 工作流，并且完成状态监控和异常回传。整个链路是：**用户说一句话，
大模型解析意图，网关通过 REST API 触发 DAG，Worker 执行 Skill，网关轮询状态并把异常
汇总回来**。”

## 1. 任务一：Airflow 平台部署与架构（0:45–3:30）

【画面】终端执行 `docker compose ps`，逐个组件指认状态 healthy。

【口播】
“这是部署后的八个组件：**api-server** 提供 Web UI 和 REST API；**scheduler** 负责调度决策；
**dag-processor** 解析 DAG 文件；**triggerer** 处理可延迟任务；**celery worker** 真正执行任务；
**postgres** 存元数据；**redis** 是 Celery 消息队列；**flower** 是任务监控面板。
任务不是 scheduler 直接执行的，而是 scheduler 投递到 Redis，worker 拉取执行，状态再回写
Postgres，api-server 统一对外查询。”

【画面】浏览器打开 http://localhost:18080 ，airflow/airflow 登录；DAG 列表展示三个课程 DAG。

【口播】
“我关闭了官方示例 DAG，界面只保留本工程的三个工作流。平台是从 apache/airflow 官方仓库的
docker-compose 裁剪定制的，配置里固定了北京时间时区、关闭历史回填、限制单 DAG 并发数。”

【画面】打开 `email_daily_report` → Graph；再切到 Code 标签。

【口播】
“这是 DAG 定义：四个业务 PythonOperator 任务用 `>>` 串成线性依赖，再加一个
`final_summary` 兜底汇总任务，设置 `trigger_rule=all_done`，上游失败也会执行；
`schedule='0 9 * * *'` 表示每天早上九点定时执行；`retries=2` 配合指数退避重试；
`execution_timeout` 防止任务挂死；成功和失败都挂了回调，把状态快照写到 output 目录。”

【画面】打开 Flower http://localhost:15555；点开历史任务实例的 Logs、XCom 标签。

【口播】
“监控分三层：UI 的 Grid/Graph 看任务状态，Flower 看 worker 队列和耗时，REST API 供外部
系统查询；任务之间通过 XCom 传递结构化数据。”

## 2. 任务二：Skill 集成与邮件工作流（3:30–7:30）

【画面】打开 `skills/email_sender/SKILL.md`。

【口播】
“**第一个创新点：Skill 标准化建模。** 我参照 SkillFabri 平台‘先描述技能做什么、再定义
输入输出契约和技能关系’的方式写 SKILL.md：明确输入字段、输出 JSON、错误模型，以及它和
数据采集 Skill、归档 Skill 的 Intersects 关系。这样 Skill 和 DAG 解耦，可以被多个工作流复用。”

【画面】打开 `skills/email_sender/sender.py`，定位 `render_report`、`save_draft`、`send_message`、`run_email_task`。

【口播】
“**第二个创新点：双模式邮件 Skill。** 它先渲染纯文本和 HTML 双格式正文，保存 .eml 草稿，
再走 SMTP 真实发送；如果授权码没配置或显式开启 DRY_RUN，就自动降级为离线模拟发送，把邮件
落盘并写模拟发送记录，保证课堂演示不会因为缺密钥中断。异常分两类：参数非法直接失败不重试；
SMTP 阶段失败包装成可重试异常，交给 Airflow 重试机制。”

【画面】打开 `dags/01_email_daily_report_dag.py` 讲依赖链；切回 UI 手动 Trigger DAG w/ config：
```json
{"to": "demo-recipient@example.com", "subject": "课程项目进度汇报", "recipient_name": "张老师"}
```

【口播】
“三个 Skill 分别被封装成 PythonOperator：data_collector 准备数据，email_sender 渲染发送，
archiver 做交付校验和归档。现在手动触发一次，传入收件人和主题。”

【画面】观察 Graph 任务依次变绿；点开 `send_report_email` 日志；宿主机依次打开
`output/drafts/`、`output/send_records.jsonl`、`output/archive/`、`output/status/`。

【口播】
“可以看到任务按依赖顺序执行；当前 QQ SMTP 配置下日志状态是 `SENT`，系统同时保留 .eml 草稿、
发送记录、归档目录和成功状态快照，全程可追溯。archiver 会按当前 `run_id / message_id`
精确匹配本次发送记录，并发补跑时也不会串用其他 Run 的草稿，再结合上游 XCom 结果做交付校验。
如果现场切到离线模式，对应状态会显示为 `DRY_RUN_OK`。”

【画面】打开 `skill_pipeline_evening` Graph。

【口播】
“**拓展部分**，第二个 DAG 演示并行分支：邮件渲染和附件摘要两条分支并行，汇合后再发送，
这就是 Airflow 的分支 join 依赖语义。”

## 3. 任务三：大模型自然语言驱动编排（7:30–11:30）

【画面】打开 `llm_bridge/intent_registry.json`。

【口播】
“**第三个创新点：注册表约束，防止大模型幻觉。** 大模型只能从这张注册表里选择已经存在的
DAG 和 Skill，不能编造工作流；注册表同时定义参数 schema 和示例，作为给大模型的上下文。”

【画面】依次打开 `deepseek_client.py`、`intent_parser.py`、`airflow_rest_client.py`、`orchestrator.py`、`cli.py`，各讲 10 秒职责。

【口播】
“网关分五层：DeepSeek 客户端走 OpenAI 兼容协议、强制 JSON 输出；意图解析器大模型优先、
规则兜底；REST 客户端负责 JWT 认证、触发、轮询和拉日志；编排器串联全流程；CLI 是对话入口。
**第四个创新点：大模型加规则双引擎**，在线大模型负责开放语言理解；网络超时、Key 失效或
输出不合法时，中文规则引擎兜底，链路在离线环境照样能演示。”

【画面】终端运行 `python llm_bridge/cli.py`（默认 DeepSeek 在线；现场异常时备用命令为
`python llm_bridge/cli.py --no-llm`），依次输入四句需求：
1. `给 zhang@whu.edu.cn 张老师发一封项目进度汇报邮件，主题是 Airflow 课程作业进展`
2. `走并行流水线，准备附件摘要后给 team@example.com 发晚间汇总`
3. `采集今天运行数据`
4. `把刚才的发送记录归档整理一下`

【口播】
“第一句：解析出邮件汇报意图，选中 `email_daily_report`，抽取邮箱、称呼、主题作为
调度参数，调用 REST API 触发；网关轮询到 success 后回传任务状态时间线和耗时，
实测单次闭环大约 3 到 10 秒。第二句：识别‘并行流水线’，选中第二个 DAG。
第三句：识别采集意图，路由到通用调度 DAG 的 collect_data Skill。第四句：识别归档意图，
路由到 archive Skill。这就是从自然语言理解、任务选择到工作流触发的完整链路。”

【画面】再输入一句与能力目录无关的话：`帮我订一张去火星的火箭票`。

【口播】
“注意看，这句需求没有匹配到任何已注册 DAG，网关在发起 REST 请求之前就拒绝触发，
不会在 Airflow 里生成未知运行记录——这是注册表白名单与规则兜底共同构成的防误触发保护。”

【画面】回 Airflow UI 刷新，展示网关注入产生的 DAG Run；终端执行 `list` 和 `status email_daily_report <run_id>`。

【口播】
“UI 里能看到网关触发的运行记录；`list` 查看能力目录，`status` 命令随时查询任意一次运行状态。”

【画面】（可选异常演示）触发一个会失败的任务，展示网关自动拉取失败 Traceback 并汇总。

【口播】
“**第五个创新点：闭环异常处理。** Airflow 侧重试、退避、超时和失败回调；网关侧轮询终态后，
自动定位失败任务、拉取日志尾部 Traceback，组织成人类可读的异常汇报，而不是只返回一个 failed。”

## 4. 创新点总结与问题展望（11:30–13:00）

【画面】打开 `README.md` 创新点章节与 `docs/problems_and_solutions.md`。

【口播】
“总结六个创新点：一，Skill 契约化建模与复用；二，双模式邮件 Skill 自动降级；三，注册表约束
大模型不产生幻觉；四，大模型加规则双引擎离线可用；五，Airflow 与网关双层异常闭环；六，
零第三方依赖的标准库网关，跨机器免安装运行。另外我还拓展了并行分支 DAG。

目前仍存在的局限和后续方向：定时规则变更目前需要改 DAG 文件，后续可以让大模型通过 REST
直接更新 DAG schedule；邮件模板可以接入大模型自动生成正文；监控可以接入告警渠道推送。”

【画面】最后回到架构图 `docs/architecture.svg`，整体回放一遍链路，结束录制。

## 录屏提醒
- 全程中文口播，术语第一次出现时给英文原名；
- 每段操作先讲“我要做什么”，再讲“为什么这样设计”；
- 本地已配置 QQ 真实 SMTP 自测；口播说明真实发送与 DRY_RUN 离线兜底双模式，未配置凭据时自动落盘模拟；
- 默认走 DeepSeek 在线；若现场大模型超时或网络异常，直接切 `--no-llm` 规则模式继续演示，不卡流程。
