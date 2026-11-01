---
name: email_sender
version: 1.0.0
modeled_on: https://skillfabri.com/  # 参考 SkillFabri 的 SKILL.md 格式和关系写法
domain: Automation Scripting / Communication
tags: [email, smtp, notification, report, html-template, retry]
status: course-demo
runtime: Python 3.11+ (标准库 smtplib / email，无第三方依赖)
relations:
  contains: []
  intersects:
    - data_collector   # 提供报表上下文
    - archiver         # 消费发送记录做归档校验
  same_as: []
---

# email_sender —— SMTP 邮件渲染与发送 Skill

## 1. Skill 做什么（What it does）
接收结构化报表上下文，渲染纯文本 + HTML 双格式邮件，保存 `.eml` 草稿，
通过 SMTP 真实发送；在凭据缺失或显式配置下进入 DRY_RUN 模拟发送，
并把每次发送结果追加到 `send_records.jsonl`，供下游归档与监控消费。

## 2. 输入字段（Inputs）
| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `payload.report_date` | string | 否 | 报表日期，缺省取当前北京时间 |
| `payload.report_title` | string | 否 | 邮件主题 |
| `payload.metrics` | object | 否 | 指标键值对，渲染进 HTML 表格 |
| `payload.highlights` | string[] | 否 | 重点事项列表 |
| `payload.recipient_name` | string | 否 | 称呼对象 |
| `conf.to` | string | 否 | 收件人（逗号分隔），覆盖环境默认收件人 |
| `conf.subject` | string | 否 | 覆盖主题 |
| `conf.text_body` / `conf.html_body` | string | 否 | 覆盖正文（LLM 生成内容入口） |

## 3. 输出结果（Outputs）
```json
{
  "skill": "email_sender",
  "run_id": "manual__2026-11-01T01:00:00+08:00",
  "config_mode": "dry-run | smtp",
  "send_result": {
    "status": "SENT | DRY_RUN_OK | FAILED",
    "mode": "smtp | dry-run",
    "recipients": ["teacher@example.com"],
    "subject": "…",
    "message_id": "<…@airflow-course.local>",
    "draft_path": "/opt/airflow/output/drafts/….eml",
    "duration_ms": 12.3,
    "timestamp": "2026-11-01T09:00:00+08:00"
  }
}
```

## 4. 错误模型（Error handling）
- 收件人格式非法：`ValueError`，任务直接失败，不进入 SMTP 重试；
- SMTP 连接 / 登录 / 发送失败：包装为 `EmailSkillError`，由 Airflow `retries`
  配合指数退避重试；失败记录写入 `send_records.jsonl`；
- 凭据缺失：自动降级 DRY_RUN，不触达网络，避免演示中断。

## 5. 在 Airflow 中的挂载方式
- 代码目录 `skills/email_sender/` 经 docker-compose 挂载到 `/opt/airflow/skills`；
- DAG 中以 `PythonOperator` 调用 `run_email_task(payload, conf, run_id)`；
- SMTP 主机、端口、账号、授权码、默认收件人由 `.env` 经容器环境变量注入。

## 6. 与其他 Skill 的关系
- **Intersects data_collector**：消费其报表上下文；
- **Intersects archiver**：产出发送记录供其归档与交付校验；
- 可独立被 `llm_skill_dispatch` DAG 按名称路由调用。

## 7. 配置项（环境变量）
`EMAIL_SMTP_HOST` / `EMAIL_SMTP_PORT` / `EMAIL_SMTP_SECURE(ssl|starttls|none)` /
`EMAIL_SMTP_USER` / `EMAIL_SMTP_PASSWORD` / `EMAIL_SMTP_FROM` /
`EMAIL_DEFAULT_TO` / `EMAIL_DRY_RUN` / `EMAIL_OUTPUT_DIR`
