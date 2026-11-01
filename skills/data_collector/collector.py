"""data_collector Skill：采集/组装日报所需的结构化上下文。

在真实生产中，本 Skill 会对接数据库、监控系统或第三方 API；
课程演示中使用可复现的模拟数据，并允许 DAG run conf 覆盖指标与重点事项，
便于演示“外部参数驱动工作流”的能力。
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

log = logging.getLogger("skill.data_collector")

CST = timezone(timedelta(hours=8))
WEEKDAYS_CN = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]


def collect_report_payload(conf: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    now = datetime.now(CST)
    conf = conf or {}

    # 以日期为种子生成可复现的模拟指标，同一天多次运行结果一致
    day_seed = now.toordinal()
    metrics: Dict[str, Any] = {
        "当日 DAG 运行次数": 12 + (day_seed % 5),
        "成功任务数": 11 + (day_seed % 4),
        "失败任务数": day_seed % 2,
        "平均任务耗时(秒)": round(3.2 + (day_seed % 7) * 0.4, 2),
        "待处理告警": day_seed % 3,
    }
    highlights = [
        "email_daily_report DAG 按 09:00 定时调度成功",
        "email_sender Skill 完成邮件渲染与发送记录归档",
        "LLM 网关自然语言触发链路状态正常",
    ]

    payload: Dict[str, Any] = {
        "report_date": now.strftime("%Y-%m-%d"),
        "weekday": WEEKDAYS_CN[now.weekday()],
        "report_title": f"{now.strftime('%Y-%m-%d')} Airflow 自动化工作流日报",
        "greeting": "各位老师、同学：",
        "recipient_name": conf.get("recipient_name", "老师"),
        "metrics": metrics,
        "highlights": highlights,
        "closing": "本邮件由 Airflow 自动化工作流通过 email_sender Skill 定时发送。",
    }

    # conf 覆盖：演示外部参数（含 LLM 传入）驱动报表内容
    if isinstance(conf.get("metrics"), dict):
        payload["metrics"].update(conf["metrics"])
    if isinstance(conf.get("highlights"), list):
        payload["highlights"] = conf["highlights"]
    if conf.get("report_title"):
        payload["report_title"] = conf["report_title"]
    if conf.get("recipient_name"):
        payload["recipient_name"] = conf["recipient_name"]

    log.info("报表上下文已生成: %s", payload["report_date"])
    return payload
