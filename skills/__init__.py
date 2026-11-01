"""课程 Skill 包根目录。

在 Airflow 容器内，项目根目录 /opt/airflow 已加入 sys.path，
因此可直接 `from skills.email_sender import sender`。
"""

__all__ = ["email_sender", "data_collector", "archiver"]
