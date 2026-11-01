# 运行宿主机离线单元测试（不依赖 Airflow 容器）
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$py = "D:\ProgramData\anaconda3\python.exe"
if (-not (Test-Path $py)) { $py = "python" }
& $py tests\test_email_skill.py
& $py tests\test_archiver_record_match.py
& $py tests\test_status_snapshot_merge.py
& $py tests\test_rule_parser.py
& $py tests\test_registry_consistency.py
& $py tests\test_failure_summary.py
