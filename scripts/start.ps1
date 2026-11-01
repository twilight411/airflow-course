# =====================================================================
# 一键启动 Airflow 平台（含 Flower 监控）
# 用法: 在项目根目录执行  ./scripts/start.ps1
# =====================================================================
$ErrorActionPreference = "Continue"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not (Test-Path ".env")) {
    Write-Host "[错误] 缺少 .env，请先复制 .env.example 为 .env" -ForegroundColor Red
    exit 1
}

Write-Host "`n[1/4] 初始化元数据库并创建管理员账号 (airflow/airflow)..." -ForegroundColor Cyan
docker compose up airflow-init
if ($LASTEXITCODE -ne 0) { Write-Host "airflow-init 失败" -ForegroundColor Red; exit 1 }

Write-Host "`n[2/4] 启动全部服务（api-server / scheduler / dag-processor / triggerer / worker / redis / postgres / flower）..." -ForegroundColor Cyan
docker compose --profile flower up -d

Write-Host "`n[3/4] 等待 api-server 健康检查通过..." -ForegroundColor Cyan
$healthy = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 5
    $status = docker inspect --format '{{.State.Health.Status}}' $(docker compose ps -q airflow-apiserver) 2>$null
    Write-Host "  api-server 状态: $status"
    if ($status -eq "healthy") { $healthy = $true; break }
}
if (-not $healthy) { Write-Host "api-server 未在超时时间内变为 healthy，请查看 docker compose logs" -ForegroundColor Red; exit 1 }

Write-Host "`n[4/4] 取消课程 DAG 暂停状态..." -ForegroundColor Cyan
$dags = @("email_daily_report", "skill_pipeline_evening", "llm_skill_dispatch")
foreach ($dag in $dags) {
    docker compose exec -T airflow-scheduler airflow dags unpause $dag
}

Write-Host "`n========== 启动完成 ==========" -ForegroundColor Green
Write-Host "Airflow UI : http://localhost:18080   账号 airflow / 密码 airflow"
Write-Host "Flower 监控: http://localhost:15555"
Write-Host "查看状态    : docker compose ps"
Write-Host "查看日志    : docker compose logs -f airflow-scheduler"
