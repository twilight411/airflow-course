# 停止全部容器（保留元数据卷与产物）
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
docker compose --profile flower down
Write-Host "已停止。数据卷保留；如需清空执行 scripts/reset.ps1" -ForegroundColor Green
