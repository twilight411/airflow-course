# 危险操作：停止容器、删除元数据卷、清空运行产物，回到初始状态
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
docker compose --profile flower down --volumes --remove-orphans
Remove-Item -Recurse -Force "$Root\output\*" -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force "$Root\logs\*" -ErrorAction SilentlyContinue
foreach ($placeholder in @("$Root\output\.gitkeep", "$Root\logs\.gitkeep")) {
    if (-not (Test-Path -LiteralPath $placeholder)) {
        New-Item -ItemType File -Path $placeholder | Out-Null
    }
}
Write-Host "环境已重置。" -ForegroundColor Yellow
