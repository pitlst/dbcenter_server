conda activate nuitka_make

# 异步启动一个 PowerShell 实例，运行无限循环
$infiniteLoopScript = @"
cd ../scheduler
while ($true) {
    python main.py 
"@

# 将脚本转换为临时文件
$tempScriptPath = [System.IO.Path]::GetTempFileName() + ".ps1"
[System.IO.File]::WriteAllText($tempScriptPath, $infiniteLoopScript)

try {
    # 异步启动 PowerShell 实例运行无限循环脚本
    Start-Process powershell.exe -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$tempScriptPath`"" -NoWait
    Write-Output "异步启动了包含无限循环的 PowerShell 实例。"
} catch {
    Write-Error "无法启动 PowerShell 实例: $_"
} finally {
    # 清理临时脚本文件（可选，因为脚本已经进入无限循环，文件不再需要）
    [System.IO.File]::Delete($tempScriptPath)
}

Write-Output "sync模块挂起执行"


$infiniteLoopScript = @"
cd ../sync
while ($true) {
    python main.py 
"@

$tempScriptPath = [System.IO.Path]::GetTempFileName() + ".ps1"
[System.IO.File]::WriteAllText($tempScriptPath, $infiniteLoopScript)

try {
    Start-Process powershell.exe -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$tempScriptPath`"" -NoWait
    Write-Output "异步启动了包含无限循环的 PowerShell 实例。"
} catch {
    Write-Error "无法启动 PowerShell 实例: $_"
} finally {
    [System.IO.File]::Delete($tempScriptPath)
}

Write-Output "sync模块挂起执行"