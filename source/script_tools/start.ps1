# 定义要调用的脚本的路径
$script1Path = ".\start_process.ps1"
$script2Path = ".\start_sync.ps1"
$script3Path = ".\start_web.ps1"

# 使用 Start-Process 启动第一个脚本
Start-Process powershell -ArgumentList "-NoProfile -File `"$script1Path`"" -WindowStyle Hidden

# 等待一小段时间以确保第一个脚本有足够的时间启动（可选）
# 注意：这仍然不是真正的同步等待，只是一个简单的延迟
Start-Sleep -Seconds 0.5

# 使用 Start-Process 启动第二个脚本
Start-Process powershell -ArgumentList "-NoProfile -File `"$script2Path`"" -WindowStyle Hidden

# 同样，可以添加另一个延迟（可选）
Start-Sleep -Seconds 0.5

# 使用 Start-Process 启动第三个脚本
Start-Process powershell -ArgumentList "-NoProfile -File `"$script3Path`"" -WindowStyle Hidden

# 此时，主脚本已经完成了启动其他脚本的任务，并且不会等待它们完成
Write-Output "启动脚本已经完成运行，各个任务已经挂起"