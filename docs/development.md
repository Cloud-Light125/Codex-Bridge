# 开发与验收

## 定向检查

在 `services/bridge-daemon` 运行 QQBot、Binding、Telegram 与 API 的定向 Go 测试；Fake HTTP/Gateway 只验证 Token 缓存/刷新、Hello/Identify/Heartbeat/READY/Resume、消息归一化、OpenID Allowlist、去重、Binding、Stop、持久化和 Request User Input。它不经过真实 QQ 平台。

```powershell
go test ./internal/commandregistry ./internal/query ./internal/qqbot ./internal/telegram ./internal/api ./cmd/bridge-daemon
```

发布构建：

```powershell
.\scripts\build.ps1 -Version 1.1.5
```

输出 `artifacts\win-x64-1.1.5`；若目录已存在脚本会拒绝覆盖。构建不会修改 Codex Sandbox、Approval、Model 或 Reasoning，也不会执行 Git 操作。

安装 Inno Setup 6/7 后，可构建包含 .NET 桌面运行时的每用户 Windows 安装包：

```powershell
.\scripts\build-installer.ps1 -Version 1.1.5
```

安装包及 SHA-256 文件输出到同一版本目录。安装器不会删除用户的 Codex 或 Bridge 数据，卸载时仅清理程序文件、快捷方式与本应用的 HKCU 开机启动项。

## 无副作用真实烟雾

只有本机已由用户配置有效 AppID/AppSecret 时，才可手动执行 Token → Gateway lookup → WebSocket → Hello → Identify → READY → Heartbeat → Stop。禁止脚本自动发送 QQ 消息、Codex Prompt、创建 Binding 或中止 Turn。没有凭据时明确记录“未执行”，不能用 Fake Gateway 冒充真实验收。

## 0.5.2 人工验收（28 步）

1. 打开 [QQ 官方机器人平台](https://q.qq.com/)。
2. 创建 Bot。
3. 获取 AppID。
4. 获取 AppSecret。
5. 配置 C2C 和群聊 @机器人消息事件权限。
6. 启动 `artifacts\win-x64-1.1.5\CloudLight.CodexBridge.exe`。
7. 打开“远程渠道 → QQ 官方机器人”。
8. 填写 AppID。
9. 安全保存 AppSecret，确认 PasswordBox 清空。
10. 如需要，配置代理。
11. 测试凭据；此步骤不得发消息。
12. 启动 Bot。
13. 确认 Gateway 为 connected/ready 且心跳更新。
14. 用 QQ 给 Bot 发消息。
15. 在 WPF“最近发现身份”找到自己的 OpenID。
16. 将对应 User/Group/Member OpenID 加入 Allowlist。
17. 在 QQ 发送 `/status`。
18. 发送 `/threads`。
19. 发送 `/bind 1`。
20. 发送 `/current`。
21. 发送“直接回复OK”。
22. 确认任务进入原 Thread，未创建或 Fork Thread。
23. 确认达到 Persisted 后 QQ 收到正式 `OK`。
24. 测试 `/stop`。
25. 测试一次 Request User Input，并用序号/文本回答。
26. 在获批群聊中测试 @机器人命令与任务，确认只有原成员可回答交互。
27. 确认 Telegram Token、代理、Polling、Binding、命令、Persisted、Stop 和 Request User Input 仍正常。
28. 正常退出，确认 Bot Stop 且无后台重连。

验收日志、SSE、GET API 与 `settings.json` 不得出现 AppSecret、Access Token、Authorization、完整 Session 或消息正文。QQ API 的被动回复有时间/次数约束，应减少进度消息并优先保证最终回复；`sendProgressUpdates` 可关闭。
