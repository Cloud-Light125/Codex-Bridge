# CloudLight Codex Bridge

CloudLight Codex Bridge 是面向 Windows 的 Codex 与 OpenClaw 桌面助手。它可以浏览并继续现有 Codex 会话，也可以通过 OpenClaw Gateway 选择已有 Session；Telegram 或 QQ 机器人始终由 Bridge 统一接收消息，再转发给选定后端。

当前版本：`1.0.3`

## 主要能力

- 现代化 Windows 桌面界面，支持跟随系统、浅色和深色主题。
- 概览、Codex 会话、远程渠道、消息同步、备份与恢复、设置和运行日志页面。
- 浏览现有 Codex 会话，查看标题、项目目录、状态和本地时间。
- 浏览 OpenClaw Gateway 的 Session，并在界面中以 `[OpenClaw]` 与 `[Codex]` 明确区分后端。
- 在现有会话中继续对话或停止正在运行的任务。
- 使用 `#N` 聊天编号从 Telegram 或 QQ 机器人精确选择会话。
- 使用 `/bind oc:<session-key>` 将 Telegram 或 QQ 聊天绑定到 OpenClaw Session。
- 每次任务只向各平台同步一次最终回答。
- 完整备份和恢复 Codex 数据目录与应用数据。
- 支持登录 Windows 后自动启动、静默启动、系统托盘和窗口状态恢复。

## 系统要求

- Windows 10 1809 或更高版本，x64。
- 已安装并能正常使用 Codex CLI 或 Codex Desktop；OpenClaw 为可选后端。
- Codex 已完成登录。
- 使用 Telegram 或 QQ 时，网络需要能够连接对应平台。

完整安装包已包含 .NET 8 Windows 桌面运行环境。便携版如提示缺少运行环境，请改用完整安装包。

## 下载与安装

1.0.3 的发布目录：

```text
artifacts\win-x64-1.0.3
```

完整安装包：

```text
artifacts\win-x64-1.0.3\CloudLight-CodexBridge-Setup-1.0.3-win-x64.exe
```

便携版可以直接运行：

```text
artifacts\win-x64-1.0.3\CloudLight.CodexBridge.exe
```

安装器只为当前 Windows 用户安装，不需要管理员权限。卸载软件不会删除 Codex 数据、应用设置、关联会话、聊天编号、日志或已保存的机器人密钥。

## 首次启动

1. 启动 CloudLight Codex Bridge。
2. 等待顶部状态显示“已连接”。
3. 打开“会话”，从左侧选择 `[Codex]` 或 `[OpenClaw]` 会话。
4. 如未自动找到 Codex，可在“设置 → Codex”中指定 Codex 程序路径；OpenClaw 可在“设置 → OpenClaw”中自动发现本机 Gateway。

软件不会更改 Codex 已生效的模型、推理强度或安全设置。

## 页面说明

### 概览

查看 Codex、QQ、Telegram 和消息同步的当前状态，以及会话数量和最近活动。

### 会话

左侧以 `[Codex]` / `[OpenClaw]` 和标题显示会话。Codex 会话使用 `#N` 编号；OpenClaw 使用 Gateway 返回的 Session Key。右侧可以查看状态、模型、创建时间、最后更新时间和历史消息。

常用操作：

- “打开”：打开当前会话的项目目录。
- “复制 #N”：复制聊天编号。
- “停止”：停止当前可控制的任务。
- “发送”：在当前会话中继续对话。
- OpenClaw 会话的“发送”和“停止”直接调用 Gateway 的 `chat.send` / `chat.abort`。
- “…”：复制会话 ID，或重新检查保存状态。

会话 ID 仅作为次要详细信息；日常使用优先使用更易读的 `#N` 编号。

### 远程渠道

在此配置 Telegram 和 QQ 机器人。页面默认展示配置与连接状态，凭据、代理和允许账号等设置位于折叠区域。

### 消息同步

消息同步每次任务只发送一次最终回答。可以分别设置 Telegram 和 QQ 的同步目标，并选择是否发送“等待你的回答”、错误或停止提醒。

### 备份与恢复

可以备份 Codex 数据目录和 CloudLight Codex Bridge 应用数据。恢复前会检查备份完整性，并自动备份当前数据，以便需要时回退。

### 设置

管理窗口行为、自动启动、主题、数据位置、Codex 程序路径、OpenClaw Gateway 地址/认证/自动重连、会话刷新间隔，以及新任务的文件访问范围。OpenClaw Token/Password 保存在 Bridge 自己的 DPAPI 凭据文件中，不写入 `settings.json`，也不会修改 OpenClawTray。

### 运行日志

运行日志保留必要的技术排错信息。普通页面只显示便于理解的错误提示，详细原因可在此页或日志目录中查看。

## 使用聊天编号

在 Telegram 或 QQ 中发送：

```text
#41 继续完成剩余功能
```

消息会进入 `#41` 对应的现有 Codex 会话。QQ 与 Telegram 使用相同的查询命令：

| 命令 | 作用 |
| --- | --- |
| `/start` | 查看机器人是否可用 |
| `/help` | 查看命令帮助 |
| `/status` | 查看 Bridge 与 Codex 连接状态；`/status <编号>` 查看指定会话 |
| `/threads [页码]` | 按最近更新时间列出会话和 `#N` 编号，每页 20 个 |
| `/thread <编号>` | 查看指定会话的状态、项目、模型和更新时间 |
| `/history <编号> [数量]` | 查看最近 1～10 轮 User/Assistant 聊天，默认 3 轮 |
| `/running` | 只列出当前真实运行中的任务 |
| `/waiting` | 列出等待用户回答或桌面端审批的会话 |
| `/recent` | 查看最近有活动的 10 个会话 |
| `/failed` | 查看最近真实失败的 Codex Turn |
| `/quota` | 读取 Codex App Server 提供的真实额度窗口 |
| `/bind <编号或 ID>` | 将当前聊天关联到现有会话 |
| `/unbind` | 删除当前关联 |
| `/current` | 查看当前关联会话 |
| `/stop` | 停止当前聊天发起的任务 |
| `/cancel` | 取消当前正在等待的回答 |

这些是首次启动时自动加载并默认锁定的系统指令。“指令”页面可以逐条解锁、改名、添加别名、停用或恢复，也可以创建任意数量映射到受支持功能的自定义指令。QQ 与 Telegram 始终读取同一个有效配置；中文指令即使不符合 Telegram 菜单规则，仍可直接在聊天文本中使用。

`/threads`、`/thread`、`/history`、`/running`、`/waiting`、`/recent`、`/failed`、`/quota` 和 `/status` 均由 Bridge 在本地直接查询，不会向 Codex 提交新任务、创建 User Message、改变会话或推进消息同步游标。查询命令不要求当前聊天先执行 `/bind`，但仍沿用 QQ 与 Telegram 已配置的允许账号列表。

`/threads` 也会列出当前可用的 OpenClaw Session。OpenClaw 不使用 Codex 的 `#N` 编号，绑定时使用完整 Session Key：

```text
/bind oc:agent:main:main
/current
你好，继续处理上次任务
/stop
```

OpenClaw 的最终回答由 Gateway `chat` 事件返回给 Bridge，再由 Bridge 发回当前 Telegram 或 QQ 聊天；OpenClaw 不会直接消费同一个 Bot。

`/quota` 只显示当前 Codex App Server 的 `account/rateLimits/read` 实际返回值，不根据 Token 用量估算，也不会抓取网页。如果当前 Codex 版本、登录方式或账号没有返回可读额度，机器人会明确说明当前无法读取。

## Telegram 配置

1. 使用 BotFather 创建机器人并取得 Bot Token。
2. 打开“远程渠道 → Telegram”。
3. 安全保存 Bot Token。
4. 填写允许使用机器人的 Telegram 用户 ID，每行一个。
5. 按需要选择直接连接、使用环境变量或自定义 HTTP 代理。
6. 测试凭据和代理，然后启动 Telegram。
7. 在 Telegram 中发送 `/threads`，再使用 `#N 消息` 或建立关联会话。

机器人密钥会安全保存到本机，不会显示在普通状态或日志中。

## QQ 机器人配置

1. 在 [QQ 开放平台](https://q.qq.com/)创建机器人。
2. 开启私聊和群聊 @机器人消息权限。
3. 打开“远程渠道 → QQ 机器人”，填写 AppID。
4. 安全保存 AppSecret（应用密钥）。
5. 按需要选择网络连接方式。
6. 依次执行“测试凭据”“测试网络”，然后启动 QQ 机器人。
7. 给机器人发送消息，在“最近发现的 QQ 账号”中添加允许的用户、群聊或群成员标识。
8. 发送 `/threads`，再使用 `#N 消息` 或建立关联会话。

QQ 配置中仍会使用 OpenID，这是 QQ 开放平台提供的用户或群聊标识。界面会分别显示为“用户标识”“群聊标识”和“群成员标识”。应用密钥会安全保存到本机。

## 备份与恢复

“Codex 数据目录”通常位于：

```text
%USERPROFILE%\.codex
```

“应用数据”通常位于：

```text
%LOCALAPPDATA%\CloudLight\CodexBridge
```

备份会生成单个 `.clcbak` 文件。完整备份可能包含账号凭据，请妥善保存。

恢复方式：

- 完整替换（推荐）：恢复到备份时的文件状态。
- 合并：只复制当前不存在的文件。

恢复开始前会检查备份是否完整，并自动创建恢复前备份。恢复过程中如果检测到 Codex 仍在写入数据，软件会提示关闭相关程序后重试。

## 数据与隐私

- 机器人密钥仅保存在当前 Windows 用户的本机数据中。
- 普通状态和日志不会显示完整机器人密钥或消息正文。
- 本机服务只监听回环地址，不对局域网或公网开放。
- 完整备份可能包含 Codex 和渠道凭据，请按敏感文件保管。

## 开发

开发环境与构建说明见 [docs/development.md](docs/development.md)，架构说明见 [docs/architecture.md](docs/architecture.md)。

生成 1.0.3 Release：

```powershell
.\scripts\build.ps1 -Version 1.0.3
```

脚本输出到 `artifacts\win-x64-1.0.3`，并拒绝覆盖已存在的版本目录。构建不会执行 Git 提交、标签或推送。

## 许可证与第三方说明

第三方许可和来源记录见：

- [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)
- [licenses/Apache-2.0.txt](licenses/Apache-2.0.txt)
- [licenses/gorilla-websocket-LICENSE.txt](licenses/gorilla-websocket-LICENSE.txt)
- [docs/upstream-sources.md](docs/upstream-sources.md)

1.0.3 安装包尚未进行商业代码签名，也不包含自动更新。
