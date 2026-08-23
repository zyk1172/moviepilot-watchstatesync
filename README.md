# MoviePilot WatchStateSync

一个用于在 MoviePilot 中将 `Plex -> Jellyfin` 观看状态单向同步的第三方插件仓库。

## Beta 状态

当前仓库处于 `beta` 阶段，版本为 `1.2.1`，定位很明确：

- 只做 `Plex -> Jellyfin`
- 不再支持双向同步
- 重点场景是 Plex 无会员时，通过插件轮询把 Plex 的状态写到 Jellyfin

## 当前能力

- 单向同步 Plex 到 Jellyfin
- 同步 `已看状态`
- 同步 `继续观看进度`
- Plex 无 Plex Pass 时，定时轮询：
  - 播放历史
  - Continue Watching
- Plex 本地 WebSocket 仅作为低延迟加速层；定时轮询始终保留为 reconciliation，监听断线时会在轮询周期检查并重连
- 通过 Bearer 认证 API 支持清除插件历史数据、Plex 轮询游标和手动轮询
- Jellyfin 继续观看写回支持用户名/密码登录换取用户 token
- Jellyfin 读取、匹配、写回和写后验证共用同一个 Jellyfin 用户上下文
- Plex history 使用分页和游标，失败事件进入 Outbox 重试
- 插件页提供最近轮询、匹配、写回和验证诊断；手动操作只通过受保护 API 提供
- Pull Request 会由 GitHub Actions 自动运行仓库内回归测试

## 当前限制

- 依赖 MoviePilot 先正确配置好 Plex 和 Jellyfin 媒体服务器
- 同步模型固定为一个 Plex 源用户到一个 Jellyfin 登录用户，不提供多个 Plex 用户映射到同一个 Jellyfin 用户
- “Plex 源用户”留空时使用 Plex token owner；填写一个用户名或 accountId 时，轮询、History 和 WebSocket 会使用该用户上下文，无法切换时跳过，避免误读 token owner 的状态
- 不做历史全量回填
- 媒体匹配仍依赖 Plex/Jellyfin 两边刮削结果尽量一致
- 双向同步已从仓库定位中移除

## 使用

1. 将本仓库放到 GitHub。
2. 在 MoviePilot V2 的插件市场添加仓库地址。
3. 安装 `观看进度同步` 插件。
4. 在插件配置页选择：
   - `Plex 源服务器`
   - `Jellyfin 目标服务器`
5. 保持 Plex 轮询开启作为稳定兜底；即使启用 WebSocket，轮询也会继续执行 reconciliation。
6. 如果开启 `同步继续观看进度`，必须填写 Jellyfin 用户名和密码。该身份用于目标状态读取、匹配、写回和写后验证；不要让 MoviePilot 媒体服务器用户与插件登录用户不一致。
7. 如需指定非 token owner 的 Plex 用户，填写一个：
   - `Plex 源用户（用户名或 accountId）`

## 推荐配置

- `Plex 源服务器 = plex`
- `Jellyfin 目标服务器 = Jellyfin`
- `poll_plex = 开`
- `sync_watched = 开`
- `sync_progress = 开`

## 已知情况

- `已看状态` 目前比 `继续观看进度` 更稳
- Jellyfin 继续观看必须使用用户登录态 token；仅 API Key 不会执行进度写回
- 每次写回后都会重新读取 Jellyfin UserData；验证失败会重试一次并保留 Outbox
- Outbox 保存源事件时间和序列号；同一媒体用户只保留最新待处理状态，检测到目标已有更高进度时会丢弃旧重试，避免进度倒退
- `sync_now`、`clear_history` 和 `diagnostics` API 均要求 Bearer 认证；当前 V2 Vuetify 页面不提交未认证的原生表单
- 当前插件实现仍是 MoviePilot V2（`package.v2.json` / `plugins.v2`），不包含 V3 实现
