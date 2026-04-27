# MoviePilot WatchStateSync

一个用于在 MoviePilot 中同步 Plex 与 Jellyfin 观看进度的第三方插件仓库。

## Beta 状态

当前仓库处于 `beta` 阶段，适合先小范围试用，不建议直接当作完全稳定版分发。

当前最稳的使用场景：

- `Plex -> Jellyfin`
- Plex 无会员，使用插件内置轮询
- 同步 `已看状态`
- 同步 `继续观看进度`
- Jellyfin 使用单用户写回

当前实现为 V2 插件：

- 插件目录：`plugins.v2/watchstatesync`
- 索引文件：`package.v2.json`

## 功能

- 监听 MoviePilot 已接收的媒体服务器 Webhook 事件
- 在没有 Plex Pass 时，定时轮询 Plex 的播放历史与继续观看列表
- 在 Plex 和 Jellyfin 之间同步：
  - 已看/未看状态
  - 继续观看进度
- 支持单向或双向同步
- 支持按用户名过滤事件，避免多用户串进度
- 支持清除插件历史数据和 Plex 轮询游标
- Jellyfin 继续观看写回支持用户名/密码登录换取用户 token

## 当前限制

- 依赖 MoviePilot 先正确配置好 Plex/Jellyfin 媒体服务器
- Jellyfin 建议继续通过 MoviePilot 的 `/api/v1/webhook` 接收事件
- Plex 无 Plex Pass 时可不依赖 webhook，改用插件内置轮询
- Jellyfin 的“继续观看”写回当前依赖插件内配置的 `Jellyfin 用户名/密码`
- 更适合单用户场景
- 首版以事件驱动同步为主，不做历史全量回填
- 双向同步和多用户场景尚未充分验证
- 媒体匹配仍依赖 Plex/Jellyfin 两边刮削结果尽量一致

## 使用

1. 将本仓库放到 GitHub。
2. 在 MoviePilot V2 的插件市场添加仓库地址。
3. 安装 `观看进度同步` 插件。
4. 在插件配置页选择两个媒体服务器、同步方向。
5. 如果 Jellyfin 参与同步，按页面提示配置 Jellyfin webhook。
6. 如果 Plex 没有会员，开启插件中的 Plex 轮询。
7. 如果需要同步 Jellyfin `继续观看`，额外填写：
   - `Jellyfin 用户名`
   - `Jellyfin 密码`

## 推荐配置

- `direction = a_to_b`
- `server_a = plex`
- `server_b = Jellyfin`
- `poll_plex = 开`
- `sync_watched = 开`
- `sync_progress = 开`

## 已知情况

- `已看状态` 目前比 `继续观看进度` 更稳
- Jellyfin 继续观看若只使用 API Key，部分环境下会出现接口返回成功但不持久化的问题
- 当前版本已改为使用 Jellyfin 用户登录态 token 处理继续观看写回
