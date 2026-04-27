# MoviePilot WatchStateSync

一个用于在 MoviePilot 中将 `Plex -> Jellyfin` 观看状态单向同步的第三方插件仓库。

## Beta 状态

当前仓库处于 `beta` 阶段，定位很明确：

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
- 支持清除插件历史数据和 Plex 轮询游标
- Jellyfin 继续观看写回支持用户名/密码登录换取用户 token

## 当前限制

- 依赖 MoviePilot 先正确配置好 Plex 和 Jellyfin 媒体服务器
- 更适合单用户场景
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
5. 如果 Plex 没有会员，开启插件中的 Plex 轮询。
6. 如果需要同步 Jellyfin `继续观看`，额外填写：
   - `Jellyfin 用户名`
   - `Jellyfin 密码`

## 推荐配置

- `Plex 源服务器 = plex`
- `Jellyfin 目标服务器 = Jellyfin`
- `poll_plex = 开`
- `sync_watched = 开`
- `sync_progress = 开`

## 已知情况

- `已看状态` 目前比 `继续观看进度` 更稳
- Jellyfin 继续观看若只使用 API Key，部分环境下会出现接口返回成功但不持久化的问题
- 当前版本已改为使用 Jellyfin 用户登录态 token 处理继续观看写回
