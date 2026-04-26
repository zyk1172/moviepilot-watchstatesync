import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.core.event import Event, eventmanager
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaServerItem, ServiceInfo, WebhookEventInfo
from app.schemas.types import EventType
from app.utils.http import RequestUtils


@dataclass
class NormalizedState:
    source_server: str
    source_type: str
    event_type: str
    user_name: Optional[str]
    media_kind: str
    title: Optional[str]
    original_title: Optional[str]
    series_title: Optional[str]
    year: Optional[int]
    tmdb_id: Optional[int]
    imdb_id: Optional[str]
    tvdb_id: Optional[str]
    season: Optional[int]
    episode: Optional[int]
    source_item_id: Optional[str]
    progress_ms: int
    duration_ms: int
    watched: bool
    percent: float
    played_at: Optional[str]


class WatchStateSync(_PluginBase):
    plugin_name = "观看进度同步"
    plugin_desc = "在 Plex 和 Jellyfin 之间同步已看状态与继续观看进度。"
    plugin_icon = "sync_file.png"
    plugin_version = "1.0.0"
    plugin_author = "OpenAI Codex"
    author_url = "https://openai.com"
    plugin_config_prefix = "watchstatesync_"
    plugin_order = 40
    auth_level = 1

    _enabled = False
    _server_a = ""
    _server_b = ""
    _direction = "two_way"
    _allowed_users: List[str] = []
    _sync_watched = True
    _sync_progress = True
    _min_progress_seconds = 60
    _progress_delta_seconds = 30
    _watched_percent = 90
    _notify_on_sync = False
    _dry_run = False

    _lock = threading.Lock()
    _recent_writes: Dict[str, float] = {}
    _recent_failures: Dict[str, float] = {}
    _write_ttl_seconds = 180
    _max_history = 30

    def init_plugin(self, config: dict = None):
        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._server_a = (config.get("server_a") or "").strip()
        self._server_b = (config.get("server_b") or "").strip()
        self._direction = config.get("direction") or "two_way"
        self._sync_watched = bool(config.get("sync_watched", True))
        self._sync_progress = bool(config.get("sync_progress", True))
        self._min_progress_seconds = self._safe_int(config.get("min_progress_seconds"), 60)
        self._progress_delta_seconds = self._safe_int(config.get("progress_delta_seconds"), 30)
        self._watched_percent = self._safe_int(config.get("watched_percent"), 90)
        self._notify_on_sync = bool(config.get("notify_on_sync", False))
        self._dry_run = bool(config.get("dry_run", False))
        self._allowed_users = [
            user.strip() for user in (config.get("allowed_users") or "").split(",") if user.strip()
        ]
        self._cleanup_caches()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        server_items = [
            {"title": config.name, "value": config.name}
            for config in MediaServerHelper().get_configs().values()
            if config.type in ["plex", "jellyfin"]
        ]

        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "enabled",
                                        "label": "启用插件"
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "sync_watched",
                                        "label": "同步已看状态"
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "sync_progress",
                                        "label": "同步继续观看进度"
                                    }
                                }]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSelect",
                                    "props": {
                                        "model": "server_a",
                                        "label": "媒体服务器 A",
                                        "items": server_items,
                                        "clearable": True
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSelect",
                                    "props": {
                                        "model": "server_b",
                                        "label": "媒体服务器 B",
                                        "items": server_items,
                                        "clearable": True
                                    }
                                }]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [{
                                    "component": "VSelect",
                                    "props": {
                                        "model": "direction",
                                        "label": "同步方向",
                                        "items": [
                                            {"title": "双向同步", "value": "two_way"},
                                            {"title": "A -> B", "value": "a_to_b"},
                                            {"title": "B -> A", "value": "b_to_a"}
                                        ]
                                    }
                                }]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "min_progress_seconds",
                                        "label": "最小进度阈值（秒）"
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "progress_delta_seconds",
                                        "label": "最小进度变化量（秒）"
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "watched_percent",
                                        "label": "视为已看百分比"
                                    }
                                }]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [{
                                    "component": "VTextarea",
                                    "props": {
                                        "model": "allowed_users",
                                        "label": "允许同步的用户名（逗号分隔，可留空）",
                                        "rows": 2,
                                        "placeholder": "alice,bob"
                                    }
                                }]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "notify_on_sync",
                                        "label": "同步成功时发送系统通知"
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "dry_run",
                                        "label": "仅记录不实际写入"
                                    }
                                }]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [{
                                    "component": "VAlert",
                                    "props": {
                                        "type": "info",
                                        "variant": "tonal",
                                        "text": "请在 Plex/Jellyfin 的 webhook 中回调到 MoviePilot: /api/v1/webhook?token=API_TOKEN&source=媒体服务器名。Plex 建议开启 media.stop 与 media.scrobble；Jellyfin 建议开启 PlaybackStop。"
                                    }
                                }]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "server_a": "",
            "server_b": "",
            "direction": "two_way",
            "sync_watched": True,
            "sync_progress": True,
            "min_progress_seconds": 60,
            "progress_delta_seconds": 30,
            "watched_percent": 90,
            "allowed_users": "",
            "notify_on_sync": False,
            "dry_run": False
        }

    def get_page(self) -> List[dict]:
        history = self.get_data("history") or []
        if not history:
            history_rows = [{
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "text": "还没有同步记录。确认 webhook 已经打到 MoviePilot，并且两个媒体服务器都已在插件中选中。"
                }
            }]
        else:
            rows = []
            for item in history[:10]:
                rows.append({
                    "component": "VListItem",
                    "props": {
                        "title": item.get("title"),
                        "subtitle": item.get("subtitle")
                    }
                })
            history_rows = [{
                "component": "VList",
                "content": rows
            }]

        return [
            {
                "component": "VCard",
                "props": {"variant": "tonal"},
                "content": [
                    {
                        "component": "VCardText",
                        "text": (
                            f"状态：{'已启用' if self._enabled else '未启用'} | "
                            f"方向：{self._direction} | "
                            f"服务器：{self._server_a or '-'} / {self._server_b or '-'}"
                        )
                    }
                ]
            },
            {
                "component": "VCard",
                "props": {"class": "mt-3"},
                "content": [
                    {"component": "VCardTitle", "text": "最近同步"},
                    {"component": "VCardText", "content": history_rows}
                ]
            }
        ]

    @eventmanager.register(EventType.WebhookMessage)
    def handle_webhook(self, event: Event):
        if not self._enabled:
            return

        event_info: WebhookEventInfo = getattr(event, "event_data", None)
        if not event_info:
            return

        source_server = event_info.server_name
        if not source_server or source_server not in [self._server_a, self._server_b]:
            return

        if self._allowed_users:
            user_name = (event_info.user_name or "").strip()
            if not user_name or user_name not in self._allowed_users:
                logger.debug("观看进度同步：事件用户不在允许列表中，忽略")
                return

        target_server = self._resolve_target_server(source_server)
        if not target_server:
            return

        source_service = self._get_service(source_server)
        target_service = self._get_service(target_server)
        if not source_service or not target_service:
            return

        state = self._build_state(source_service, event_info)
        if not state:
            return

        if self._is_duplicate_source_event(state):
            return

        target_item = self._find_target_item(target_service, state)
        if not target_item:
            self._record_history(
                title=f"{source_server} -> {target_server} 未匹配到目标条目",
                subtitle=self._state_label(state)
            )
            return

        write_key = self._make_write_key(target_server, target_item.item_id, state)
        if self._seen_recently(write_key):
            logger.debug("观看进度同步：目标状态近期已写入，跳过回环")
            return

        should_write, reason = self._target_needs_update(target_service, target_item, state)
        if not should_write:
            self._record_history(
                title=f"{source_server} -> {target_server} 跳过",
                subtitle=f"{self._state_label(state)} | {reason}"
            )
            return

        ok, message = self._apply_state(target_service, target_item, state)
        if ok:
            self._remember_write(write_key)

        title = f"{source_server} -> {target_server} {'成功' if ok else '失败'}"
        subtitle = f"{self._state_label(state)} | {message}"
        self._record_history(title=title, subtitle=subtitle)

        if ok and self._notify_on_sync:
            self.post_message(title=title, text=subtitle)

    def stop_service(self):
        self._cleanup_caches(force=True)

    def _get_service(self, service_name: str) -> Optional[ServiceInfo]:
        service = MediaServerHelper().get_service(name=service_name)
        if not service or not service.instance:
            logger.warning(f"观看进度同步：未找到媒体服务器 {service_name}")
            return None
        if service.type not in ["plex", "jellyfin"]:
            logger.warning(f"观看进度同步：暂不支持的媒体服务器类型 {service.type}")
            return None
        return service

    def _resolve_target_server(self, source_server: str) -> Optional[str]:
        if not self._server_a or not self._server_b or self._server_a == self._server_b:
            return None
        if source_server == self._server_a:
            if self._direction in ["two_way", "a_to_b"]:
                return self._server_b
            return None
        if source_server == self._server_b:
            if self._direction in ["two_way", "b_to_a"]:
                return self._server_a
            return None
        return None

    def _build_state(self, service: ServiceInfo, event_info: WebhookEventInfo) -> Optional[NormalizedState]:
        if service.type == "jellyfin":
            return self._build_jellyfin_state(service, event_info)
        if service.type == "plex":
            return self._build_plex_state(service, event_info)
        return None

    def _build_jellyfin_state(self, service: ServiceInfo, event_info: WebhookEventInfo) -> Optional[NormalizedState]:
        if event_info.event not in ["PlaybackStop", "ItemMarkedPlayed", "ItemMarkedUnplayed"]:
            return None

        payload = event_info.json_object or {}
        progress_ticks = self._safe_int(payload.get("PlaybackPositionTicks"), 0)
        duration_ticks = self._safe_int(payload.get("RunTimeTicks"), 0)
        progress_ms = int(progress_ticks / 10000) if progress_ticks else 0
        duration_ms = int(duration_ticks / 10000) if duration_ticks else 0
        percent = round((progress_ms / duration_ms) * 100, 2) if progress_ms and duration_ms else 0.0

        played = bool(payload.get("Played")) or bool(payload.get("PlayedToCompletion"))
        if not played and percent >= self._watched_percent:
            played = True

        iteminfo = service.instance.get_iteminfo(event_info.item_id) if event_info.item_id else None
        tmdb_id = self._coerce_int(
            payload.get("Provider_tmdb") or (iteminfo.tmdbid if iteminfo else None)
        )
        imdb_id = payload.get("Provider_imdb") or (iteminfo.imdbid if iteminfo else None)
        tvdb_id = payload.get("Provider_tvdb") or (iteminfo.tvdbid if iteminfo else None)

        media_kind = "episode" if event_info.item_type == "TV" else "movie"
        series_title = payload.get("SeriesName") if media_kind == "episode" else None
        title = payload.get("Name") or (iteminfo.title if iteminfo else event_info.item_name)

        if played:
            progress_ms = 0
            percent = 100.0

        if not self._should_sync(progress_ms, duration_ms, played):
            return None

        return NormalizedState(
            source_server=service.name,
            source_type="jellyfin",
            event_type=event_info.event,
            user_name=event_info.user_name,
            media_kind=media_kind,
            title=title,
            original_title=iteminfo.original_title if iteminfo else None,
            series_title=series_title,
            year=self._coerce_int(payload.get("Year") or (iteminfo.year if iteminfo else None)),
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            tvdb_id=tvdb_id,
            season=self._coerce_int(payload.get("SeasonNumber")),
            episode=self._coerce_int(payload.get("EpisodeNumber")),
            source_item_id=event_info.item_id,
            progress_ms=progress_ms,
            duration_ms=duration_ms,
            watched=played,
            percent=percent,
            played_at=payload.get("LastPlayedDate")
        )

    def _build_plex_state(self, service: ServiceInfo, event_info: WebhookEventInfo) -> Optional[NormalizedState]:
        if event_info.event not in ["media.stop", "media.scrobble", "media.unscrobble"]:
            return None

        plex = service.instance.get_plex()
        if not plex or not event_info.item_id:
            return None

        try:
            item = plex.fetchItem(event_info.item_id)
        except Exception as err:
            logger.error(f"观看进度同步：读取 Plex 条目失败 {err}")
            return None

        payload = event_info.json_object or {}
        metadata = payload.get("Metadata") or {}
        guids = metadata.get("Guid") or []
        ids = self._extract_provider_ids_from_plex_guids(guids)

        progress_ms = self._safe_int(getattr(item, "viewOffset", 0), 0)
        duration_ms = self._safe_int(getattr(item, "duration", 0), 0)
        percent = round((progress_ms / duration_ms) * 100, 2) if progress_ms and duration_ms else 0.0
        watched = bool(getattr(item, "isPlayed", False)) or event_info.event == "media.scrobble"
        if not watched and percent >= self._watched_percent:
            watched = True

        media_kind = "episode" if getattr(item, "type", None) == "episode" else "movie"
        series_title = getattr(item, "grandparentTitle", None) if media_kind == "episode" else None
        title = getattr(item, "title", None)
        year = getattr(item, "year", None)
        season = getattr(item, "parentIndex", None) or metadata.get("parentIndex")
        episode = getattr(item, "index", None) or metadata.get("index")

        if watched:
            progress_ms = 0
            percent = 100.0

        if not self._should_sync(progress_ms, duration_ms, watched):
            return None

        return NormalizedState(
            source_server=service.name,
            source_type="plex",
            event_type=event_info.event,
            user_name=event_info.user_name,
            media_kind=media_kind,
            title=title,
            original_title=getattr(item, "originalTitle", None),
            series_title=series_title,
            year=self._coerce_int(year),
            tmdb_id=self._coerce_int(ids.get("tmdb")),
            imdb_id=ids.get("imdb"),
            tvdb_id=ids.get("tvdb"),
            season=self._coerce_int(season),
            episode=self._coerce_int(episode),
            source_item_id=event_info.item_id,
            progress_ms=progress_ms,
            duration_ms=duration_ms,
            watched=watched,
            percent=percent,
            played_at=self._to_iso(getattr(item, "lastViewedAt", None))
        )

    def _find_target_item(self, target_service: ServiceInfo, state: NormalizedState) -> Optional[MediaServerItem]:
        if state.media_kind == "movie":
            return self._find_target_movie(target_service, state)
        if state.media_kind == "episode":
            return self._find_target_episode(target_service, state)
        return None

    def _find_target_movie(self, target_service: ServiceInfo, state: NormalizedState) -> Optional[MediaServerItem]:
        if target_service.type == "jellyfin":
            items = target_service.instance.get_movies(
                title=state.title,
                year=state.year,
                tmdb_id=state.tmdb_id
            ) or []
            return items[0] if items else None

        items = target_service.instance.get_movies(
            title=state.title,
            original_title=state.original_title,
            year=state.year,
            tmdb_id=state.tmdb_id
        ) or []
        return items[0] if items else None

    def _find_target_episode(self, target_service: ServiceInfo, state: NormalizedState) -> Optional[MediaServerItem]:
        if not state.season or not state.episode:
            return None

        if target_service.type == "jellyfin":
            show_id, _ = target_service.instance.get_tv_episodes(
                title=state.series_title or state.title,
                year=state.year,
                tmdb_id=state.tmdb_id,
                season=state.season
            )
            if not show_id:
                return None
            return self._find_jellyfin_episode_item(target_service, show_id, state.season, state.episode)

        show_key, _ = target_service.instance.get_tv_episodes(
            title=state.series_title or state.title,
            year=state.year,
            tmdb_id=state.tmdb_id,
            season=state.season
        )
        if not show_key:
            return None
        return self._find_plex_episode_item(target_service, show_key, state.season, state.episode)

    def _find_jellyfin_episode_item(
        self, target_service: ServiceInfo, show_id: str, season: int, episode: int
    ) -> Optional[MediaServerItem]:
        server = target_service.instance
        url = f"{server._host}Shows/{show_id}/Episodes"
        params = {
            "season": season,
            "userId": server.user,
            "isMissing": "false",
            "api_key": server._apikey
        }
        res = RequestUtils().get_res(url, params=params)
        if not res:
            return None
        for item in res.json().get("Items", []):
            if item.get("ParentIndexNumber") == season and item.get("IndexNumber") == episode:
                return server.get_iteminfo(item.get("Id"))
        return None

    def _find_plex_episode_item(
        self, target_service: ServiceInfo, show_key: str, season: int, episode: int
    ) -> Optional[MediaServerItem]:
        plex = target_service.instance.get_plex()
        if not plex:
            return None
        try:
            show = plex.fetchItem(show_key)
            for item in show.episodes():
                if int(getattr(item, "seasonNumber", 0)) == int(season) and int(getattr(item, "index", 0)) == int(episode):
                    return target_service.instance.get_iteminfo(item.key)
        except Exception as err:
            logger.error(f"观看进度同步：定位 Plex 剧集失败 {err}")
        return None

    def _apply_state(self, target_service: ServiceInfo, target_item: MediaServerItem, state: NormalizedState) -> Tuple[bool, str]:
        if self._dry_run:
            return True, "dry-run"
        if target_service.type == "jellyfin":
            return self._apply_to_jellyfin(target_service, target_item, state)
        if target_service.type == "plex":
            return self._apply_to_plex(target_service, target_item, state)
        return False, "unsupported target"

    def _apply_to_jellyfin(
        self, target_service: ServiceInfo, target_item: MediaServerItem, state: NormalizedState
    ) -> Tuple[bool, str]:
        server = target_service.instance
        url = f"{server._host}Users/{server.user}/Items/{target_item.item_id}"
        params = {"api_key": server._apikey}
        res = RequestUtils().get_res(url, params=params)
        if not res:
            return False, "read target userdata failed"

        body = (res.json().get("UserData") or {}).copy()
        body["ItemId"] = target_item.item_id
        body["PlaybackPositionTicks"] = max(state.progress_ms, 0) * 10000
        body["Played"] = state.watched
        body["PlayCount"] = max(self._safe_int(body.get("PlayCount"), 0), 1 if state.watched else 0)
        body["PlayedPercentage"] = 100 if state.watched else round(state.percent, 2)
        body["LastPlayedDate"] = state.played_at or datetime.now(timezone.utc).isoformat()
        if state.watched:
            body["PlaybackPositionTicks"] = 0
        headers = {
            "Content-Type": "application/json",
            "X-Emby-Token": server._apikey
        }
        update_url = f"{server._host}UserItems/{target_item.item_id}/UserData"
        update_res = RequestUtils(headers=headers, content_type="application/json").post_res(
            update_url, json=body
        )
        if not update_res or update_res.status_code >= 300:
            code = update_res.status_code if update_res else "n/a"
            return False, f"write jellyfin failed ({code})"
        return True, f"jellyfin:{target_item.item_id}"

    def _apply_to_plex(
        self, target_service: ServiceInfo, target_item: MediaServerItem, state: NormalizedState
    ) -> Tuple[bool, str]:
        server = target_service.instance
        base = server._host.rstrip("/")
        params = {
            "identifier": "com.plexapp.plugins.library",
            "key": target_item.item_id,
            "X-Plex-Token": server._token
        }

        if state.watched and self._sync_watched:
            url = f"{base}/:/scrobble"
            res = RequestUtils().put_res(url, params=params)
            if res and res.status_code < 300:
                return True, f"plex watched:{target_item.item_id}"
            return False, f"plex scrobble failed ({res.status_code if res else 'n/a'})"

        if state.progress_ms <= 0:
            if not self._sync_watched:
                return True, "nothing to clear"
            url = f"{base}/:/unscrobble"
            res = RequestUtils().put_res(url, params=params)
            if res and res.status_code < 300:
                return True, f"plex unwatch:{target_item.item_id}"
            return False, f"plex unscrobble failed ({res.status_code if res else 'n/a'})"

        clear_res = RequestUtils().put_res(f"{base}/:/unscrobble", params=params)
        if clear_res and clear_res.status_code >= 300:
            logger.warning(f"观看进度同步：Plex 预清除已看状态失败 {clear_res.status_code}")

        progress_params = params.copy()
        progress_params["time"] = max(state.progress_ms, max(60001, self._min_progress_seconds * 1000 + 1))
        progress_params["state"] = "stopped"
        progress_url = f"{base}/:/progress"
        progress_res = RequestUtils().put_res(progress_url, params=progress_params)
        if progress_res and progress_res.status_code < 300:
            return True, f"plex progress:{target_item.item_id}"
        return False, f"plex progress failed ({progress_res.status_code if progress_res else 'n/a'})"

    def _should_sync(self, progress_ms: int, duration_ms: int, watched: bool) -> bool:
        if watched:
            return self._sync_watched
        if not self._sync_progress:
            return False
        if progress_ms < self._min_progress_seconds * 1000:
            return False
        if duration_ms and progress_ms >= duration_ms:
            return False
        return True

    def _target_needs_update(
        self, target_service: ServiceInfo, target_item: MediaServerItem, state: NormalizedState
    ) -> Tuple[bool, str]:
        current = self._read_current_target_state(target_service, target_item)
        if not current:
            return True, "无法读取目标当前状态"

        current_watched = bool(current.get("watched"))
        current_progress_ms = self._safe_int(current.get("progress_ms"), 0)

        if state.watched:
            if current_watched and current_progress_ms == 0:
                return False, "目标已是已看"
            return True, "需要标记已看"

        if current_watched:
            return True, "目标当前为已看，需要改成继续观看"

        delta_ms = abs(current_progress_ms - state.progress_ms)
        if delta_ms < self._progress_delta_seconds * 1000:
            return False, f"目标进度差仅 {int(delta_ms / 1000)} 秒"
        return True, "进度变化达到阈值"

    def _read_current_target_state(
        self, target_service: ServiceInfo, target_item: MediaServerItem
    ) -> Optional[Dict[str, Any]]:
        if target_service.type == "plex":
            plex = target_service.instance.get_plex()
            if not plex:
                return None
            try:
                item = plex.fetchItem(target_item.item_id)
                return {
                    "watched": bool(getattr(item, "isPlayed", False)),
                    "progress_ms": self._safe_int(getattr(item, "viewOffset", 0), 0)
                }
            except Exception as err:
                logger.error(f"观看进度同步：读取 Plex 目标状态失败 {err}")
                return None

        server = target_service.instance
        url = f"{server._host}Users/{server.user}/Items/{target_item.item_id}"
        params = {"api_key": server._apikey}
        res = RequestUtils().get_res(url, params=params)
        if not res:
            return None
        user_data = (res.json() or {}).get("UserData") or {}
        return {
            "watched": bool(user_data.get("Played")),
            "progress_ms": int(self._safe_int(user_data.get("PlaybackPositionTicks"), 0) / 10000)
        }

    def _is_duplicate_source_event(self, state: NormalizedState) -> bool:
        key = self._make_write_key(state.source_server, state.source_item_id, state)
        if self._seen_recently(key):
            logger.debug("观看进度同步：源事件命中短期缓存，跳过")
            return True
        return False

    def _make_write_key(self, server_name: str, item_id: Optional[str], state: NormalizedState) -> str:
        bucket = int(state.progress_ms / 1000) if state.progress_ms else 0
        return f"{server_name}|{item_id}|{int(state.watched)}|{bucket}"

    def _remember_write(self, key: str):
        with self._lock:
            self._recent_writes[key] = time.time()
            self._cleanup_caches_locked()

    def _seen_recently(self, key: str) -> bool:
        with self._lock:
            ts = self._recent_writes.get(key)
            if not ts:
                return False
            return (time.time() - ts) < self._write_ttl_seconds

    def _cleanup_caches(self, force: bool = False):
        with self._lock:
            if force:
                self._recent_writes = {}
                self._recent_failures = {}
                return
            self._cleanup_caches_locked()

    def _cleanup_caches_locked(self):
        now = time.time()
        self._recent_writes = {
            key: ts for key, ts in self._recent_writes.items()
            if (now - ts) < self._write_ttl_seconds
        }
        self._recent_failures = {
            key: ts for key, ts in self._recent_failures.items()
            if (now - ts) < self._write_ttl_seconds
        }

    def _record_history(self, title: str, subtitle: str):
        history = self.get_data("history") or []
        history.insert(0, {
            "title": title,
            "subtitle": subtitle,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        self.save_data("history", history[:self._max_history])

    @staticmethod
    def _extract_provider_ids_from_plex_guids(guids: List[dict]) -> Dict[str, Optional[str]]:
        ret = {"tmdb": None, "imdb": None, "tvdb": None}
        for guid in guids or []:
            value = guid.get("id")
            if not value or "://" not in value:
                continue
            provider, provider_id = value.split("://", 1)
            if provider in ret:
                ret[provider] = provider_id
        return ret

    @staticmethod
    def _to_iso(value: Any) -> Optional[str]:
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return None

    @staticmethod
    def _coerce_int(value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _state_label(state: NormalizedState) -> str:
        if state.media_kind == "episode":
            season = state.season or 0
            episode = state.episode or 0
            return (
                f"{state.series_title or state.title} "
                f"S{season:02d}E{episode:02d} "
                f"{'已看' if state.watched else f'{int(state.progress_ms / 1000)}s'}"
            )
        return f"{state.title} {'已看' if state.watched else f'{int(state.progress_ms / 1000)}s'}"
