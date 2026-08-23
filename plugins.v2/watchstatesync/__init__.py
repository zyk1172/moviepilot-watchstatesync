import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

from app.core.event import Event, eventmanager
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaServerItem, ServiceInfo, WebhookEventInfo
from app.schemas.types import EventType
from app.utils.http import RequestUtils


class StateOperation:
    """同步操作的显式语义，避免用 watched/progress 的组合表达取消已看。"""

    WATCHED = "watched"
    UNWATCHED = "unwatched"
    PROGRESS = "progress"


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
    user_id: Optional[str] = None
    operation: str = ""
    series_tmdb_id: Optional[int] = None
    series_imdb_id: Optional[str] = None
    series_tvdb_id: Optional[str] = None
    episode_tmdb_id: Optional[int] = None
    episode_imdb_id: Optional[str] = None
    episode_tvdb_id: Optional[str] = None
    source_event_at: float = 0.0
    source_sequence: int = 0


class WatchStateSync(_PluginBase):
    plugin_name = "观看进度同步"
    plugin_desc = "将 Plex 的已看状态与继续观看进度单向同步到 Jellyfin。"
    plugin_icon = "sync_file.png"
    plugin_version = "1.2.1"
    plugin_author = "OpenAI Codex"
    author_url = "https://openai.com"
    plugin_config_prefix = "watchstatesync_"
    plugin_order = 40
    auth_level = 1

    _enabled = False
    _server_a = ""
    _server_b = ""
    _allowed_users: List[str] = []
    _sync_watched = True
    _sync_progress = True
    _min_progress_seconds = 60
    _progress_delta_seconds = 30
    _watched_percent = 90
    _notify_on_sync = False
    _dry_run = False
    _poll_plex = True
    _poll_interval_minutes = 5
    _use_websocket = True
    _jellyfin_username = ""
    _jellyfin_password = ""

    # WebSocket、Webhook、定时任务和手动 API 可能同时触发同步。持久化数据必须
    # 使用可重入锁做 get-modify-save，避免不同线程互相覆盖。
    _lock = threading.RLock()
    _sync_lock = threading.RLock()
    _poll_lock = threading.RLock()
    _recent_writes: Dict[str, float] = {}
    _write_ttl_seconds = 180
    _max_history = 30
    _max_source_events = 2000
    _plex_history_page_size = 50
    _plex_history_max_pages = 200
    _plex_history_overlap_seconds = 2
    _outbox_retry_base_seconds = 60
    _outbox_retry_max_seconds = 3600
    _jellyfin_auth_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
    _jellyfin_auth_ttl_seconds = 3600
    _plex_alert_listener: Any = None
    _plex_sessions: Dict[str, Dict[str, Any]] = {}
    _plex_user_identity_cache: Dict[str, List[str]] = {}
    _plex_user_servers: Dict[Tuple[str, str], Any] = {}
    _source_sequence = 0

    def init_plugin(self, config: dict = None):
        config = config or {}
        self._stop_plex_alert_listener()
        self._jellyfin_auth_cache = {}
        self._plex_sessions = {}
        self._plex_user_identity_cache = {}
        self._plex_user_servers = {}
        self._source_sequence = self._safe_int(self.get_data("source_sequence"), 0)

        self._enabled = bool(config.get("enabled", False))
        self._server_a = (config.get("server_a") or "").strip()
        self._server_b = (config.get("server_b") or "").strip()
        self._sync_watched = bool(config.get("sync_watched", True))
        self._sync_progress = bool(config.get("sync_progress", True))
        self._min_progress_seconds = self._safe_int(config.get("min_progress_seconds"), 60)
        self._progress_delta_seconds = self._safe_int(config.get("progress_delta_seconds"), 30)
        self._watched_percent = self._safe_int(config.get("watched_percent"), 90)
        self._notify_on_sync = bool(config.get("notify_on_sync", False))
        self._dry_run = bool(config.get("dry_run", False))
        self._poll_plex = bool(config.get("poll_plex", True))
        self._poll_interval_minutes = max(1, self._safe_int(config.get("poll_interval_minutes"), 5))
        self._use_websocket = bool(config.get("use_websocket", True))
        self._jellyfin_username = (config.get("jellyfin_username") or "").strip()
        self._jellyfin_password = config.get("jellyfin_password") or ""
        self._allowed_users = [
            user.strip() for user in (config.get("allowed_users") or "").split(",") if user.strip()
        ]
        self._cleanup_caches()
        if self._enabled and self._use_websocket and self._has_plex_source():
            self._start_plex_alert_listener()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [{
            "path": "/clear_history",
            "endpoint": self.clear_history,
            "methods": ["POST"],
            "summary": "清除插件历史数据",
            "description": "清空同步记录、Outbox，并重置 Plex 轮询历史游标与继续观看快照。"
        }, {
            "path": "/sync_now",
            "endpoint": self.sync_now,
            "methods": ["POST"],
            "summary": "立即轮询 Plex",
            "description": "立即执行一次 Plex history、Continue Watching 和失败重试。"
        }, {
            "path": "/diagnostics",
            "endpoint": self.get_diagnostics,
            "methods": ["GET"],
            "summary": "查看同步诊断",
            "description": "返回最近轮询、匹配、写回和验证状态。"
        }]

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        if not self._has_plex_source():
            return []
        if not self._poll_plex and not self._use_websocket:
            return []
        return [{
            "id": "WatchStateSync_poll_plex",
            "name": "观看进度同步轮询 Plex",
            "trigger": "interval",
            "func": self.poll_plex_sources,
            "kwargs": {"minutes": self._poll_interval_minutes}
        }]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        configs = MediaServerHelper().get_configs().values()
        plex_items = [
            {"title": config.name, "value": config.name}
            for config in configs
            if config.type == "plex"
        ]
        jellyfin_items = [
            {"title": config.name, "value": config.name}
            for config in configs
            if config.type == "jellyfin"
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
                                        "label": "Plex 源服务器",
                                        "items": plex_items,
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
                                        "label": "Jellyfin 目标服务器",
                                        "items": jellyfin_items,
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
                                        "label": "允许同步的用户名或 Plex accountId（逗号分隔，可留空）",
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
                                        "model": "poll_plex",
                                        "label": "Plex 轮询 reconciliation（WebSocket 开启时始终保留）"
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
                                        "model": "use_websocket",
                                        "label": "使用 Plex 本地 WebSocket（低延迟加速，轮询始终兜底）"
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
                                    "component": "VTextField",
                                    "props": {
                                        "model": "poll_interval_minutes",
                                        "label": "Plex 轮询间隔（分钟）"
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
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "jellyfin_username",
                                        "label": "Jellyfin 用户名（目标用户上下文）"
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "jellyfin_password",
                                        "label": "Jellyfin 密码（目标用户上下文）",
                                        "type": "password"
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
                                        "text": "当前仓库为 Plex -> Jellyfin 单向同步。开启继续观看进度时必须填写 Jellyfin 用户名和密码；搜索、读取、写回和验证会使用同一个目标用户。"
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
            "sync_watched": True,
            "sync_progress": True,
            "min_progress_seconds": 60,
            "progress_delta_seconds": 30,
            "watched_percent": 90,
            "allowed_users": "",
            "notify_on_sync": False,
            "dry_run": False,
            "poll_plex": True,
            "use_websocket": True,
            "poll_interval_minutes": 5,
            "jellyfin_username": "",
            "jellyfin_password": ""
        }

    def get_page(self) -> List[dict]:
        history = self.get_data("history") or []
        diagnostics = self.get_data("diagnostics") or {}
        poll_status = diagnostics.get("poll") or {}
        history_status = diagnostics.get("plex_history") or {}
        resume_status = diagnostics.get("plex_resume") or {}
        write_status = diagnostics.get("jellyfin_write") or {}
        diagnostic_text = (
            f"轮询：{poll_status.get('status', '尚未运行')}"
            f" | History：{history_status.get('read', 0)} 条"
            f" | Continue Watching：{resume_status.get('read', 0)} 条"
            f" | 写回成功/失败：{write_status.get('success', 0)}/{write_status.get('failed', 0)}"
        )
        if not history:
            history_rows = [{
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "text": "还没有同步记录。确认已经正确选择 Plex 源服务器和 Jellyfin 目标服务器。"
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
                            f"方向：Plex -> Jellyfin | "
                            f"服务器：{self._server_a or '-'} -> {self._server_b or '-'}"
                        )
                    }
                ]
            },
            {
                "component": "VCard",
                "props": {"class": "mt-3", "variant": "tonal"},
                "content": [
                    {"component": "VCardTitle", "text": "运行诊断"},
                    {"component": "VCardText", "text": diagnostic_text},
                    {
                        "component": "VCardActions",
                        "content": [{
                            "component": "VForm",
                            "props": {
                                "action": "/api/v1/plugin/WatchStateSync/sync_now",
                                "method": "POST"
                            },
                            "content": [{
                                "component": "VBtn",
                                "props": {
                                    "type": "submit",
                                    "variant": "tonal",
                                    "text": "立即同步一次"
                                }
                            }]
                        }]
                    }
                ]
            },
            {
                "component": "VCard",
                "props": {"class": "mt-3"},
                "content": [
                    {"component": "VCardTitle", "text": "历史数据"},
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "warning",
                                    "variant": "tonal",
                                    "text": "清除后会同时重置最近同步记录和 Plex 轮询游标。下一轮轮询会重新处理最近一批 Plex 历史与继续观看数据。"
                                }
                            },
                            {
                                "component": "VForm",
                                "props": {
                                    "action": "/api/v1/plugin/WatchStateSync/clear_history",
                                    "method": "POST",
                                    "class": "mt-3"
                                },
                                "content": [{
                                    "component": "VBtn",
                                    "props": {
                                        "type": "submit",
                                        "color": "error",
                                        "variant": "tonal",
                                        "text": "清除历史数据"
                                    }
                                }]
                            }
                        ]
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
        if not source_server or source_server != self._server_a:
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

        result = self._sync_state_to_target(source_server, target_server, target_service, state)
        if result == "success" and self._notify_on_sync:
            self.post_message(
                title=f"{source_server} -> {target_server} 成功",
                text=self._state_label(state)
            )

    def clear_history(self):
        cleared = self._clear_history_data()
        return {
            "success": True,
            "message": "已清除历史数据",
            "data": cleared
        }

    def sync_now(self):
        if not self._enabled:
            return {"success": False, "message": "插件未启用", "data": self.get_diagnostics()}
        self.poll_plex_sources()
        return {
            "success": True,
            "message": "已执行一次同步轮询",
            "data": self.get_diagnostics(),
        }

    def get_diagnostics(self):
        return {
            "success": True,
            "data": self.get_data("diagnostics") or {},
        }

    def stop_service(self):
        self._stop_plex_alert_listener()
        self._cleanup_caches(force=True)

    def poll_plex_sources(self):
        """串行执行轮询，避免手动同步与调度任务互相覆盖持久化状态。"""
        with self._poll_lock:
            self._poll_plex_sources_locked()

    def _poll_plex_sources_locked(self):
        if not self._enabled:
            return
        if not self._server_a or not self._server_b:
            return
        source_service = self._get_service(self._server_a)
        target_service = self._get_service(self._server_b)
        if not source_service or not target_service:
            return
        if source_service.type != "plex":
            logger.warning("观看进度同步：Plex 源服务器配置无效")
            return
        if target_service.type != "jellyfin":
            logger.warning("观看进度同步：Jellyfin 目标服务器配置无效")
            return
        self._record_diagnostic(
            "poll",
            status="running",
            started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            source=self._server_a,
            target=self._server_b,
        )
        try:
            if self._use_websocket:
                # WebSocket 只负责低延迟加速；轮询始终作为 reconciliation，
                # 即使配置曾关闭“无 Plex Pass 时轮询”也不能让实时监听失去兜底。
                self._ensure_plex_alert_listener(source_service)
            if self._poll_plex or self._use_websocket:
                self._poll_single_plex_source(source_service, target_service)
            self._reconcile_plex_sessions(source_service, target_service)
            self._process_outbox(target_service)
            self._record_diagnostic(
                "poll",
                status="ok",
                finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
        except Exception as err:
            self._record_diagnostic(
                "poll",
                status="failed",
                finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                error=str(err),
            )
            logger.error(f"观看进度同步：轮询 Plex 源 {self._server_a} 失败 {err}")

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
        return self._server_b if source_server == self._server_a else None

    def _build_state(self, service: ServiceInfo, event_info: WebhookEventInfo) -> Optional[NormalizedState]:
        if service.type == "plex":
            return self._build_plex_state(service, event_info)
        return None

    def _has_plex_source(self) -> bool:
        if not self._server_a or not self._server_b:
            return False
        source = MediaServerHelper().get_service(name=self._server_a)
        target = MediaServerHelper().get_service(name=self._server_b)
        return bool(
            source
            and source.type == "plex"
            and target
            and target.type == "jellyfin"
            and self._resolve_target_server(self._server_a)
        )

    def _poll_single_plex_source(self, source_service: ServiceInfo, target_service: ServiceInfo):
        try:
            self._poll_plex_history(source_service, target_service)
        except Exception as err:
            self._record_diagnostic("plex_history", ok=False, error=str(err))
            logger.error(f"观看进度同步：Plex history 轮询失败 {err}")
        try:
            self._poll_plex_resume(source_service, target_service)
        except Exception as err:
            self._record_diagnostic("plex_resume", ok=False, error=str(err))
            logger.error(f"观看进度同步：Plex Continue Watching 轮询失败 {err}")

    def _poll_plex_history(self, source_service: ServiceInfo, target_service: ServiceInfo):
        state_key = f"plex_history_ts::{source_service.name}"
        processed_key = f"plex_history_processed::{source_service.name}"
        last_seen = self._safe_int(self.get_data(state_key), 0)
        processed_ids = set(self.get_data(processed_key) or [])

        # 首次运行不做全量回填，只处理最近一天；之后回退一个小窗口，
        # 让同一秒内延迟出现的 history 事件仍有机会返回，再由 event-id 去重。
        since_ts = last_seen if last_seen else int(time.time()) - 24 * 3600
        query_since = max(since_ts - self._plex_history_overlap_seconds, 0) if last_seen else since_ts
        history = self._get_plex_history(source_service, since_ts=query_since)
        self._record_diagnostic(
            "plex_history",
            ok=True,
            http_status=200,
            read=len(history),
        )
        if not history:
            return

        max_seen = last_seen
        all_ok = True

        # 同一秒内可能有多条记录，必须用事件 ID 去重，不能只靠时间戳。
        new_items = []
        for item in history:
            viewed_at = self._safe_int(item.get("viewedAt"), 0)
            event_id = self._history_event_id(item)
            if viewed_at >= query_since and event_id not in processed_ids:
                new_items.append(item)

        for item in sorted(new_items, key=lambda x: self._safe_int(x.get("viewedAt"), 0)):
            viewed_at = self._safe_int(item.get("viewedAt"), 0)
            event_id = self._history_event_id(item)
            if event_id in processed_ids:
                max_seen = max(max_seen, viewed_at)
                continue
            try:
                state = self._build_plex_history_state(source_service, item)
            except Exception:
                all_ok = False
                continue
            if not state:
                # 不需要同步（例如未看完且未达到阈值）也视为已消费，避免反复扫描。
                processed_ids.add(event_id)
                max_seen = max(max_seen, viewed_at)
                continue
            result = self._sync_state_to_target(source_service.name, target_service.name, target_service, state)
            if result == "failed":
                all_ok = False
                continue
            processed_ids.add(event_id)
            max_seen = max(max_seen, viewed_at)

        # 即使有失败也保存已成功的 event-id，避免重复处理；但游标只在整批成功时推进，
        # 否则失败的那条会在下一轮重新被扫描到。
        self.save_data(processed_key, sorted(processed_ids)[-self._max_source_events:])
        if all_ok and max_seen > last_seen:
            self.save_data(state_key, max_seen)

    def _poll_plex_resume(self, source_service: ServiceInfo, target_service: ServiceInfo):
        resume_items = source_service.instance.get_resume(num=50) or []
        self._record_diagnostic("plex_resume", ok=True, read=len(resume_items))
        snapshot_key = f"plex_resume_snapshot::{source_service.name}"
        raw_snapshot = self.get_data(snapshot_key) or {}
        last_snapshot = {str(key): value for key, value in raw_snapshot.items()}
        current_snapshot: Dict[str, Dict[str, Any]] = {}

        for resume in resume_items:
            item_id = getattr(resume, "id", None) or getattr(resume, "ratingKey", None)
            if not item_id:
                continue
            state = self._build_plex_resume_state(source_service, item_id)
            if not state:
                continue
            if not self._user_allowed(state):
                logger.debug("观看进度同步：Plex 继续观看用户不在允许列表，跳过")
                continue
            snapshot_id = state.source_item_id or str(self._plex_item_reference(item_id))
            sec = int(state.progress_ms / 1000)
            last_entry = last_snapshot.get(snapshot_id)
            if last_entry is None:
                # 兼容 1.2.0 使用 int 作为快照 key 的旧数据。
                last_entry = last_snapshot.get(str(item_id))
            last_sec = self._resume_snapshot_seconds(last_entry)
            snapshot_entry = {
                "seconds": sec,
                "user_id": state.user_id,
                "user_name": state.user_name,
            }
            if last_sec >= 0 and abs(sec - last_sec) < self._progress_delta_seconds:
                current_snapshot[snapshot_id] = snapshot_entry
                continue
            if last_sec < 0 and sec < self._min_progress_seconds:
                current_snapshot[snapshot_id] = snapshot_entry
                continue
            result = self._sync_state_to_target(source_service.name, target_service.name, target_service, state)
            # 失败时不推进快照，下一轮继续重试；成功/跳过才更新快照。
            if result != "failed":
                current_snapshot[snapshot_id] = snapshot_entry

        # Continue Watching 消失不等于已看。只回源确认 Plex 的最终 isPlayed/百分比，
        # 确认完成后才生成 WATCHED 操作；未完成的条目直接丢弃本轮快照。
        for item_id in set(last_snapshot) - set(current_snapshot):
            previous = last_snapshot.get(item_id)
            previous_user_id = previous.get("user_id") if isinstance(previous, dict) else None
            previous_user_name = previous.get("user_name") if isinstance(previous, dict) else None
            state = self._build_plex_websocket_stopped_state(
                source_service, item_id, previous_user_id, previous_user_name
            )
            if not state or self._state_operation(state) != StateOperation.WATCHED:
                continue
            if not self._user_allowed(state):
                continue
            result = self._sync_state_to_target(source_service.name, target_service.name, target_service, state)
            if result == "failed":
                current_snapshot[item_id] = previous if isinstance(previous, dict) else {
                    "seconds": self._resume_snapshot_seconds(previous),
                    "user_id": previous_user_id,
                    "user_name": previous_user_name,
                }

        self.save_data(snapshot_key, current_snapshot)

    def _resume_snapshot_seconds(self, entry: Any) -> int:
        if isinstance(entry, dict):
            return self._safe_int(entry.get("seconds"), -1)
        return self._safe_int(entry, -1)

    @staticmethod
    def _plex_item_reference(item_id: Any) -> Any:
        """把 Plex 的 ratingKey 转成 PlexAPI 能正确识别的引用。

        Plex WebSocket 通知通常只给数字字符串（例如 ``"5033"``），而
        PlexAPI 只有在收到 Python int 时才会自动补全 library metadata 路径。
        history/WebSocket 若带有 ``/library/metadata/...`` 则保留原路径。
        """
        if isinstance(item_id, int):
            return item_id
        text = str(item_id).strip() if item_id is not None else ""
        if text.isdigit():
            return int(text)
        return text

    @staticmethod
    def _plex_source_item_id(item_id: Any) -> str:
        """为 history、polling 和 WebSocket 生成同一形式的源条目标识。"""
        text = str(item_id).strip() if item_id is not None else ""
        marker = "/library/metadata/"
        if marker in text:
            return text.split(marker, 1)[1].split("/", 1)[0]
        return text

    def _get_plex_history(self, source_service: ServiceInfo, since_ts: int = 0, limit: Optional[int] = None) -> List[dict]:
        server = source_service.instance
        url = f"{server._host.rstrip('/')}/status/sessions/history/all"
        headers = {
            "Accept": "application/json",
            "X-Plex-Token": server._token,
        }
        items: List[dict] = []
        start = 0
        page_size = self._plex_history_page_size
        max_pages = self._plex_history_max_pages
        if limit:
            max_pages = max(1, (limit + page_size - 1) // page_size)

        for _ in range(max_pages):
            params = {
                "sort": "viewedAt:desc",
            }
            if since_ts:
                params["viewedAt>"] = since_ts
            page_headers = {
                **headers,
                "X-Plex-Container-Start": str(start),
                "X-Plex-Container-Size": str(page_size),
            }
            res = RequestUtils(headers=page_headers).get_res(url, params=params)
            if not res or res.status_code >= 300:
                code = res.status_code if res else "n/a"
                raise RuntimeError(f"Plex history request failed at start={start} ({code})")
            try:
                payload = res.json() or {}
            except Exception as err:
                raise RuntimeError(f"Plex history returned invalid JSON at start={start}: {err}") from err
            metadata = (((payload.get("MediaContainer") or {}).get("Metadata")) or [])
            if not isinstance(metadata, list):
                raise RuntimeError(f"Plex history returned invalid Metadata at start={start}")
            if not metadata:
                break
            for item in metadata:
                viewed_at = self._safe_int(item.get("viewedAt"), 0)
                if since_ts and viewed_at < since_ts:
                    return items
                items.append(item)
            if len(metadata) < page_size:
                break
            start += len(metadata)
        return items

    @staticmethod
    def _history_event_id(item: dict) -> str:
        stable_id = (
            item.get("historyKey")
            or item.get("historyId")
            or item.get("id")
            or item.get("ratingKey")
            or item.get("key")
        )
        return f"{item.get('viewedAt')}:{stable_id}:{item.get('accountID') or ''}"

    def _fetch_plex_metadata_and_state_item(
        self,
        source_service: ServiceInfo,
        item_reference: Any,
        user_id: Optional[str] = None,
        user_name: Optional[str] = None,
        use_user_state: bool = True,
    ) -> Tuple[Any, Any, Any]:
        """返回 base Plex、媒体元数据条目和指定用户状态条目。"""
        plex = source_service.instance.get_plex()
        metadata_item = plex.fetchItem(item_reference)
        if not use_user_state:
            return plex, metadata_item, metadata_item
        state_plex = self._get_plex_user_server(
            source_service, plex, user_id, user_name
        )
        if state_plex is None:
            raise RuntimeError(
                f"Plex user state context unavailable for {user_name or user_id or 'unknown user'}"
            )
        state_item = metadata_item if state_plex is plex else state_plex.fetchItem(item_reference)
        return plex, metadata_item, state_item

    def _build_plex_history_state(self, source_service: ServiceInfo, history_item: dict) -> Optional[NormalizedState]:
        item_key = history_item.get("key") or history_item.get("ratingKey")
        if not item_key:
            return None
        item_reference = self._plex_item_reference(item_key)
        plex = source_service.instance.get_plex()
        user_id, user_name = self._plex_user_fields(
            plex,
            history_item.get("accountID"),
            history_item.get("userName") or history_item.get("username")
        )
        user_name = user_name or self._plex_default_user_name(source_service.name)
        try:
            _, item, state_item = self._fetch_plex_metadata_and_state_item(
                source_service, item_reference, user_id, user_name
            )
        except Exception as err:
            logger.error(f"观看进度同步：读取 Plex 历史条目失败 {err}")
            raise

        media_kind = "episode" if getattr(item, "type", None) == "episode" else "movie"
        ids = self._get_plex_provider_ids(plex, item, media_kind)
        progress_ms = self._safe_int(getattr(state_item, "viewOffset", 0), 0)
        duration_ms = self._safe_int(getattr(item, "duration", 0), 0)
        percent = round((progress_ms / duration_ms) * 100, 2) if progress_ms and duration_ms else 0.0
        watched = bool(getattr(state_item, "isPlayed", False))
        if not watched and percent >= self._watched_percent:
            watched = True
        operation = StateOperation.WATCHED if watched else StateOperation.PROGRESS
        if watched:
            progress_ms = 0
            percent = 100.0

        if not watched and progress_ms < self._min_progress_seconds * 1000:
            return None

        viewed_at = self._safe_int(history_item.get("viewedAt"), 0)
        played_at = datetime.fromtimestamp(viewed_at, tz=timezone.utc).isoformat() if viewed_at else self._to_iso(getattr(item, "lastViewedAt", None))
        source_event_at, source_sequence = self._new_source_event_meta(time.time())
        return NormalizedState(
            source_server=source_service.name,
            source_type="plex",
            event_type="poll.history",
            user_name=user_name,
            media_kind=media_kind,
            title=getattr(item, "title", None),
            original_title=getattr(item, "originalTitle", None),
            series_title=getattr(item, "grandparentTitle", None) if media_kind == "episode" else None,
            year=self._coerce_int(getattr(item, "year", None)),
            **self._provider_state_fields(ids, media_kind),
            season=self._coerce_int(getattr(item, "parentIndex", None)),
            episode=self._coerce_int(getattr(item, "index", None)),
            source_item_id=self._plex_source_item_id(item_reference),
            progress_ms=progress_ms,
            duration_ms=duration_ms,
            watched=watched,
            percent=percent,
            played_at=played_at,
            user_id=user_id,
            operation=operation,
            source_event_at=source_event_at,
            source_sequence=source_sequence,
        )

    def _build_plex_resume_state(
        self,
        source_service: ServiceInfo,
        item_id: Any,
        user_id: Optional[str] = None,
        user_name: Optional[str] = None,
        fallback_to_token_owner: bool = True,
        progress_ms_override: Optional[int] = None,
    ) -> Optional[NormalizedState]:
        plex = source_service.instance.get_plex()
        item_reference = self._plex_item_reference(item_id)
        try:
            item = plex.fetchItem(item_reference)
        except Exception as err:
            logger.error(f"观看进度同步：读取 Plex 继续观看条目失败 {err}")
            return None

        resolved_user_id, resolved_user_name = self._plex_user_fields(
            plex,
            user_id or getattr(item, "accountID", None),
            user_name or getattr(item, "userName", None) or getattr(item, "username", None)
        )
        resolved_user_name = resolved_user_name or (
            self._plex_default_user_name(source_service.name) if fallback_to_token_owner else None
        )
        state_item = item
        if progress_ms_override is None:
            try:
                state_plex = self._get_plex_user_server(
                    source_service, plex, resolved_user_id, resolved_user_name
                )
                if state_plex is None:
                    return None
                if state_plex is not plex:
                    state_item = state_plex.fetchItem(item_reference)
            except Exception as err:
                logger.error(f"观看进度同步：读取 Plex 用户状态失败 {err}")
                return None

        progress_ms = (
            self._safe_int(progress_ms_override, 0)
            if progress_ms_override is not None
            else self._safe_int(getattr(state_item, "viewOffset", 0), 0)
        )
        duration_ms = self._safe_int(
            getattr(item, "duration", 0) or getattr(state_item, "duration", 0), 0
        )
        if progress_ms < self._min_progress_seconds * 1000 or not duration_ms:
            return None
        percent = round((progress_ms / duration_ms) * 100, 2)
        if percent >= self._watched_percent:
            return None
        media_kind = "episode" if getattr(item, "type", None) == "episode" else "movie"
        ids = self._get_plex_provider_ids(plex, item, media_kind)
        source_event_at, source_sequence = self._new_source_event_meta(time.time())
        return NormalizedState(
            source_server=source_service.name,
            source_type="plex",
            event_type="poll.resume",
            user_name=resolved_user_name,
            media_kind=media_kind,
            title=getattr(item, "title", None),
            original_title=getattr(item, "originalTitle", None),
            series_title=getattr(item, "grandparentTitle", None) if media_kind == "episode" else None,
            year=self._coerce_int(getattr(item, "year", None)),
            **self._provider_state_fields(ids, media_kind),
            season=self._coerce_int(getattr(item, "parentIndex", None)),
            episode=self._coerce_int(getattr(item, "index", None)),
            source_item_id=self._plex_source_item_id(item_reference),
            progress_ms=progress_ms,
            duration_ms=duration_ms,
            watched=False,
            percent=percent,
            played_at=self._to_iso(getattr(state_item, "lastViewedAt", None)),
            user_id=resolved_user_id,
            operation=StateOperation.PROGRESS,
            source_event_at=source_event_at,
            source_sequence=source_sequence,
        )

    def _build_plex_websocket_stopped_state(
        self,
        source_service: ServiceInfo,
        item_id: Any,
        account_id: Optional[str] = None,
        user_name: Optional[str] = None,
        fallback_to_token_owner: bool = True,
        progress_ms_override: Optional[int] = None,
        watched_override: Optional[bool] = None,
    ) -> Optional[NormalizedState]:
        plex = source_service.instance.get_plex()
        item_reference = self._plex_item_reference(item_id)
        try:
            item = plex.fetchItem(item_reference)
        except Exception as err:
            logger.error(f"观看进度同步：读取 Plex WebSocket 停止条目失败 {err}")
            return None

        resolved_user_id, resolved_user_name = self._plex_user_fields(
            plex,
            account_id or getattr(item, "accountID", None),
            user_name or getattr(item, "userName", None) or getattr(item, "username", None)
        )
        resolved_user_name = resolved_user_name or (
            self._plex_default_user_name(source_service.name) if fallback_to_token_owner else None
        )
        state_item = item
        if progress_ms_override is None:
            try:
                state_plex = self._get_plex_user_server(
                    source_service, plex, resolved_user_id, resolved_user_name
                )
                if state_plex is None:
                    return None
                if state_plex is not plex:
                    state_item = state_plex.fetchItem(item_reference)
            except Exception as err:
                logger.error(f"观看进度同步：读取 Plex 用户停止状态失败 {err}")
                return None

        progress_ms = (
            self._safe_int(progress_ms_override, 0)
            if progress_ms_override is not None
            else self._safe_int(getattr(state_item, "viewOffset", 0), 0)
        )
        duration_ms = self._safe_int(
            getattr(item, "duration", 0) or getattr(state_item, "duration", 0), 0
        )
        percent = round((progress_ms / duration_ms) * 100, 2) if progress_ms and duration_ms else 0.0
        if watched_override is not None:
            watched = bool(watched_override)
        elif progress_ms_override is not None:
            # 通知中的进度属于当前 session；不能再读取 owner token 的 isPlayed。
            watched = percent >= self._watched_percent
        else:
            watched = bool(getattr(state_item, "isPlayed", False)) or percent >= self._watched_percent
        operation = StateOperation.WATCHED if watched else StateOperation.PROGRESS
        if not watched and progress_ms < self._min_progress_seconds * 1000:
            return None
        if watched:
            progress_ms = 0
            percent = 100.0
        media_kind = "episode" if getattr(item, "type", None) == "episode" else "movie"
        ids = self._get_plex_provider_ids(plex, item, media_kind)
        source_event_at, source_sequence = self._new_source_event_meta(time.time())
        return NormalizedState(
            source_server=source_service.name,
            source_type="plex",
            event_type="websocket.stop",
            user_name=resolved_user_name,
            media_kind=media_kind,
            title=getattr(item, "title", None),
            original_title=getattr(item, "originalTitle", None),
            series_title=getattr(item, "grandparentTitle", None) if media_kind == "episode" else None,
            year=self._coerce_int(getattr(item, "year", None)),
            **self._provider_state_fields(ids, media_kind),
            season=self._coerce_int(getattr(item, "parentIndex", None)),
            episode=self._coerce_int(getattr(item, "index", None)),
            source_item_id=self._plex_source_item_id(item_reference),
            progress_ms=progress_ms,
            duration_ms=duration_ms,
            watched=watched,
            percent=percent,
            played_at=self._to_iso(getattr(state_item, "lastViewedAt", None)),
            user_id=resolved_user_id,
            operation=operation,
            source_event_at=source_event_at,
            source_sequence=source_sequence,
        )


    def _get_plex_provider_ids(self, plex: Any, item: Any, media_kind: str) -> Dict[str, Optional[str]]:
        """同时保留剧集的 Series ID 和 Episode ID，避免跨层级匹配。"""
        item_guids = self._plex_guid_dicts(item)
        if media_kind == "episode":
            episode_ids = self._extract_provider_ids_from_plex_guids(item_guids)
            show_key = (
                getattr(item, "grandparentRatingKey", None)
                or getattr(item, "grandparentKey", None)
                or getattr(item, "parentRatingKey", None)
                or getattr(item, "parentKey", None)
            )
            series_ids: Dict[str, Optional[str]] = {"tmdb": None, "imdb": None, "tvdb": None}
            if show_key:
                try:
                    show = plex.fetchItem(self._plex_item_reference(show_key))
                    series_ids = self._extract_provider_ids_from_plex_guids(
                        self._plex_guid_dicts(show)
                    )
                except Exception as err:
                    logger.warning(f"观看进度同步：读取 Plex 剧集所属剧集 GUID 失败 {err}")
            return {
                "tmdb": series_ids.get("tmdb"),
                "imdb": series_ids.get("imdb"),
                "tvdb": series_ids.get("tvdb"),
                "series_tmdb": series_ids.get("tmdb"),
                "series_imdb": series_ids.get("imdb"),
                "series_tvdb": series_ids.get("tvdb"),
                "episode_tmdb": episode_ids.get("tmdb"),
                "episode_imdb": episode_ids.get("imdb"),
                "episode_tvdb": episode_ids.get("tvdb"),
            }
        ids = self._extract_provider_ids_from_plex_guids(
            item_guids
        )
        return {
            "tmdb": ids.get("tmdb"),
            "imdb": ids.get("imdb"),
            "tvdb": ids.get("tvdb"),
            "series_tmdb": None,
            "series_imdb": None,
            "series_tvdb": None,
            "episode_tmdb": None,
            "episode_imdb": None,
            "episode_tvdb": None,
        }

    @staticmethod
    def _provider_state_fields(ids: Dict[str, Optional[str]], media_kind: str) -> Dict[str, Any]:
        """把 Plex provider id 映射为 NormalizedState 字段。"""
        fields = {
            "tmdb_id": WatchStateSync._coerce_int(ids.get("tmdb")),
            "imdb_id": ids.get("imdb"),
            "tvdb_id": ids.get("tvdb"),
        }
        if media_kind == "episode":
            fields.update({
                "series_tmdb_id": WatchStateSync._coerce_int(ids.get("series_tmdb")),
                "series_imdb_id": ids.get("series_imdb"),
                "series_tvdb_id": ids.get("series_tvdb"),
                "episode_tmdb_id": WatchStateSync._coerce_int(ids.get("episode_tmdb")),
                "episode_imdb_id": ids.get("episode_imdb"),
                "episode_tvdb_id": ids.get("episode_tvdb"),
            })
        return fields

    @staticmethod
    def _plex_guid_dicts(item: Any) -> List[dict]:
        result = []
        for guid in getattr(item, "guids", []) or []:
            value = guid.get("id") if isinstance(guid, dict) else getattr(guid, "id", None)
            if value:
                result.append({"id": value})
        return result

    def _plex_user_fields(
        self, plex: Any, account_id: Any = None, user_name: Any = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """从 history/item 的 accountID 尽量补齐 Plex 本地用户名称。"""
        resolved_id = self._coerce_str(account_id)
        resolved_name = self._coerce_str(user_name)
        if resolved_id and hasattr(plex, "systemAccount"):
            try:
                account = plex.systemAccount(int(resolved_id))
                resolved_name = resolved_name or self._coerce_str(
                    getattr(account, "title", None)
                    or getattr(account, "username", None)
                    or getattr(account, "name", None)
                )
            except Exception:
                # 共享用户 token 可能无法读取 /accounts，保留已有 accountID 即可。
                pass
        return resolved_id, resolved_name

    def _get_plex_token_identity(self, source_server: str) -> List[str]:
        """返回 Plex token 对应账户的可比对身份，失败时返回空列表。"""
        if source_server in self._plex_user_identity_cache:
            return self._plex_user_identity_cache[source_server]

        service = self._get_service(source_server)
        identities: List[str] = []
        plex = service.instance.get_plex() if service and service.type == "plex" else None
        if plex:
            for method_name in ("myPlexAccount", "account"):
                method = getattr(plex, method_name, None)
                if not method:
                    continue
                try:
                    account = method()
                except Exception:
                    continue
                for attr in ("id", "username", "title", "name", "email"):
                    value = self._coerce_str(getattr(account, attr, None))
                    if value and value.lower() not in {item.lower() for item in identities}:
                        identities.append(value)

            # history 的 accountID 通常是 PMS 本地账号 ID；把它对应的显示名也纳入比较。
            if identities and hasattr(plex, "systemAccounts"):
                try:
                    for account in plex.systemAccounts() or []:
                        account_name = self._coerce_str(
                            getattr(account, "title", None)
                            or getattr(account, "username", None)
                            or getattr(account, "name", None)
                        )
                        if account_name and any(
                            account_name.lower() == identity.lower() for identity in identities
                        ):
                            account_id = self._coerce_str(getattr(account, "id", None))
                            if account_id:
                                identities.append(account_id)
                except Exception:
                    pass

        self._plex_user_identity_cache[source_server] = identities
        if not identities:
            logger.warning(
                f"观看进度同步：无法解析 Plex token 用户 {source_server}，"
                "请配置允许同步的用户名或 accountId 以避免多用户串写"
            )
        return identities

    def _plex_default_user_name(self, source_server: str) -> Optional[str]:
        for identity in self._get_plex_token_identity(source_server):
            if not identity.isdigit() and "@" not in identity:
                return identity
        return None

    def _plex_token_matches_user(
        self, source_server: str, user_id: Optional[str], user_name: Optional[str]
    ) -> bool:
        identities = {value.casefold() for value in self._get_plex_token_identity(source_server)}
        if not identities:
            return False
        return any(
            value and value.casefold() in identities
            for value in (user_id, user_name)
        )

    def _get_plex_user_server(
        self,
        source_service: ServiceInfo,
        plex: Any,
        user_id: Optional[str],
        user_name: Optional[str],
    ) -> Optional[Any]:
        """取得指定 Plex 用户上下文；无法切换时宁可跳过，不能读 owner 状态。"""
        if not user_id and not user_name:
            return plex
        if self._plex_token_matches_user(source_service.name, user_id, user_name):
            return plex

        cache_key = (source_service.name, (user_name or user_id or "").casefold())
        with self._lock:
            if cache_key in self._plex_user_servers:
                return self._plex_user_servers[cache_key]

        switch_user = getattr(plex, "switchUser", None)
        if not callable(switch_user):
            logger.warning(
                f"观看进度同步：Plex token 无法切换到用户 {user_name or user_id}，"
                "跳过该用户状态，避免误读 token owner"
            )
            return None

        candidates: List[Any] = [value for value in (user_name, user_id) if value]
        account_method = getattr(plex, "myPlexAccount", None)
        if callable(account_method):
            try:
                account = account_method()
                for method_name in ("user", "getUser"):
                    method = getattr(account, method_name, None)
                    if callable(method):
                        for value in (user_name, user_id):
                            if not value:
                                continue
                            try:
                                candidate = method(value)
                            except Exception:
                                continue
                            if candidate:
                                candidates.append(candidate)
                users = getattr(account, "users", None)
                users = users() if callable(users) else users
                for candidate in users or []:
                    candidate_values = {
                        self._coerce_str(getattr(candidate, attr, None)).casefold()
                        for attr in ("id", "username", "title", "name", "email")
                        if self._coerce_str(getattr(candidate, attr, None))
                    }
                    if any(
                        value and value.casefold() in candidate_values
                        for value in (user_id, user_name)
                    ):
                        candidates.append(candidate)
            except Exception:
                pass

        seen = set()
        for candidate in candidates:
            marker = repr(candidate)
            if marker in seen:
                continue
            seen.add(marker)
            try:
                user_server = switch_user(candidate)
            except Exception:
                continue
            if user_server:
                with self._lock:
                    self._plex_user_servers[cache_key] = user_server
                return user_server

        logger.warning(
            f"观看进度同步：Plex 无法切换到用户 {user_name or user_id}，"
            "跳过该用户状态，避免误读 token owner"
        )
        return None

    def _sync_state_to_target(
        self, source_server: str, target_server: str, target_service: ServiceInfo, state: NormalizedState
    ) -> str:
        """串行处理一个源事件，避免 WebSocket/Webhook/轮询并发写目标。"""
        with self._sync_lock:
            return self._sync_state_to_target_locked(
                source_server, target_server, target_service, state
            )

    def _sync_state_to_target_locked(
        self, source_server: str, target_server: str, target_service: ServiceInfo, state: NormalizedState
    ) -> str:
        """返回 success / skipped / failed。failed 表示需要保留游标并进入 Outbox 重试。"""
        if not self._should_sync(state.progress_ms, state.duration_ms, state.watched, state.operation):
            self._remember_source_event(state)
            return "skipped"
        if not self._user_allowed(state):
            logger.debug("观看进度同步：用户不在允许列表，跳过")
            return "skipped"
        if self._is_duplicate_source_event(state):
            return "skipped"
        # 失败事件进入 Outbox 前也要记录为“已观察”，这样后来更新的源事件
        # 成功或进入 Outbox 后，旧事件都不会在重试时倒灌覆盖新状态。
        self._record_latest_source_state(state)

        try:
            target_item = self._find_target_item(target_service, state)
        except Exception as err:
            self._increment_diagnostic("matching", "failed")
            self._record_history(
                title=f"{source_server} -> {target_server} 目标查询失败",
                subtitle=f"{self._state_label(state)} | {err}"
            )
            self._enqueue_outbox(source_server, target_server, state, None)
            return "failed"
        if not target_item:
            self._increment_diagnostic("matching", "failed")
            self._record_history(
                title=f"{source_server} -> {target_server} 未匹配到目标条目",
                subtitle=self._state_label(state)
            )
            self._enqueue_outbox(source_server, target_server, state, None)
            return "failed"

        self._increment_diagnostic("matching", "success")
        write_key = self._make_write_key(target_server, target_item.item_id, state)
        if self._seen_recently(write_key):
            self._remember_source_event(state)
            return "skipped"

        try:
            should_write, reason = self._target_needs_update(target_service, target_item, state)
        except Exception as err:
            self._record_history(
                title=f"{source_server} -> {target_server} 目标状态读取失败",
                subtitle=f"{self._state_label(state)} | {err}"
            )
            self._enqueue_outbox(source_server, target_server, state, target_item)
            return "failed"
        if not should_write:
            self._record_history(
                title=f"{source_server} -> {target_server} 跳过",
                subtitle=f"{self._state_label(state)} | {reason}"
            )
            self._remember_source_event(state)
            return "skipped"

        try:
            ok, message = self._apply_state(target_service, target_item, state)
        except Exception as err:
            ok, message = False, f"写回异常: {err}"
        self._increment_diagnostic("jellyfin_write", "attempts")
        if ok:
            self._increment_diagnostic("jellyfin_write", "success")
            self._remember_write(write_key)
            self._remember_source_event(state)
            self._record_history(
                title=f"{source_server} -> {target_server} 成功",
                subtitle=f"{self._state_label(state)} | {message}"
            )
            return "success"

        self._increment_diagnostic("jellyfin_write", "failed")
        self._record_history(
            title=f"{source_server} -> {target_server} 失败",
            subtitle=f"{self._state_label(state)} | {message}"
        )
        self._enqueue_outbox(source_server, target_server, state, target_item)
        return "failed"

    def _build_plex_state(self, service: ServiceInfo, event_info: WebhookEventInfo) -> Optional[NormalizedState]:
        if event_info.event not in ["media.stop", "media.scrobble", "media.unscrobble"]:
            return None

        plex = service.instance.get_plex()
        if not plex or not event_info.item_id:
            return None

        payload = event_info.json_object if isinstance(event_info.json_object, dict) else {}
        metadata = payload.get("Metadata") if isinstance(payload.get("Metadata"), dict) else {}
        item_reference = self._plex_item_reference(event_info.item_id)
        try:
            item = plex.fetchItem(item_reference)
        except Exception as err:
            logger.error(f"观看进度同步：读取 Plex 条目失败 {err}")
            return None

        event_user_id, event_user_name = self._plex_user_fields(
            plex,
            payload.get("AccountID") or metadata.get("accountID"),
            event_info.user_name or metadata.get("userName") or metadata.get("username")
        )
        event_user_name = event_user_name or self._plex_default_user_name(service.name)
        raw_progress = next(
            (
                value for value in (
                    payload.get("viewOffset"),
                    payload.get("ViewOffset"),
                    metadata.get("viewOffset"),
                    metadata.get("ViewOffset"),
                )
                if value is not None
            ),
            None,
        )
        progress_override = self._coerce_int(raw_progress)
        state_item = item
        if progress_override is None and event_info.event == "media.stop":
            try:
                state_plex = self._get_plex_user_server(
                    service, plex, event_user_id, event_user_name
                )
                if state_plex is None:
                    return None
                if state_plex is not plex:
                    state_item = state_plex.fetchItem(item_reference)
            except Exception as err:
                logger.error(f"观看进度同步：读取 Plex 用户停止状态失败 {err}")
                return None

        progress_ms = (
            progress_override
            if progress_override is not None
            else self._safe_int(getattr(state_item, "viewOffset", 0), 0)
        )
        duration_ms = self._safe_int(getattr(item, "duration", 0), 0)
        percent = round((progress_ms / duration_ms) * 100, 2) if progress_ms and duration_ms else 0.0
        is_unscrobble = event_info.event == "media.unscrobble"
        watched = False if is_unscrobble else (
            event_info.event == "media.scrobble"
            or (
                bool(getattr(state_item, "isPlayed", False))
                if progress_override is None
                else False
            )
        )
        if not is_unscrobble and not watched and percent >= self._watched_percent:
            watched = True
        operation = (
            StateOperation.UNWATCHED if is_unscrobble
            else StateOperation.WATCHED if watched
            else StateOperation.PROGRESS
        )

        media_kind = "episode" if getattr(item, "type", None) == "episode" else "movie"
        ids = self._get_plex_provider_ids(plex, item, media_kind)
        series_title = getattr(item, "grandparentTitle", None) if media_kind == "episode" else None
        title = getattr(item, "title", None)
        year = getattr(item, "year", None)
        season = getattr(item, "parentIndex", None) or metadata.get("parentIndex")
        episode = getattr(item, "index", None) or metadata.get("index")

        if watched:
            progress_ms = 0
            percent = 100.0
        elif is_unscrobble:
            progress_ms = 0
            percent = 0.0

        if not self._should_sync(progress_ms, duration_ms, watched, operation):
            return None

        source_event_at, source_sequence = self._new_source_event_meta(
            self._payload_event_timestamp(payload)
        )
        return NormalizedState(
            source_server=service.name,
            source_type="plex",
            event_type=event_info.event,
            user_name=event_user_name,
            media_kind=media_kind,
            title=title,
            original_title=getattr(item, "originalTitle", None),
            series_title=series_title,
            year=self._coerce_int(year),
            **self._provider_state_fields(ids, media_kind),
            season=self._coerce_int(season),
            episode=self._coerce_int(episode),
            source_item_id=self._plex_source_item_id(item_reference),
            progress_ms=progress_ms,
            duration_ms=duration_ms,
            watched=watched,
            percent=percent,
            played_at=self._to_iso(getattr(state_item, "lastViewedAt", None)),
            user_id=event_user_id,
            operation=operation,
            source_event_at=source_event_at,
            source_sequence=source_sequence,
        )

    def _find_target_item(self, target_service: ServiceInfo, state: NormalizedState) -> Optional[MediaServerItem]:
        if state.media_kind == "movie":
            return self._find_target_movie(target_service, state)
        if state.media_kind == "episode":
            return self._find_target_episode(target_service, state)
        return None

    def _find_target_movie(self, target_service: ServiceInfo, state: NormalizedState) -> Optional[MediaServerItem]:
        if target_service.type != "jellyfin":
            return None
        return self._find_jellyfin_movie_fallback(target_service, state)

    def _find_target_episode(self, target_service: ServiceInfo, state: NormalizedState) -> Optional[MediaServerItem]:
        if target_service.type != "jellyfin":
            return None
        if not state.season or not state.episode:
            return None

        # 先用 Series 层 provider id 找剧，再按季号/集号找 Episode；
        # 不能把 Episode TMDB ID 传给 get_tv_episodes 的 Series 查询。
        show_id = self._find_jellyfin_series_id_fallback(target_service, state)
        if not show_id:
            return None
        return self._find_jellyfin_episode_item(target_service, show_id, state.season, state.episode)

    def _find_jellyfin_episode_item(
        self, target_service: ServiceInfo, show_id: str, season: int, episode: int
    ) -> Optional[MediaServerItem]:
        server = target_service.instance
        context = self._get_jellyfin_request_context(server)
        if not context:
            return None
        url = f"{server._host}Shows/{show_id}/Episodes"
        params = context["params"].copy()
        params["isMissing"] = "false"
        res = RequestUtils(headers=context["headers"]).get_res(url, params=params)
        if not res or res.status_code >= 300:
            code = res.status_code if res else "n/a"
            raise RuntimeError(f"Jellyfin episode query failed ({code})")
        items = (res.json() or {}).get("Items", [])
        for item in items:
            if (
                self._coerce_int(item.get("ParentIndexNumber")) == season
                and self._coerce_int(item.get("IndexNumber")) == episode
            ):
                return self._get_jellyfin_iteminfo(server, item.get("Id"), context)
        # 季号没对齐时不使用“全剧唯一同集号”的低置信度兜底，宁可不写，避免写错季/错集。
        return None

    def _find_jellyfin_movie_fallback(
        self, target_service: ServiceInfo, state: NormalizedState
    ) -> Optional[MediaServerItem]:
        server = target_service.instance
        candidates = self._search_jellyfin_items(
            server=server,
            include_item_types="Movie",
            terms=self._build_search_terms([state.title, state.original_title]),
            limit=20
        )
        best = self._pick_best_jellyfin_match(candidates, state, media_kind="movie")
        if best:
            return self._get_jellyfin_iteminfo(server, best.get("Id"))
        return None

    def _get_jellyfin_iteminfo(
        self,
        server: Any,
        item_id: Optional[str],
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[MediaServerItem]:
        """用插件自己的目标用户上下文读取 Jellyfin 条目。

        MoviePilot 的 ``server.get_iteminfo`` 固定使用 server.user，可能与插件
        登录用户不同；匹配、读取 UserData、写回和验证必须始终使用同一身份。
        """
        if not item_id:
            return None
        context = context or self._get_jellyfin_request_context(server)
        if not context:
            raise RuntimeError("missing jellyfin user context")
        url = f"{server._host}Users/{context['user_id']}/Items/{item_id}"
        res = RequestUtils(headers=context["headers"]).get_res(
            url, params=context["params"].copy()
        )
        if not res:
            raise RuntimeError("Jellyfin item query failed (n/a)")
        if res.status_code in [401, 403]:
            self._invalidate_jellyfin_auth(server)
            raise RuntimeError(f"Jellyfin item query failed ({res.status_code})")
        if res.status_code == 404:
            return None
        if res.status_code >= 300:
            raise RuntimeError(f"Jellyfin item query failed ({res.status_code})")
        try:
            payload = res.json() or {}
        except Exception as err:
            raise RuntimeError(f"Jellyfin item query returned invalid JSON: {err}") from err
        resolved_id = payload.get("Id") or item_id
        # 同步逻辑只依赖 item_id；SimpleNamespace 避免再次走 server.user。
        return SimpleNamespace(item_id=str(resolved_id))

    def _find_jellyfin_series_id_fallback(
        self, target_service: ServiceInfo, state: NormalizedState
    ) -> Optional[str]:
        server = target_service.instance
        candidates = self._search_jellyfin_items(
            server=server,
            include_item_types="Series",
            terms=self._build_search_terms([state.series_title or state.title]),
            limit=30
        )
        best = self._pick_best_jellyfin_match(candidates, state, media_kind="episode")
        if best:
            logger.info(
                f"观看进度同步：Jellyfin 剧集兜底匹配成功 "
                f"{state.series_title or state.title} -> {best.get('Name')}"
            )
            return best.get("Id")
        return None

    def _search_jellyfin_items(
        self, server: Any, include_item_types: str, terms: List[str], limit: int = 20
    ) -> List[dict]:
        all_items: List[dict] = []
        seen_ids = set()
        context = self._get_jellyfin_request_context(server)
        if not context:
            return all_items
        for term in terms:
            url = f"{server._host}Users/{context['user_id']}/Items"
            params = {
                "IncludeItemTypes": include_item_types,
                "Fields": "ProviderIds,OriginalTitle,ProductionYear,Path,UserDataPlayCount,UserDataLastPlayedDate,ParentId",
                "StartIndex": 0,
                "Recursive": "true",
                "searchTerm": term,
                "Limit": limit,
            }
            params.update(context["params"])
            res = RequestUtils(headers=context["headers"]).get_res(url, params=params)
            if not res or res.status_code >= 300:
                code = res.status_code if res else "n/a"
                raise RuntimeError(f"Jellyfin search failed ({code}) for {term}")
            payload = res.json() or {}
            for item in payload.get("Items", []):
                item_id = item.get("Id")
                if item_id and item_id not in seen_ids:
                    seen_ids.add(item_id)
                    all_items.append(item)
        return all_items

    def _pick_best_jellyfin_match(
        self, candidates: List[dict], state: NormalizedState, media_kind: str
    ) -> Optional[dict]:
        if not candidates:
            return None

        target_titles = self._build_search_terms(
            [state.series_title] if media_kind == "episode"
            else [state.title, state.original_title]
        )
        target_title_norms = {self._normalize_title(title) for title in target_titles if title}

        strong_candidates = []
        medium_candidates = []

        for item in candidates:
            provider_ids = item.get("ProviderIds") or {}
            if media_kind == "episode":
                target_tmdb = state.series_tmdb_id or state.tmdb_id
                target_tvdb = state.series_tvdb_id or state.tvdb_id
                target_imdb = state.series_imdb_id or state.imdb_id
            else:
                target_tmdb = state.tmdb_id
                target_tvdb = state.tvdb_id
                target_imdb = state.imdb_id
            item_tmdb = self._coerce_int(provider_ids.get("Tmdb"))
            item_tvdb = provider_ids.get("Tvdb")
            item_imdb = provider_ids.get("Imdb")
            type_ok = (
                (media_kind == "movie" and item.get("Type") == "Movie")
                or (media_kind == "episode" and item.get("Type") == "Series")
            )
            provider_strong = (
                type_ok and (
                    (target_tmdb and item_tmdb and target_tmdb == item_tmdb)
                    or (target_tvdb and item_tvdb and str(target_tvdb) == str(item_tvdb))
                    or (target_imdb and item_imdb and str(target_imdb) == str(item_imdb))
                )
            )

            name_norm = self._normalize_title(item.get("Name"))
            original_norm = self._normalize_title(item.get("OriginalTitle"))
            title_match = name_norm in target_title_norms or (original_norm and original_norm in target_title_norms)
            item_year = self._coerce_int(item.get("ProductionYear"))
            if provider_strong:
                strong_candidates.append(item)
                continue
            if title_match and state.year and item_year and state.year == item_year and type_ok:
                medium_candidates.append(item)

        # Strong：Provider ID 精确一致，可直接同步。
        if strong_candidates:
            return strong_candidates[0]
        # Medium：标题 + 年份 + 类型一致才自动同步；只有标题相似不再写。
        if medium_candidates:
            return medium_candidates[0]
        return None

    @staticmethod
    def _build_search_terms(values: List[Optional[str]]) -> List[str]:
        terms: List[str] = []
        seen = set()
        for value in values:
            if not value:
                continue
            variants = [value.strip()]
            compact = re.sub(r"\s+", "", value).strip()
            if compact and compact not in variants:
                variants.append(compact)
            for variant in variants:
                if variant and variant not in seen:
                    seen.add(variant)
                    terms.append(variant)
        return terms

    @staticmethod
    def _normalize_title(value: Optional[str]) -> str:
        if not value:
            return ""
        value = value.lower().strip()
        value = re.sub(r"[\s\-_:：!！?？,，。·'\"“”‘’\(\)\[\]【】]+", "", value)
        return value

    def _apply_state(self, target_service: ServiceInfo, target_item: MediaServerItem, state: NormalizedState) -> Tuple[bool, str]:
        if self._dry_run:
            return True, "dry-run"
        if target_service.type != "jellyfin":
            return False, "unsupported target"
        return self._apply_to_jellyfin(target_service, target_item, state)

    def _apply_to_jellyfin(
        self, target_service: ServiceInfo, target_item: MediaServerItem, state: NormalizedState
    ) -> Tuple[bool, str]:
        server = target_service.instance
        auth_context = self._get_jellyfin_request_context(server)
        if not auth_context:
            return False, "missing jellyfin user context"

        if (
            self._state_operation(state) == StateOperation.PROGRESS
            and not auth_context.get("is_user_token")
        ):
            return False, "jellyfin progress requires username/password user token"

        operation = self._state_operation(state)
        if operation == StateOperation.WATCHED:
            ok, message = self._jellyfin_mark_watched(server, target_item.item_id, auth_context)
            if not ok and self._is_jellyfin_auth_error(message):
                self._invalidate_jellyfin_auth(server)
                auth_context = self._get_jellyfin_auth_context(server)
                if auth_context:
                    ok, message = self._jellyfin_mark_watched(server, target_item.item_id, auth_context)
            if ok and self._jellyfin_verify_state_with_retry(target_service, target_item, state):
                return True, message
            if ok:
                retry_ok, retry_message = self._jellyfin_mark_watched(
                    server, target_item.item_id, auth_context
                )
                if retry_ok and self._jellyfin_verify_state_with_retry(target_service, target_item, state):
                    return True, f"{retry_message} (retry)"
                return False, f"{message} (read-back verification failed)"
            return ok, message

        if operation == StateOperation.UNWATCHED:
            ok, message = self._jellyfin_mark_unwatched(server, target_item.item_id, auth_context)
            if not ok and self._is_jellyfin_auth_error(message):
                self._invalidate_jellyfin_auth(server)
                auth_context = self._get_jellyfin_auth_context(server)
                if auth_context:
                    ok, message = self._jellyfin_mark_unwatched(server, target_item.item_id, auth_context)
            if ok and self._jellyfin_verify_state_with_retry(target_service, target_item, state):
                return True, message
            if ok:
                retry_ok, retry_message = self._jellyfin_mark_unwatched(
                    server, target_item.item_id, auth_context
                )
                if retry_ok and self._jellyfin_verify_state_with_retry(target_service, target_item, state):
                    return True, f"{retry_message} (retry)"
                return False, f"{message} (read-back verification failed)"
            return ok, message

        # 继续观看：先确保不是已看，再用 Stop 语义写回最终停止位置。
        ok, message = self._jellyfin_mark_unwatched(server, target_item.item_id, auth_context)
        if not ok and self._is_jellyfin_auth_error(message):
            self._invalidate_jellyfin_auth(server)
            auth_context = self._get_jellyfin_auth_context(server)
            if auth_context:
                ok, message = self._jellyfin_mark_unwatched(server, target_item.item_id, auth_context)
        if not ok:
            return False, message

        ok, message = self._jellyfin_write_progress(server, target_item.item_id, state.progress_ms, auth_context)
        if not ok and self._is_jellyfin_auth_error(message):
            self._invalidate_jellyfin_auth(server)
            auth_context = self._get_jellyfin_auth_context(server)
            if auth_context:
                ok, message = self._jellyfin_write_progress(server, target_item.item_id, state.progress_ms, auth_context)
        if ok and self._jellyfin_verify_state_with_retry(target_service, target_item, state):
            return True, message
        if ok:
            retry_ok, retry_message = self._jellyfin_write_progress(
                server, target_item.item_id, state.progress_ms, auth_context
            )
            if retry_ok and self._jellyfin_verify_state_with_retry(target_service, target_item, state):
                return True, f"{retry_message} (retry)"
            return False, f"{message} (read-back verification failed)"
        return ok, message

    @staticmethod
    def _is_jellyfin_auth_error(message: str) -> bool:
        return "401" in message or "403" in message

    def _jellyfin_mark_watched(
        self, server: Any, item_id: str, auth_context: Dict[str, Any]
    ) -> Tuple[bool, str]:
        url = f"{server._host}UserPlayedItems/{item_id}"
        res = RequestUtils(headers=auth_context["headers"]).post_res(url, params=auth_context["params"])
        if not res or res.status_code >= 300:
            code = res.status_code if res else "n/a"
            return False, f"write jellyfin watched failed ({code})"
        return True, f"jellyfin watched:{item_id}"

    def _jellyfin_mark_unwatched(
        self, server: Any, item_id: str, auth_context: Dict[str, Any]
    ) -> Tuple[bool, str]:
        url = f"{server._host}UserPlayedItems/{item_id}"
        res = RequestUtils(headers=auth_context["headers"]).delete_res(url, params=auth_context["params"])
        if not res or res.status_code >= 300:
            code = res.status_code if res else "n/a"
            return False, f"write jellyfin unplayed failed ({code})"
        return True, f"jellyfin unplayed:{item_id}"

    def _jellyfin_write_progress(
        self, server: Any, item_id: str, progress_ms: int, auth_context: Dict[str, Any]
    ) -> Tuple[bool, str]:
        # 使用 /PlayingItems/{id} 的 Stop 语义，携带 positionTicks 表示停止位置。
        params = auth_context["params"].copy()
        params["positionTicks"] = max(progress_ms, 0) * 10000
        url = f"{server._host}PlayingItems/{item_id}"
        res = RequestUtils(headers=auth_context["headers"]).delete_res(url, params=params)
        if not res or res.status_code >= 300:
            code = res.status_code if res else "n/a"
            return False, f"write jellyfin progress failed ({code})"
        return True, f"jellyfin progress:{item_id}"

    def _jellyfin_verify_state(
        self, target_service: ServiceInfo, target_item: MediaServerItem, state: NormalizedState
    ) -> bool:
        try:
            current = self._read_current_target_state(target_service, target_item)
            if not current:
                return False
            operation = self._state_operation(state)
            if operation == StateOperation.WATCHED:
                return bool(current.get("watched"))
            if operation == StateOperation.UNWATCHED:
                return not bool(current.get("watched"))
            # 继续观看状态必须同时满足“未标记已看”和进度接近目标。
            return (
                not bool(current.get("watched"))
                and abs(self._safe_int(current.get("progress_ms"), 0) - state.progress_ms) <= 10000
            )
        except Exception as err:
            logger.warning(f"观看进度同步：Jellyfin 写后校验失败 {err}")
            return False

    def _jellyfin_verify_state_with_retry(
        self, target_service: ServiceInfo, target_item: MediaServerItem, state: NormalizedState
    ) -> bool:
        """写回后最多读两次，避免 Jellyfin 的 UserData 写入尚未完成时误报失败。"""
        for attempt in range(2):
            if self._jellyfin_verify_state(target_service, target_item, state):
                return True
            if attempt == 0:
                time.sleep(0.2)
        return False

    @staticmethod
    def _state_operation(state: NormalizedState) -> str:
        if state.operation in {
            StateOperation.WATCHED,
            StateOperation.UNWATCHED,
        }:
            return state.operation
        if state.watched:
            return StateOperation.WATCHED
        return StateOperation.PROGRESS if state.progress_ms > 0 else StateOperation.UNWATCHED

    def _should_sync(
        self,
        progress_ms: int,
        duration_ms: int,
        watched: bool,
        operation: Optional[str] = None,
    ) -> bool:
        if operation == StateOperation.UNWATCHED:
            return self._sync_watched
        if watched or operation == StateOperation.WATCHED:
            return self._sync_watched
        if not self._sync_progress:
            return False
        if progress_ms < self._min_progress_seconds * 1000:
            return False
        if duration_ms and progress_ms >= duration_ms:
            return False
        return True

    def _target_needs_update(
        self,
        target_service: ServiceInfo,
        target_item: MediaServerItem,
        state: NormalizedState,
        reject_progress_regression: bool = False,
    ) -> Tuple[bool, str]:
        current = self._read_current_target_state(target_service, target_item)
        if not current:
            return True, "无法读取目标当前状态"

        current_watched = bool(current.get("watched"))
        current_progress_ms = self._safe_int(current.get("progress_ms"), 0)

        operation = self._state_operation(state)
        if operation == StateOperation.WATCHED:
            if current_watched and current_progress_ms == 0:
                return False, "目标已是已看"
            return True, "需要标记已看"

        if operation == StateOperation.UNWATCHED:
            if not current_watched:
                return False, "目标已是未看"
            return True, "需要取消已看"

        if current_watched:
            if reject_progress_regression:
                return False, "目标已有更新的已看状态，丢弃过期进度"
            return True, "目标当前为已看，需要改成继续观看"

        delta_ms = abs(current_progress_ms - state.progress_ms)
        if reject_progress_regression and current_progress_ms > state.progress_ms:
            return False, "目标已有更高进度，丢弃过期进度"
        if delta_ms < self._progress_delta_seconds * 1000:
            return False, f"目标进度差仅 {int(delta_ms / 1000)} 秒"
        return True, "进度变化达到阈值"

    def _read_current_target_state(
        self, target_service: ServiceInfo, target_item: MediaServerItem
    ) -> Optional[Dict[str, Any]]:
        if target_service.type != "jellyfin":
            return None
        server = target_service.instance
        context = self._get_jellyfin_request_context(server)
        if not context:
            return None
        headers = context["headers"]
        params = context["params"]
        user_id = context["user_id"]
        url = f"{server._host}Users/{user_id}/Items/{target_item.item_id}"
        res = RequestUtils(headers=headers).get_res(url, params=params)
        if not res:
            return None
        if res.status_code in [401, 403]:
            self._invalidate_jellyfin_auth(server)
            return None
        if res.status_code >= 300:
            return None
        try:
            user_data = (res.json() or {}).get("UserData") or {}
        except Exception as err:
            logger.warning(f"观看进度同步：Jellyfin UserData 返回无效 JSON {err}")
            return None
        return {
            "watched": bool(user_data.get("Played")),
            "progress_ms": int(self._safe_int(user_data.get("PlaybackPositionTicks"), 0) / 10000)
        }

    def _user_allowed(self, state: NormalizedState) -> bool:
        configured = {item.casefold() for item in self._allowed_users}
        if configured:
            return any(
                value and value.casefold() in configured
                for value in (state.user_name, state.user_id)
            )

        # 未配置显式列表时，轮询优先限制为 Plex token 所属账户；
        # 如果 Plex token 无法返回账户身份，则保留兼容行为，但在日志中提示风险。
        identities = self._get_plex_token_identity(state.source_server)
        if not identities or not (state.user_name or state.user_id):
            return True
        identity_set = {item.casefold() for item in identities}
        return any(
            value and value.casefold() in identity_set
            for value in (state.user_name, state.user_id)
        )

    def _start_plex_alert_listener(self):
        try:
            source_service = self._get_service(self._server_a)
            if not source_service or source_service.type != "plex":
                return
            plex = source_service.instance.get_plex()
            if not plex:
                return
            self._stop_plex_alert_listener()
            listener = plex.startAlertListener(self._on_plex_alert)
            with self._lock:
                self._plex_alert_listener = listener
            self._record_diagnostic("plex_websocket", status="started")
            logger.info("观看进度同步：已启动 Plex 本地 WebSocket AlertListener")
        except Exception as err:
            self._record_diagnostic("plex_websocket", status="failed", error=str(err))
            logger.error(f"观看进度同步：启动 Plex WebSocket 失败，继续依赖轮询 {err}")

    @staticmethod
    def _plex_alert_listener_is_alive(listener: Any) -> bool:
        """尽量检查不同 PlexAPI AlertListener 版本暴露的线程状态。"""
        if listener is None:
            return False
        for method_name in ("is_alive", "isAlive"):
            method = getattr(listener, method_name, None)
            if callable(method):
                try:
                    return bool(method())
                except Exception:
                    return False
        for attr_name in ("running", "is_running"):
            value = getattr(listener, attr_name, None)
            if value is not None:
                return bool(value() if callable(value) else value)
        # 某些 PlexAPI 版本只返回一个没有状态属性的控制对象，不能据此误判断线。
        return True

    def _ensure_plex_alert_listener(self, source_service: ServiceInfo):
        with self._lock:
            listener = self._plex_alert_listener
        if listener is not None and self._plex_alert_listener_is_alive(listener):
            return
        if listener is not None:
            logger.warning("观看进度同步：检测到 Plex WebSocket listener 已停止，正在重连")
            self._record_diagnostic("plex_websocket", status="reconnecting")
        self._start_plex_alert_listener()

    def _stop_plex_alert_listener(self):
        with self._lock:
            listener = self._plex_alert_listener
            self._plex_alert_listener = None
        if listener:
            try:
                listener.stop()
            except Exception:
                pass

    def _resolve_plex_session_user(
        self, plex: Any, session_key: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """通过 sessionKey 查询当前播放 session 的真实 Plex 用户。"""
        if not session_key:
            return None, None
        try:
            sessions = plex.sessions() or []
        except Exception as err:
            logger.warning(f"观看进度同步：读取 Plex session 用户失败 {err}")
            return None, None

        for session in sessions:
            current_key = (
                getattr(session, "sessionKey", None)
                or getattr(session, "session", None)
                or getattr(session, "key", None)
            )
            if current_key is None or str(current_key) != str(session_key):
                continue

            user = getattr(session, "user", None)
            account_id = None
            user_name = None
            if isinstance(user, dict):
                account_id = user.get("id") or user.get("accountID") or user.get("accountId")
                user_name = user.get("title") or user.get("username") or user.get("name")
            elif user is not None and not isinstance(user, (str, int, float)):
                account_id = (
                    getattr(user, "id", None)
                    or getattr(user, "accountID", None)
                    or getattr(user, "accountId", None)
                )
                user_name = (
                    getattr(user, "title", None)
                    or getattr(user, "username", None)
                    or getattr(user, "name", None)
                )
            elif user is not None:
                user_name = str(user)

            account_id = account_id or getattr(session, "accountID", None)
            account_id = account_id or getattr(session, "accountId", None)
            user_name = user_name or getattr(session, "username", None)
            user_name = user_name or getattr(session, "userName", None)
            return self._plex_user_fields(plex, account_id, user_name)

        return None, None

    def _on_plex_alert(self, data: dict):
        try:
            if not self._enabled:
                return
            if not data or data.get("type") != "playing":
                return
            for notif in data.get("PlaySessionStateNotification") or []:
                self._handle_plex_alert_notification(notif)
        except Exception as err:
            logger.error(f"观看进度同步：处理 Plex WebSocket 通知失败 {err}")

    def _handle_plex_alert_notification(self, notif: dict):
        rating_key = notif.get("ratingKey") or notif.get("RatingKey")
        if not rating_key:
            return
        source_service = self._get_service(self._server_a)
        target_service = self._get_service(self._server_b)
        if not source_service or not target_service:
            return
        if source_service.type != "plex" or target_service.type != "jellyfin":
            return

        state_name = (notif.get("state") or notif.get("State") or "").lower()
        session_key = str(notif.get("sessionKey") or notif.get("SessionKey") or rating_key)
        item_key = notif.get("key") or notif.get("Key") or self._plex_item_reference(rating_key)
        raw_view_offset = (
            notif.get("viewOffset")
            if "viewOffset" in notif
            else notif.get("ViewOffset")
        )
        notification_progress_ms = self._coerce_int(raw_view_offset)
        plex = source_service.instance.get_plex()
        notification_user_id = self._coerce_str(
            notif.get("accountID") or notif.get("AccountID")
        )
        session_user_id, session_user_name = self._resolve_plex_session_user(plex, session_key)
        with self._lock:
            previous_session = dict(self._plex_sessions.get(session_key) or {})
        user_id = session_user_id or notification_user_id or previous_session.get("user_id")
        user_name = session_user_name or previous_session.get("user_name")
        if not session_user_id and notification_user_id:
            user_id, user_name = self._plex_user_fields(plex, user_id, user_name)
        if not user_id and not user_name:
            # WebSocket 通知通常没有用户字段，绝不能把 token owner 猜成当前播放用户。
            # 等待下一次 polling 或带有 session 用户信息的通知。
            logger.warning(
                f"观看进度同步：Plex WebSocket session {session_key} 缺少真实用户，跳过"
            )
            self._record_diagnostic("plex_websocket", status="missing_user")
            return

        if state_name == "stopped":
            with self._lock:
                self._plex_sessions.pop(session_key, None)
            state = self._build_plex_websocket_stopped_state(
                source_service,
                item_key,
                user_id,
                user_name,
                False,
                notification_progress_ms,
            )
            if state:
                self._sync_state_to_target(source_service.name, target_service.name, target_service, state)
            return

        # playing / paused / buffering 等优先使用通知中的当前用户进度；只有
        # 通知缺少 viewOffset 时才尝试切换 Plex 用户上下文读取状态。
        state = self._build_plex_resume_state(
            source_service,
            item_key,
            user_id,
            user_name,
            False,
            notification_progress_ms,
        )
        if not state:
            return
        state.event_type = "websocket.playing"
        state.user_id = user_id or state.user_id
        state.user_name = user_name or state.user_name
        sec = int(state.progress_ms / 1000)
        with self._lock:
            last = self._plex_sessions.get(session_key, {}).get("last_sec")
        if last is not None and abs(sec - last) < self._progress_delta_seconds:
            with self._lock:
                self._plex_sessions[session_key] = {
                    "item_key": str(item_key),
                    "last_sec": sec,
                    "state": state_name,
                    "user_id": state.user_id,
                    "user_name": state.user_name,
                }
            return
        with self._lock:
            self._plex_sessions[session_key] = {
                "item_key": str(item_key),
                "last_sec": sec,
                "state": state_name,
                "user_id": state.user_id,
                "user_name": state.user_name,
            }
        self._sync_state_to_target(source_service.name, target_service.name, target_service, state)

    def _reconcile_plex_sessions(self, source_service: ServiceInfo, target_service: ServiceInfo):
        if not self._plex_sessions:
            return
        try:
            plex = source_service.instance.get_plex()
            active_keys = set()
            for session in plex.sessions():
                key = getattr(session, "sessionKey", None) or getattr(session, "session", None)
                if key is not None:
                    active_keys.add(str(key))
            with self._lock:
                tracked_sessions = list(self._plex_sessions.items())
            for session_key, info in tracked_sessions:
                if session_key in active_keys:
                    continue
                with self._lock:
                    self._plex_sessions.pop(session_key, None)
                item_key = info.get("item_key")
                if not item_key or not (info.get("user_id") or info.get("user_name")):
                    continue
                state = self._build_plex_websocket_stopped_state(
                    source_service,
                    item_key,
                    info.get("user_id"),
                    info.get("user_name"),
                    False,
                )
                if state:
                    state.event_type = "session.lost"
                    self._sync_state_to_target(source_service.name, target_service.name, target_service, state)
        except Exception as err:
            logger.debug(f"观看进度同步：Plex session 兜底检查失败 {err}")

    def _source_event_key(self, state: NormalizedState) -> str:
        bucket = int(state.progress_ms / 1000) if state.progress_ms else 0
        return "|".join([
            state.source_server or "",
            state.source_type or "",
            state.source_item_id or "",
            state.event_type or "",
            self._state_operation(state),
            str(int(state.watched)),
            str(bucket),
            state.user_id or ""
        ])

    def _source_event_identity(self, state: NormalizedState) -> str:
        """同一条媒体、同一用户的不同操作共享一个事件顺序。"""
        item_id = state.source_item_id or ""
        if not item_id:
            item_id = "|".join([
                state.media_kind or "",
                state.title or "",
                str(state.season or ""),
                str(state.episode or ""),
            ])
        # 优先使用可读用户名，让 history 的 accountID+名称与 WebSocket
        # session 的用户对象能够落到同一个身份；没有名称时再退回 ID。
        user = state.user_name or state.user_id or ""
        return "|".join([state.source_server or "", item_id, user])

    @staticmethod
    def _event_version(value: Any) -> Tuple[float, int]:
        if isinstance(value, NormalizedState):
            return (
                WatchStateSync._safe_float(value.source_event_at, 0.0),
                WatchStateSync._safe_int(value.source_sequence, 0),
            )
        return (
            WatchStateSync._safe_float(value.get("source_event_at"), 0.0),
            WatchStateSync._safe_int(value.get("source_sequence"), 0),
        )

    def _new_source_event_meta(self, event_at: Optional[float] = None) -> Tuple[float, int]:
        event_timestamp = self._safe_float(event_at, 0.0) or time.time()
        with self._lock:
            persisted = self._safe_int(self.get_data("source_sequence"), 0)
            self._source_sequence = max(self._source_sequence, persisted) + 1
            sequence = self._source_sequence
            self.save_data("source_sequence", sequence)
        return event_timestamp, sequence

    def _load_source_events(self) -> List[str]:
        with self._lock:
            return list(self.get_data("source_events") or [])

    def _remember_source_event(self, state: NormalizedState):
        key = self._source_event_key(state)
        with self._lock:
            events = list(self.get_data("source_events") or [])
            if key not in events:
                events.insert(0, key)
                self.save_data("source_events", events[:self._max_source_events])

    def _is_duplicate_source_event(self, state: NormalizedState) -> bool:
        with self._lock:
            return self._source_event_key(state) in set(self.get_data("source_events") or [])

    def _record_latest_source_state(self, state: NormalizedState):
        """记录最新已观察到的源事件，供旧 Outbox 在重试前丢弃。"""
        identity = self._source_event_identity(state)
        version = self._event_version(state)
        with self._lock:
            latest = dict(self.get_data("source_latest_events") or {})
            current = latest.get(identity) or {}
            if current and self._event_version(current) >= version:
                return
            latest[identity] = {
                "source_event_at": version[0],
                "source_sequence": version[1],
                "key": self._source_event_key(state),
                "operation": self._state_operation(state),
                "progress_ms": state.progress_ms,
            }
            self.save_data("source_latest_events", latest)

    def _outbox_is_stale(self, entry: Dict[str, Any], state: NormalizedState) -> bool:
        """判断 Outbox 是否早于同媒体用户后来观察到的源事件。"""
        identity = self._source_event_identity(state)
        with self._lock:
            latest = (self.get_data("source_latest_events") or {}).get(identity)
        if not latest:
            return False
        entry_version = self._outbox_entry_version(entry, state)
        return self._event_version(latest) > entry_version

    def _outbox_entry_version(
        self, entry: Dict[str, Any], state: Optional[NormalizedState] = None
    ) -> Tuple[float, int]:
        version = self._event_version(entry)
        if version == (0.0, 0) and state is not None:
            return self._event_version(state)
        if version == (0.0, 0):
            try:
                return self._event_version(self._state_from_dict(entry.get("state") or {}))
            except Exception:
                pass
        return version

    def _state_to_dict(self, state: NormalizedState) -> Dict[str, Any]:
        return {
            "source_server": state.source_server,
            "source_type": state.source_type,
            "event_type": state.event_type,
            "user_name": state.user_name,
            "user_id": state.user_id,
            "media_kind": state.media_kind,
            "title": state.title,
            "original_title": state.original_title,
            "series_title": state.series_title,
            "year": state.year,
            "tmdb_id": state.tmdb_id,
            "imdb_id": state.imdb_id,
            "tvdb_id": state.tvdb_id,
            "season": state.season,
            "episode": state.episode,
            "source_item_id": state.source_item_id,
            "progress_ms": state.progress_ms,
            "duration_ms": state.duration_ms,
            "watched": state.watched,
            "percent": state.percent,
            "played_at": state.played_at,
            "operation": self._state_operation(state),
            "series_tmdb_id": state.series_tmdb_id,
            "series_imdb_id": state.series_imdb_id,
            "series_tvdb_id": state.series_tvdb_id,
            "episode_tmdb_id": state.episode_tmdb_id,
            "episode_imdb_id": state.episode_imdb_id,
            "episode_tvdb_id": state.episode_tvdb_id,
            "source_event_at": state.source_event_at,
            "source_sequence": state.source_sequence,
        }

    @staticmethod
    def _state_from_dict(data: Dict[str, Any]) -> NormalizedState:
        return NormalizedState(
            source_server=data.get("source_server", ""),
            source_type=data.get("source_type", ""),
            event_type=data.get("event_type", ""),
            user_name=data.get("user_name"),
            media_kind=data.get("media_kind", "movie"),
            title=data.get("title"),
            original_title=data.get("original_title"),
            series_title=data.get("series_title"),
            year=data.get("year"),
            tmdb_id=WatchStateSync._coerce_int(data.get("tmdb_id")),
            imdb_id=data.get("imdb_id"),
            tvdb_id=data.get("tvdb_id"),
            season=WatchStateSync._coerce_int(data.get("season")),
            episode=WatchStateSync._coerce_int(data.get("episode")),
            source_item_id=data.get("source_item_id"),
            progress_ms=WatchStateSync._safe_int(data.get("progress_ms", 0)),
            duration_ms=WatchStateSync._safe_int(data.get("duration_ms", 0)),
            watched=bool(data.get("watched", False)),
            percent=float(data.get("percent", 0.0) or 0.0),
            played_at=data.get("played_at"),
            user_id=data.get("user_id"),
            operation=data.get("operation") or (
                StateOperation.WATCHED
                if data.get("watched")
                else StateOperation.PROGRESS
                if data.get("progress_ms", 0)
                else StateOperation.UNWATCHED
            ),
            series_tmdb_id=WatchStateSync._coerce_int(data.get("series_tmdb_id")),
            series_imdb_id=data.get("series_imdb_id"),
            series_tvdb_id=data.get("series_tvdb_id"),
            episode_tmdb_id=WatchStateSync._coerce_int(data.get("episode_tmdb_id")),
            episode_imdb_id=data.get("episode_imdb_id"),
            episode_tvdb_id=data.get("episode_tvdb_id"),
            source_event_at=WatchStateSync._safe_float(data.get("source_event_at", 0), 0.0),
            source_sequence=WatchStateSync._safe_int(data.get("source_sequence", 0)),
        )

    def _load_outbox(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.get_data("outbox") or [])

    def _save_outbox(self, outbox: List[Dict[str, Any]]):
        with self._lock:
            # 新事件追加在尾部，容量满时必须优先保留最新状态。
            self.save_data("outbox", outbox[-500:])

    def _enqueue_outbox(
        self, source_server: str, target_server: str, state: NormalizedState, target_item: Optional[MediaServerItem]
    ):
        key = self._source_event_key(state)
        new_entry = {
            "key": key,
            "source_server": source_server,
            "target_server": target_server,
            "state": self._state_to_dict(state),
            "source_event_at": state.source_event_at,
            "source_sequence": state.source_sequence,
            "target_item_id": target_item.item_id if target_item else None,
            "attempts": 0,
            "next_attempt": 0,
            "created_at": time.time()
        }
        identity = self._source_event_identity(state)
        with self._lock:
            outbox = list(self.get_data("outbox") or [])
            existing_same_key = None
            retained = []
            for item in outbox:
                if item.get("key") == key:
                    existing_same_key = dict(item)
                    continue
                try:
                    old_state = self._state_from_dict(item.get("state") or {})
                    old_identity = self._source_event_identity(old_state)
                    old_version = self._outbox_entry_version(item, old_state)
                except Exception:
                    old_identity = None
                    old_version = (0.0, 0)
                if old_identity == identity and old_version <= self._event_version(state):
                    # 同一媒体/用户只保留最新待处理状态，避免 Outbox 被同一条
                    # 内容的连续进度填满。
                    continue
                retained.append(item)

            if existing_same_key is not None:
                existing_same_key.update({
                    "source_event_at": state.source_event_at,
                    "source_sequence": state.source_sequence,
                    "state": self._state_to_dict(state),
                    "target_item_id": target_item.item_id if target_item else existing_same_key.get("target_item_id"),
                })
                retained.append(existing_same_key)
            else:
                retained.append(new_entry)
            self.save_data("outbox", retained[-500:])

    def _process_outbox(self, target_service: ServiceInfo):
        with self._sync_lock:
            self._process_outbox_locked(target_service)

    def _process_outbox_locked(self, target_service: ServiceInfo):
        outbox = self._load_outbox()
        if not outbox:
            return
        now = time.time()
        changed = False
        remaining = []
        for entry in outbox:
            if entry.get("next_attempt", 0) > now:
                remaining.append(entry)
                continue
            state = self._state_from_dict(entry.get("state") or {})
            if self._outbox_is_stale(entry, state):
                self._remember_source_event(state)
                self._increment_diagnostic("outbox", "stale_dropped")
                changed = True
                continue
            if not self._should_sync(state.progress_ms, state.duration_ms, state.watched, state.operation):
                self._remember_source_event(state)
                changed = True
                continue
            if not self._user_allowed(state):
                changed = True
                continue

            target_item = None
            if entry.get("target_item_id"):
                try:
                    target_item = self._get_jellyfin_iteminfo(
                        target_service.instance, entry["target_item_id"]
                    )
                except Exception:
                    target_item = None
            try:
                if not target_item:
                    target_item = self._find_target_item(target_service, state)
            except Exception as err:
                entry["attempts"] = entry.get("attempts", 0) + 1
                entry["next_attempt"] = now + self._outbox_backoff(entry["attempts"])
                changed = True
                remaining.append(entry)
                self._record_history(
                    title=f"{entry.get('source_server')} -> {entry.get('target_server')} 重试异常",
                    subtitle=f"{self._state_label(state)} | {err}"
                )
                continue

            if not target_item:
                entry["attempts"] = entry.get("attempts", 0) + 1
                entry["next_attempt"] = now + self._outbox_backoff(entry["attempts"])
                changed = True
                remaining.append(entry)
                self._record_history(
                    title=f"{entry.get('source_server')} -> {entry.get('target_server')} 重试未匹配",
                    subtitle=self._state_label(state)
                )
                continue

            try:
                should_write, reason = self._target_needs_update(
                    target_service, target_item, state, reject_progress_regression=True
                )
            except Exception as err:
                should_write, reason = True, f"目标状态读取异常: {err}"
            if not should_write:
                self._remember_write(self._make_write_key(target_service.name, target_item.item_id, state))
                self._remember_source_event(state)
                changed = True
                continue

            try:
                ok, message = self._apply_state(target_service, target_item, state)
            except Exception as err:
                ok, message = False, f"写回异常: {err}"
            if ok:
                self._remember_write(self._make_write_key(target_service.name, target_item.item_id, state))
                self._remember_source_event(state)
                self._record_history(
                    title=f"{entry.get('source_server')} -> {entry.get('target_server')} 重试成功",
                    subtitle=f"{self._state_label(state)} | {message}"
                )
                changed = True
                continue

            entry["attempts"] = entry.get("attempts", 0) + 1
            entry["next_attempt"] = now + self._outbox_backoff(entry["attempts"])
            changed = True
            remaining.append(entry)
            self._record_history(
                title=f"{entry.get('source_server')} -> {entry.get('target_server')} 重试失败",
                subtitle=f"{self._state_label(state)} | {message}"
            )

        if changed:
            self._save_outbox(remaining)

    def _outbox_backoff(self, attempts: int) -> int:
        return min(self._outbox_retry_base_seconds * (2 ** max(attempts - 1, 0)), self._outbox_retry_max_seconds)



    def _make_write_key(self, server_name: str, item_id: Optional[str], state: NormalizedState) -> str:
        bucket = int(state.progress_ms / 1000) if state.progress_ms else 0
        return f"{server_name}|{item_id}|{self._state_operation(state)}|{bucket}|{state.user_id or ''}"

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
                self._jellyfin_auth_cache = {}
                self._plex_user_servers = {}
                return
            self._cleanup_caches_locked()

    def _cleanup_caches_locked(self):
        now = time.time()
        self._recent_writes = {
            key: ts for key, ts in self._recent_writes.items()
            if (now - ts) < self._write_ttl_seconds
        }

    def _clear_history_data(self) -> Dict[str, Any]:
        with self._lock:
            history_count = len(self.get_data("history") or [])
            outbox_count = len(self.get_data("outbox") or [])
            self.save_data("history", [])
            self.save_data("outbox", [])
            self.save_data("source_events", [])
            self.save_data("source_latest_events", {})
            self.save_data("diagnostics", {})

            reset_keys = []
            for service_name in [self._server_a, self._server_b]:
                if not service_name:
                    continue
                history_key = f"plex_history_ts::{service_name}"
                snapshot_key = f"plex_resume_snapshot::{service_name}"
                processed_key = f"plex_history_processed::{service_name}"
                self.save_data(history_key, 0)
                self.save_data(snapshot_key, {})
                self.save_data(processed_key, [])
                reset_keys.extend([history_key, snapshot_key, processed_key])

            self._cleanup_caches(force=True)
            self._plex_sessions = {}
        logger.info("观看进度同步：已清除历史数据、Outbox 并重置轮询游标")
        return {
            "history_count": history_count,
            "outbox_count": outbox_count,
            "reset_keys": reset_keys
        }

    def _get_jellyfin_auth_context(self, server: Any) -> Optional[Dict[str, Any]]:
        if not self._jellyfin_username or not self._jellyfin_password:
            return None

        host = server._host.rstrip("/")
        cache_key = (host, self._jellyfin_username)
        with self._lock:
            cached = self._jellyfin_auth_cache.get(cache_key)
            if cached and (time.time() - self._safe_int(cached.get("ts"), 0)) < self._jellyfin_auth_ttl_seconds:
                return cached.get("context")

        auth_url = f"{host}/Users/AuthenticateByName"
        auth_headers = {
            "Content-Type": "application/json",
            "X-Emby-Authorization": (
                f'MediaBrowser Client="MoviePilotWatchStateSync", Device="MoviePilot", '
                f'DeviceId="watchstatesync", Version="{self.plugin_version}"'
            )
        }
        payload = {
            "Username": self._jellyfin_username,
            "Pw": self._jellyfin_password
        }
        res = RequestUtils(headers=auth_headers, content_type="application/json").post_res(
            auth_url,
            json=payload
        )
        if not res or res.status_code >= 300:
            logger.error(
                f"观看进度同步：Jellyfin 登录失败 {res.status_code if res else 'n/a'}"
            )
            return None

        try:
            data = res.json() or {}
        except Exception as err:
            logger.error(f"观看进度同步：Jellyfin 登录返回无效 JSON {err}")
            return None
        access_token = data.get("AccessToken")
        user_id = ((data.get("User") or {}).get("Id")) or server.user
        if not access_token or not user_id:
            logger.error("观看进度同步：Jellyfin 登录返回缺少 access token 或 user id")
            return None

        context = {
            "headers": {
                "X-Emby-Token": access_token
            },
            "params": {
                "userId": user_id
            },
            "user_id": user_id,
            "username": self._jellyfin_username,
            "is_user_token": True,
        }
        with self._lock:
            self._jellyfin_auth_cache[cache_key] = {
                "ts": time.time(),
                "context": context
            }
        return context

    def _invalidate_jellyfin_auth(self, server: Any):
        host = server._host.rstrip("/")
        cache_key = (host, self._jellyfin_username)
        with self._lock:
            self._jellyfin_auth_cache.pop(cache_key, None)

    def _get_jellyfin_request_context(self, server: Any) -> Optional[Dict[str, Any]]:
        """所有 Jellyfin 搜索、读取、写入共用同一个用户上下文。"""
        auth_context = self._get_jellyfin_auth_context(server)
        if auth_context:
            return auth_context

        # 一旦用户填写了登录配置但登录失败，禁止退回 server.user，避免读 A 写 B。
        if self._jellyfin_username or self._jellyfin_password:
            return None
        user_id = self._coerce_str(getattr(server, "user", None))
        api_key = self._coerce_str(getattr(server, "_apikey", None))
        if not user_id or not api_key:
            return None
        return {
            "headers": {},
            "params": {
                "userId": user_id,
                "api_key": api_key,
            },
            "user_id": user_id,
            "username": None,
            "is_user_token": False,
        }

    def _record_diagnostic(self, section: str, **values: Any):
        with self._lock:
            diagnostics = dict(self.get_data("diagnostics") or {})
            current = dict(diagnostics.get(section) or {})
            current.update(values)
            current["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            diagnostics[section] = current
            self.save_data("diagnostics", diagnostics)

    def _increment_diagnostic(self, section: str, key: str, amount: int = 1):
        with self._lock:
            diagnostics = dict(self.get_data("diagnostics") or {})
            current = dict(diagnostics.get(section) or {})
            current[key] = self._safe_int(current.get(key), 0) + amount
            current["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            diagnostics[section] = current
            self.save_data("diagnostics", diagnostics)

    def _record_history(self, title: str, subtitle: str):
        with self._lock:
            history = list(self.get_data("history") or [])
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
            provider = provider.lower()
            if provider in ret:
                ret[provider] = provider_id
        return ret

    @staticmethod
    def _to_iso(value: Any) -> Optional[str]:
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return None

    @staticmethod
    def _to_timestamp(value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if hasattr(value, "timestamp"):
            try:
                return float(value.timestamp())
            except Exception:
                return None
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            except Exception:
                return None
        return None

    def _payload_event_timestamp(self, payload: Dict[str, Any]) -> float:
        for key in ("eventTimestamp", "timestamp", "occurredAt", "event_time"):
            timestamp = self._to_timestamp(payload.get(key))
            if timestamp:
                # Webhook payloads occasionally use milliseconds since epoch.
                return timestamp / 1000 if timestamp > 10_000_000_000 else timestamp
        return time.time()

    @staticmethod
    def _coerce_int(value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _coerce_str(value: Any) -> Optional[str]:
        if value is None:
            return None
        return str(value).strip() or None

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _state_label(state: NormalizedState) -> str:
        operation = WatchStateSync._state_operation(state)
        state_text = {
            StateOperation.WATCHED: "已看",
            StateOperation.UNWATCHED: "未看",
        }.get(operation, f"{int(state.progress_ms / 1000)}s")
        if state.media_kind == "episode":
            season = state.season or 0
            episode = state.episode or 0
            return (
                f"{state.series_title or state.title} "
                f"S{season:02d}E{episode:02d} "
                f"{state_text}"
            )
        return f"{state.title} {state_text}"
