import importlib.util
import sys
import types
import unittest
from pathlib import Path


def _install_moviepilot_stubs():
    app = types.ModuleType("app")
    app.__path__ = []
    core = types.ModuleType("app.core")
    event = types.ModuleType("app.core.event")
    helper = types.ModuleType("app.helper")
    mediaserver = types.ModuleType("app.helper.mediaserver")
    log = types.ModuleType("app.log")
    plugins = types.ModuleType("app.plugins")
    schemas = types.ModuleType("app.schemas")
    schemas_types = types.ModuleType("app.schemas.types")
    utils = types.ModuleType("app.utils")
    http = types.ModuleType("app.utils.http")

    class Event:
        pass

    class EventManager:
        @staticmethod
        def register(_event_type):
            return lambda function: function

    class MediaServerHelper:
        def get_service(self, name=None):
            return None

        def get_configs(self):
            return {}

    class Logger:
        def __getattr__(self, _name):
            return lambda *_args, **_kwargs: None

    class PluginBase:
        def __init__(self):
            self._test_data = {}

        def get_data(self, key):
            return self._test_data.get(key)

        def save_data(self, key, value):
            self._test_data[key] = value

        def post_message(self, **_kwargs):
            return None

    class EventType:
        WebhookMessage = "WebhookMessage"

    class RequestUtils:
        def __init__(self, *args, **kwargs):
            pass

    event.Event = Event
    event.eventmanager = EventManager()
    mediaserver.MediaServerHelper = MediaServerHelper
    log.logger = Logger()
    plugins._PluginBase = PluginBase
    schemas.MediaServerItem = object
    schemas.ServiceInfo = object
    schemas.WebhookEventInfo = object
    schemas_types.EventType = EventType
    http.RequestUtils = RequestUtils

    modules = {
        "app": app,
        "app.core": core,
        "app.core.event": event,
        "app.helper": helper,
        "app.helper.mediaserver": mediaserver,
        "app.log": log,
        "app.plugins": plugins,
        "app.schemas": schemas,
        "app.schemas.types": schemas_types,
        "app.utils": utils,
        "app.utils.http": http,
    }
    sys.modules.update(modules)


_install_moviepilot_stubs()
MODULE_PATH = Path(__file__).parents[1] / "plugins.v2" / "watchstatesync" / "__init__.py"
SPEC = importlib.util.spec_from_file_location("watchstatesync_under_test", MODULE_PATH)
WATCHSTATESYNC = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(WATCHSTATESYNC)


class Guid:
    def __init__(self, value):
        self.id = value


class PlexItem:
    def __init__(self, **values):
        self.type = values.get("type", "movie")
        self.title = values.get("title", "Movie")
        self.originalTitle = values.get("originalTitle")
        self.grandparentTitle = values.get("grandparentTitle")
        self.grandparentRatingKey = values.get("grandparentRatingKey")
        self.parentIndex = values.get("parentIndex")
        self.index = values.get("index")
        self.year = values.get("year", 2024)
        self.viewOffset = values.get("viewOffset", 0)
        self.duration = values.get("duration", 1000 * 1000)
        self.isPlayed = values.get("isPlayed", False)
        self.lastViewedAt = None
        self.guids = [Guid(value) for value in values.get("guids", [])]
        self.accountID = values.get("accountID")
        self.userName = values.get("userName")


class FakePlex:
    def __init__(self, items=None):
        self.items = items or {}

    def fetchItem(self, key):
        return self.items[key]

    def systemAccount(self, account_id):
        return types.SimpleNamespace(id=account_id, title="alice")


class FakeSourceInstance:
    def __init__(self, plex):
        self._plex = plex

    def get_plex(self):
        return self._plex


class FakeService:
    def __init__(self, name, service_type, instance):
        self.name = name
        self.type = service_type
        self.instance = instance


class FakeResponse:
    def __init__(self, payload=None, status_code=200):
        self.payload = payload or {}
        self.status_code = status_code

    def json(self):
        return self.payload


class RecordingRequestUtils:
    calls = []
    responses = {}

    def __init__(self, headers=None, **_kwargs):
        self.headers = headers or {}

    def get_res(self, url, params=None):
        self.calls.append(("GET", url, self.headers, params or {}))
        return self.responses.get(("GET", url, self.headers.get("X-Plex-Container-Start"))) or FakeResponse()

    def delete_res(self, url, params=None):
        self.calls.append(("DELETE", url, self.headers, params or {}))
        return FakeResponse(status_code=204)


class WatchStateSyncTests(unittest.TestCase):
    def setUp(self):
        self.plugin = WATCHSTATESYNC.WatchStateSync()
        self.plugin._allowed_users = []
        self.plugin._sync_watched = True
        self.plugin._sync_progress = True
        self.plugin._min_progress_seconds = 60
        self.plugin._watched_percent = 90

    def test_unscrobble_is_explicit_unwatched_operation(self):
        item = PlexItem(viewOffset=0, duration=1000 * 1000, isPlayed=False, guids=["tmdb://10"])
        plex = FakePlex({"/library/metadata/1": item})
        source = FakeService("plex", "plex", FakeSourceInstance(plex))
        event = types.SimpleNamespace(
            event="media.unscrobble",
            item_id="/library/metadata/1",
            user_name="alice",
            json_object={"AccountID": "7"},
        )

        state = self.plugin._build_plex_state(source, event)

        self.assertIsNotNone(state)
        self.assertEqual(state.operation, WATCHSTATESYNC.StateOperation.UNWATCHED)
        self.assertFalse(state.watched)
        self.assertTrue(
            self.plugin._should_sync(state.progress_ms, state.duration_ms, state.watched, state.operation)
        )

    def test_partial_history_is_progress_not_watched(self):
        item = PlexItem(viewOffset=35 * 60 * 1000, duration=120 * 60 * 1000, isPlayed=False)
        plex = FakePlex({"/library/metadata/2": item})
        source = FakeService("plex", "plex", FakeSourceInstance(plex))

        state = self.plugin._build_plex_history_state(
            source,
            {"key": "/library/metadata/2", "viewedAt": 100, "accountID": "7"},
        )

        self.assertIsNotNone(state)
        self.assertEqual(state.operation, WATCHSTATESYNC.StateOperation.PROGRESS)
        self.assertFalse(state.watched)
        self.assertEqual(state.progress_ms, 35 * 60 * 1000)

    def test_episode_keeps_series_and_episode_provider_ids_separate(self):
        episode = PlexItem(
            type="episode",
            title="Pilot",
            grandparentTitle="Example Show",
            grandparentRatingKey="show-1",
            parentIndex=1,
            index=1,
            guids=["tmdb://222", "tvdb://episode-222"],
        )
        show = PlexItem(
            type="show",
            title="Example Show",
            guids=["tmdb://111", "tvdb://show-111"],
        )
        plex = FakePlex({"episode-1": episode, "show-1": show})
        source = FakeService("plex", "plex", FakeSourceInstance(plex))

        state = self.plugin._build_plex_resume_state(source, "episode-1")

        self.assertIsNone(state)  # default fake offset is below the 60s threshold
        episode.viewOffset = 120 * 1000
        state = self.plugin._build_plex_resume_state(source, "episode-1")
        self.assertEqual(state.series_tmdb_id, 111)
        self.assertEqual(state.episode_tmdb_id, 222)
        self.assertEqual(state.series_tvdb_id, "show-111")
        self.assertEqual(state.episode_tvdb_id, "episode-222")

    def test_history_uses_plex_paging_headers(self):
        old_request_utils = WATCHSTATESYNC.RequestUtils
        try:
            RecordingRequestUtils.calls = []
            RecordingRequestUtils.responses = {
                ("GET", "http://plex/status/sessions/history/all", "0"): FakeResponse(
                    {"MediaContainer": {"Metadata": [{"viewedAt": 200}, {"viewedAt": 199}]}}
                ),
                ("GET", "http://plex/status/sessions/history/all", "2"): FakeResponse(
                    {"MediaContainer": {"Metadata": [{"viewedAt": 198}]}}
                ),
            }
            WATCHSTATESYNC.RequestUtils = RecordingRequestUtils
            server = types.SimpleNamespace(
                _host="http://plex/",
                _token="plex-token",
            )
            source = FakeService("plex", "plex", server)
            self.plugin._plex_history_page_size = 2

            result = self.plugin._get_plex_history(source)

            self.assertEqual(len(result), 3)
            first_call = RecordingRequestUtils.calls[0]
            self.assertEqual(first_call[2]["X-Plex-Container-Start"], "0")
            self.assertEqual(first_call[2]["X-Plex-Container-Size"], "2")
            self.assertEqual(first_call[2]["X-Plex-Token"], "plex-token")
        finally:
            WATCHSTATESYNC.RequestUtils = old_request_utils

    def test_episode_matching_reads_with_authenticated_user_context(self):
        old_request_utils = WATCHSTATESYNC.RequestUtils
        try:
            RecordingRequestUtils.calls = []
            RecordingRequestUtils.responses = {}
            WATCHSTATESYNC.RequestUtils = RecordingRequestUtils
            self.plugin._jellyfin_username = "target-user"
            self.plugin._jellyfin_password = "password"
            auth_context = {
                "headers": {"X-Emby-Token": "token"},
                "params": {"userId": "auth-user"},
                "user_id": "auth-user",
                "is_user_token": True,
            }
            self.plugin._get_jellyfin_auth_context = lambda _server: auth_context
            target_server = types.SimpleNamespace(
                _host="http://jellyfin/",
                user="server-user",
                _apikey="api-key",
                get_iteminfo=lambda item_id: types.SimpleNamespace(item_id=item_id),
            )
            target = FakeService("jellyfin", "jellyfin", target_server)
            state = WATCHSTATESYNC.NormalizedState(
                source_server="plex",
                source_type="plex",
                event_type="poll.resume",
                user_name="alice",
                media_kind="episode",
                title="Pilot",
                original_title=None,
                series_title="Example Show",
                year=2024,
                tmdb_id=111,
                imdb_id=None,
                tvdb_id="show-111",
                season=1,
                episode=2,
                source_item_id="episode-1",
                progress_ms=120000,
                duration_ms=1000000,
                watched=False,
                percent=12,
                played_at=None,
                series_tmdb_id=111,
                series_tvdb_id="show-111",
                operation=WATCHSTATESYNC.StateOperation.PROGRESS,
            )
            RecordingRequestUtils.responses["GET", "http://jellyfin/Users/auth-user/Items", None] = FakeResponse(
                {"Items": [{"Id": "show-id", "Name": "Example Show", "Type": "Series", "ProductionYear": 2024,
                            "ProviderIds": {"Tmdb": "111", "Tvdb": "show-111"}}]}
            )
            RecordingRequestUtils.responses["GET", "http://jellyfin/Shows/show-id/Episodes", None] = FakeResponse(
                {"Items": [{"Id": "episode-id", "ParentIndexNumber": "1", "IndexNumber": "2"}]}
            )

            item = self.plugin._find_target_episode(target, state)

            self.assertEqual(item.item_id, "episode-id")
            self.assertTrue(any("Users/auth-user/Items" in call[1] for call in RecordingRequestUtils.calls))
            episode_call = next(call for call in RecordingRequestUtils.calls if "/Episodes" in call[1])
            self.assertEqual(episode_call[3]["userId"], "auth-user")
            self.assertNotIn("api_key", episode_call[3])
        finally:
            WATCHSTATESYNC.RequestUtils = old_request_utils

    def test_progress_write_uses_user_context_and_ticks(self):
        old_request_utils = WATCHSTATESYNC.RequestUtils
        try:
            RecordingRequestUtils.calls = []
            WATCHSTATESYNC.RequestUtils = RecordingRequestUtils
            server = types.SimpleNamespace(_host="http://jellyfin/")
            context = {
                "headers": {"X-Emby-Token": "token"},
                "params": {"userId": "auth-user"},
            }

            ok, _message = self.plugin._jellyfin_write_progress(server, "item-id", 1234, context)

            self.assertTrue(ok)
            method, url, headers, params = RecordingRequestUtils.calls[0]
            self.assertEqual(method, "DELETE")
            self.assertEqual(url, "http://jellyfin/PlayingItems/item-id")
            self.assertEqual(headers["X-Emby-Token"], "token")
            self.assertEqual(params["userId"], "auth-user")
            self.assertEqual(params["positionTicks"], 1234 * 10000)
        finally:
            WATCHSTATESYNC.RequestUtils = old_request_utils

if __name__ == "__main__":
    unittest.main()
