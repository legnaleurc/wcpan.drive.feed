import asyncio
from pathlib import Path

from aiohttp import web

from ._db import OffMainThread
from ._types import Config


APP_CONFIG: web.AppKey[Config] = web.AppKey("config", Config)
APP_OFF_MAIN: web.AppKey[OffMainThread] = web.AppKey("off_main", OffMainThread)
APP_WATCH_ROOT_PATHS: web.AppKey[dict[str, Path]] = web.AppKey("watch_root_paths", dict)
APP_KEY_READY: web.AppKey[asyncio.Event] = web.AppKey("ready_event", asyncio.Event)
