from pathlib import Path

from aiohttp import web

from ._db import OffMainProcess
from ._types import Config


APP_CONFIG: web.AppKey[Config] = web.AppKey("config", Config)
APP_OFF_MAIN: web.AppKey[OffMainProcess] = web.AppKey("off_main", OffMainProcess)
APP_WATCH_ROOT_PATHS: web.AppKey[dict[str, Path]] = web.AppKey("watch_root_paths", dict)
