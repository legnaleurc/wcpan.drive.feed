import argparse
import logging
import sys
from logging.config import dictConfig
from pathlib import Path

import yaml
from aiohttp import web
from wcpan.logging import ConfigBuilder

from ._app import create_app
from ._types import Config, FanotifyWatcherConfig, InotifyWatcherConfig


_L = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="wcpan.drive.feed")
    parser.add_argument(
        "--config",
        default="/data/server.yaml",
        help="Path to YAML config file (default: /data/server.yaml)",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    raw_watcher = raw.get("watcher")
    if not raw_watcher:
        print("Config error: 'watcher' is required", file=sys.stderr)
        sys.exit(1)
    match raw_watcher.get("backend"):
        case "fanotify":
            watcher = FanotifyWatcherConfig()
        case "inotify":
            watcher = InotifyWatcherConfig()
        case _:
            print(
                "Config error: 'watcher.backend' must be 'inotify' or 'fanotify'",
                file=sys.stderr,
            )
            sys.exit(1)

    config = Config(
        host=raw.get("host", "0.0.0.0"),
        port=int(raw.get("port", 8080)),
        database_url=raw["database_url"],
        watches=dict(raw.get("watches", {})),
        exclude=tuple(raw.get("exclude", [])),
        log_path=raw.get("log_path"),
        watcher=watcher,
    )

    dictConfig(
        ConfigBuilder(path=config.log_path)
        .add("wcpan.drive.feed", level="D")
        .add("aiohttp")
        .to_dict()
    )

    app = create_app(config)
    _L.info("listening on %s:%s", config.host, config.port)
    web.run_app(app, host=config.host, port=config.port, print=None)
