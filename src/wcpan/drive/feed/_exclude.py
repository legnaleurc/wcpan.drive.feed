from fnmatch import fnmatch
from pathlib import Path


_DEFAULT_EXCLUDES: tuple[str, ...] = (
    # macOS
    ".DS_Store",
    ".Spotlight-V100",
    ".Trashes",
    ".fseventsd",
    # Windows
    "Thumbs.db",
    "ehthumbs.db",
    "desktop.ini",
    "$RECYCLE.BIN",
    # Synology
    "@eaDir",
    "#recycle",
    # QNAP
    ".@__thumb",
    # Download staging files (renamed to final name on completion)
    "*.__tmp__",
)


def is_excluded(name: str, extra: tuple[str, ...] = ()) -> bool:
    return any(fnmatch(name, p) for p in _DEFAULT_EXCLUDES + extra)


def is_path_excluded(path: Path, extra: tuple[str, ...] = ()) -> bool:
    return any(is_excluded(part, extra) for part in path.parts)
