from dataclasses import dataclass
from datetime import datetime
from typing import Literal, TypedDict


@dataclass(frozen=True, kw_only=True)
class InotifyWatcherConfig:
    backend: Literal["inotify"] = "inotify"


@dataclass(frozen=True, kw_only=True)
class FanotifyWatcherConfig:
    backend: Literal["fanotify"] = "fanotify"


type WatcherConfig = InotifyWatcherConfig | FanotifyWatcherConfig


@dataclass(frozen=True, kw_only=True)
class Config:
    host: str
    port: int
    database_url: str
    watches: dict[str, str]  # namespace -> path
    exclude: tuple[str, ...] = ()
    log_path: str | None = None
    watcher: WatcherConfig


@dataclass(frozen=True, kw_only=True)
class NodeRecord:
    node_id: str
    parent_id: str | None
    name: str
    is_directory: bool
    ctime: datetime
    mtime: datetime
    mime_type: str
    hash: str
    size: int
    is_image: bool
    is_video: bool
    width: int
    height: int
    ms_duration: int


@dataclass(frozen=True, kw_only=True)
class RemovedChange:
    removed: Literal[True]
    node_id: str


@dataclass(frozen=True, kw_only=True)
class UpdatedChange:
    removed: Literal[False]
    node: NodeRecord


type MergedChange = RemovedChange | UpdatedChange


class NodeDict(TypedDict):
    id: str
    parent_id: str | None
    name: str
    is_directory: bool
    ctime: str
    mtime: str
    mime_type: str
    hash: str
    size: int
    is_image: bool
    is_video: bool
    width: int
    height: int
    ms_duration: int


class NodeParams(TypedDict):
    node_id: str
    parent_id: str | None
    name: str
    is_directory: int
    ctime: int
    mtime: int
    mime_type: str
    hash: str
    size: int
    is_image: int
    is_video: int
    width: int
    height: int
    ms_duration: int


@dataclass(frozen=True, kw_only=True)
class FileMetadata:
    mime_type: str
    hash: str  # MD5 hex
    size: int
    is_image: bool
    is_video: bool
    width: int
    height: int
    ms_duration: int
