from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass(frozen=True, kw_only=True)
class Config:
    host: str
    port: int
    database_url: str
    watches: dict[str, str]  # namespace -> path
    exclude: tuple[str, ...] = ()


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
