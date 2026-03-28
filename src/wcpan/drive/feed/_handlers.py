from pathlib import Path

from aiohttp import web

from ._db import SUPER_ROOT_ID
from ._keys import APP_OFF_MAIN, APP_STORAGE, APP_WATCH_ROOT_PATHS
from ._lib import dispatch_change
from ._types import NodeDict, NodeRecord


def _node_to_dict(node: NodeRecord) -> NodeDict:
    return {
        "id": node.node_id,
        "parent_id": node.parent_id,
        "name": node.name,
        "is_directory": node.is_directory,
        "ctime": node.ctime.isoformat(),
        "mtime": node.mtime.isoformat(),
        "mime_type": node.mime_type,
        "hash": node.hash,
        "size": node.size,
        "is_image": node.is_image,
        "is_video": node.is_video,
        "width": node.width,
        "height": node.height,
        "ms_duration": node.ms_duration,
    }


async def handle_cursor(request: web.Request) -> web.Response:
    storage = request.app[APP_STORAGE]
    off_main = request.app[APP_OFF_MAIN]
    cursor = await off_main(storage.get_cursor)
    return web.json_response({"cursor": cursor})


_MAX_LIMIT = 1000


async def handle_changes(request: web.Request) -> web.Response:
    storage = request.app[APP_STORAGE]
    off_main = request.app[APP_OFF_MAIN]

    cursor_str = request.rel_url.query.get("cursor", "0")
    try:
        cursor = int(cursor_str)
    except ValueError:
        raise web.HTTPBadRequest(reason="invalid cursor")

    try:
        limit = int(request.rel_url.query.get("max_size", str(_MAX_LIMIT)))
    except ValueError:
        limit = _MAX_LIMIT
    if limit <= 0 or limit > _MAX_LIMIT:
        limit = _MAX_LIMIT

    changes, new_cursor, has_more = await off_main(
        storage.get_changes_since, cursor, limit
    )

    result: list[dict[str, object]] = []
    for change in changes:
        dispatch_change(
            change,
            on_removed=lambda node_id: result.append(
                {"removed": True, "node_id": node_id}
            ),
            on_updated=lambda node: result.append(
                {"removed": False, "node": _node_to_dict(node)}
            ),
        )

    return web.json_response(
        {"cursor": new_cursor, "changes": result, "has_more": has_more}
    )


async def handle_node_path(request: web.Request) -> web.Response:
    node_id = request.match_info["id"]
    storage = request.app[APP_STORAGE]
    off_main = request.app[APP_OFF_MAIN]
    watch_root_paths: dict[str, Path] = request.app[APP_WATCH_ROOT_PATHS]

    chain = await off_main(storage.get_ancestor_chain, node_id)
    if chain is None:
        raise web.HTTPNotFound()
    if not chain:  # super-root itself
        raise web.HTTPBadRequest(reason="no path for super-root")

    watch_root_node = chain[-1]
    real_root = watch_root_paths.get(watch_root_node.node_id)
    if real_root is None:
        raise web.HTTPInternalServerError(reason="watch root not found")

    # chain is [target, ..., watch_root]; build path from watch_root downward
    parts = [n.name for n in reversed(chain[:-1])]
    full_path = real_root.joinpath(*parts) if parts else real_root
    return web.json_response({"path": str(full_path)})


async def handle_root(request: web.Request) -> web.Response:
    storage = request.app[APP_STORAGE]
    off_main = request.app[APP_OFF_MAIN]
    node = await off_main(storage.get_node_by_id, SUPER_ROOT_ID)
    if node is None:
        raise web.HTTPInternalServerError(reason="super-root not found")
    return web.json_response(_node_to_dict(node))
