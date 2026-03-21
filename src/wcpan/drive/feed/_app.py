import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from logging import getLogger

from aiohttp import web

from ._db import Storage
from ._handlers import handle_changes, handle_cursor, handle_node_path, handle_root
from ._keys import (
    APP_CONFIG,
    APP_KEY_READY,
    APP_OFF_MAIN,
    APP_STORAGE,
    APP_WATCH_ROOT_PATHS,
)
from ._lib import OffMainThread
from ._scanner import (
    build_watch_root_paths,
    reconcile_stale_roots,
    scan_all_watch_paths,
)
from ._types import Config, MetadataQueue, WriteQueue
from ._workers import (
    NUM_META_WORKERS,
    checkpoint_worker,
    create_metadata_queue,
    create_write_queue,
    metadata_worker,
    write_worker,
)


type _Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


_L = getLogger(__name__)


@contextmanager
def _managed_pool() -> Generator[ThreadPoolExecutor, None, None]:
    pool = ThreadPoolExecutor()
    try:
        yield pool
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


@asynccontextmanager
async def _background[T](
    group: asyncio.TaskGroup, c: Coroutine[None, None, T]
) -> AsyncGenerator[None, None]:
    task = group.create_task(c)
    try:
        yield
    finally:
        task.cancel()


async def _startup(
    storage: Storage,
    off_main: OffMainThread,
    config: Config,
    write_queue: WriteQueue,
    metadata_queue: MetadataQueue,
    ready_event: asyncio.Event,
) -> None:
    await reconcile_stale_roots(storage, off_main, config, write_queue)
    await scan_all_watch_paths(storage, off_main, config, write_queue, metadata_queue)
    _L.info("startup scan complete; waiting for queues to drain")
    await metadata_queue.join()
    await write_queue.join()
    _L.info("queues idle; server is ready")
    ready_event.set()


@web.middleware
async def _ready_middleware(
    request: web.Request, handler: _Handler
) -> web.StreamResponse:
    if not request.app[APP_KEY_READY].is_set():
        raise web.HTTPServiceUnavailable(reason="Server starting")
    return await handler(request)


async def _app_lifecycle(app: web.Application) -> AsyncGenerator[None, None]:
    config: Config = app[APP_CONFIG]
    dsn = config.database_url

    async with AsyncExitStack() as stack:
        # 1. Single thread pool for both DB ops and metadata computation
        pool = stack.enter_context(_managed_pool())
        off_main = OffMainThread(pool)
        storage = Storage(dsn)
        app[APP_OFF_MAIN] = off_main
        app[APP_STORAGE] = storage

        # 2. Initialize DB
        _L.info("initializing database: %s", dsn)
        await off_main(storage.ensure_schema)
        await off_main(storage.upsert_super_root)

        # 3. Queues and ready event
        metadata_queue = create_metadata_queue()
        write_queue = create_write_queue()
        ready_event = asyncio.Event()
        app[APP_KEY_READY] = ready_event
        app[APP_WATCH_ROOT_PATHS] = build_watch_root_paths(config)

        from ._watcher import WatcherHandlers, make_watcher_backend

        watcher_fn = make_watcher_backend(config.watcher)
        watcher_handlers = WatcherHandlers(
            storage=storage,
            off_main=off_main,
            metadata_queue=metadata_queue,
            write_queue=write_queue,
            exclude=config.exclude,
        )

        # 4. Start background tasks under a single TaskGroup
        group = await stack.enter_async_context(asyncio.TaskGroup())

        # Single write consumer — serialises all DB writes
        await stack.enter_async_context(
            _background(group, write_worker(write_queue, off_main))
        )

        # Periodic WAL checkpoint — keeps WAL bounded during normal operation
        await stack.enter_async_context(
            _background(group, checkpoint_worker(write_queue, storage))
        )

        # N metadata workers — each computes file metadata and enqueues its own write
        for _ in range(NUM_META_WORKERS):
            await stack.enter_async_context(
                _background(
                    group,
                    metadata_worker(metadata_queue, write_queue, storage, off_main),
                )
            )

        # Watcher starts BEFORE scan to capture events that arrive during scanning
        await stack.enter_async_context(
            _background(
                group,
                watcher_fn(
                    list(config.watches.values()),
                    handlers=watcher_handlers,
                ),
            )
        )

        # Startup: scan → drain queues → set ready
        await stack.enter_async_context(
            _background(
                group,
                _startup(
                    storage, off_main, config, write_queue, metadata_queue, ready_event
                ),
            )
        )

        yield


def create_app(config: Config) -> web.Application:
    app = web.Application(middlewares=[_ready_middleware])
    app[APP_CONFIG] = config
    app.cleanup_ctx.append(_app_lifecycle)
    app.router.add_get("/api/v1/cursor", handle_cursor)
    app.router.add_get("/api/v1/changes", handle_changes)
    app.router.add_get("/api/v1/root", handle_root)
    app.router.add_get("/api/v1/nodes/{id}/path", handle_node_path)
    return app
