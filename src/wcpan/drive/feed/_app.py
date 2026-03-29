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
from ._scanner import Scanner, build_watch_root_paths
from ._types import Config
from ._workers import (
    checkpoint_worker,
    create_metadata_queue,
    create_write_queue,
    metadata_worker,
    resolve_meta_workers,
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
        num_workers = resolve_meta_workers(config.metadata_workers)
        metadata_queue = create_metadata_queue(num_workers)
        write_queue = create_write_queue(num_workers)
        ready_event = asyncio.Event()
        app[APP_KEY_READY] = ready_event
        app[APP_WATCH_ROOT_PATHS] = build_watch_root_paths(config)

        from ._watcher import (
            WatcherConsumer,
            WatcherHandlers,
            create_event_queue,
            make_watcher_backend,
        )

        watcher_fn = make_watcher_backend(config.watcher)
        event_queue = create_event_queue()
        watcher_handlers = WatcherHandlers(event_queue=event_queue)
        scanner = Scanner(
            storage=storage,
            off_main=off_main,
            config=config,
            write_queue=write_queue,
            metadata_queue=metadata_queue,
        )
        watcher_consumer = WatcherConsumer(
            event_queue=event_queue,
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
        for _ in range(num_workers):
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

        # Consumer drives the actual async processing of watcher events
        await stack.enter_async_context(_background(group, watcher_consumer.consume()))

        # Startup: scan → drain queues → set ready
        await stack.enter_async_context(
            _background(group, scanner.startup(ready_event))
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
