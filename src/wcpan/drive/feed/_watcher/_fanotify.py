import asyncio
import os
import struct
from collections.abc import Generator
from contextlib import contextmanager
from logging import getLogger
from pathlib import Path

import cffi

from ._lib import WatcherHandlers


_L = getLogger(__name__)

ffi = cffi.FFI()
ffi.cdef("""
    int fanotify_init(unsigned int flags, unsigned int event_f_flags);
    int fanotify_mark(int fanotify_fd, unsigned int flags,
                      uint64_t mask, int dirfd, const char *pathname);
    int open_by_handle_at(int mount_fd, void *handle, int flags);
""")
lib = ffi.dlopen(None)

# fanotify_init flags
FAN_CLASS_NOTIF = 0x00000000
FAN_REPORT_DFID_NAME = 0x00000C00

# fanotify_mark flags
FAN_MARK_ADD = 0x00000001
FAN_MARK_FILESYSTEM = 0x00000100

# fanotify event mask
FAN_CREATE = 0x00000100
FAN_DELETE = 0x00000200
FAN_MOVED_FROM = 0x00000400
FAN_MOVED_TO = 0x00000800
FAN_CLOSE_WRITE = 0x00000008
FAN_ONDIR = 0x40000000

_FAN_MASK = (
    FAN_CREATE
    | FAN_DELETE
    | FAN_MOVED_FROM
    | FAN_MOVED_TO
    | FAN_CLOSE_WRITE
    | FAN_ONDIR
)

# fanotify_event_info_type
FAN_EVENT_INFO_TYPE_DFID_NAME = 0x02

# open flags
AT_FDCWD = -100
O_RDONLY = 0
O_PATH = 0o10000000
O_CLOEXEC = 0o2000000
O_LARGEFILE = 0o0100000

# fanotify_event_metadata struct: versioned 24-byte header
_META_FMT = "=IBBHI Q I"
_META_SIZE = struct.calcsize(_META_FMT)  # 24

# fanotify_event_info_header: type(1) + pad(1) + len(2)
_INFO_HDR_FMT = "=BBH"
_INFO_HDR_SIZE = struct.calcsize(_INFO_HDR_FMT)

# file_handle: handle_bytes(4) + handle_type(4)
_FH_PREFIX_FMT = "=II"
_FH_PREFIX_SIZE = struct.calcsize(_FH_PREFIX_FMT)

_READ_SIZE = 65536


@contextmanager
def _fanotify_fd(flags: int, event_f_flags: int) -> Generator[int, None, None]:
    fd = lib.fanotify_init(flags, event_f_flags)
    if fd < 0:
        err = ffi.errno
        if err == 38:  # ENOSYS: kernel compiled without CONFIG_FANOTIFY
            raise OSError(
                err,
                "fanotify is not supported by this kernel "
                "(CONFIG_FANOTIFY may be disabled); "
                "switch to inotify in your watcher configuration",
            )
        raise OSError(err, "fanotify_init failed")
    try:
        yield fd
    finally:
        os.close(fd)


def _parse_events(data: bytes) -> list[tuple[int, int, Path | None]]:
    """Parse raw fanotify read buffer.

    Returns list of (mask, cookie, path_or_None).
    path is reconstructed from FAN_EVENT_INFO_TYPE_DFID_NAME info record.
    """
    results = []
    offset = 0
    while offset + _META_SIZE <= len(data):
        (
            event_len,
            vers,
            _reserved,
            metadata_len,
            fd_raw,
            mask,
            pid,
        ) = struct.unpack_from(_META_FMT, data, offset)

        if event_len == 0:
            break

        info_offset = offset + metadata_len
        info_end = offset + event_len
        path = None

        while info_offset + _INFO_HDR_SIZE <= info_end:
            info_type, _pad, info_len = struct.unpack_from(
                _INFO_HDR_FMT, data, info_offset
            )
            if info_len == 0:
                break

            if info_type == FAN_EVENT_INFO_TYPE_DFID_NAME:
                # After the header: fsid (8 bytes) then file_handle then name
                fh_offset = info_offset + _INFO_HDR_SIZE + 8  # skip fsid
                path = _extract_path(data, fh_offset, info_offset + info_len)

            info_offset += info_len

        # cookie is in the upper 32 bits of mask for move events (kernel >= 5.16)
        # For older kernels it's always 0 in fanotify; we use mask+pid as key
        cookie = 0

        results.append((mask, cookie, path))
        offset += event_len

    return results


def _extract_path(data: bytes, fh_offset: int, end: int) -> Path | None:
    """Extract path from file_handle + name bytes."""
    if fh_offset + _FH_PREFIX_SIZE > end:
        return None

    handle_bytes, handle_type = struct.unpack_from(_FH_PREFIX_FMT, data, fh_offset)
    fh_data_offset = fh_offset + _FH_PREFIX_SIZE
    fh_data_end = fh_data_offset + handle_bytes

    if fh_data_end > end:
        return None

    # Build a file_handle struct for open_by_handle_at
    # struct file_handle { __u32 handle_bytes; int handle_type; unsigned char f_handle[]; }
    fh_buf = ffi.new("char[]", _FH_PREFIX_SIZE + handle_bytes)
    ffi.cast("unsigned int *", fh_buf)[0] = handle_bytes
    ffi.cast("int *", ffi.cast("char *", fh_buf) + 4)[0] = handle_type
    ffi.buffer(fh_buf, _FH_PREFIX_SIZE + handle_bytes)[_FH_PREFIX_SIZE:] = data[
        fh_data_offset:fh_data_end
    ]

    dir_fd = lib.open_by_handle_at(AT_FDCWD, fh_buf, O_PATH | O_CLOEXEC)
    if dir_fd < 0:
        return None

    try:
        parent = Path(os.readlink(f"/proc/self/fd/{dir_fd}"))
    except OSError:
        return None
    finally:
        os.close(dir_fd)

    # Name follows immediately after the file_handle data
    name_start = fh_data_end
    # Find null terminator
    name_end = data.find(b"\x00", name_start, end)
    if name_end == -1:
        name_end = end
    name = data[name_start:name_end].decode(errors="replace")

    if not name or name in (".", ".."):
        return None

    return parent / name


class FanotifyWatcher:
    async def __call__(
        self,
        watch_paths: list[str],
        *,
        handlers: WatcherHandlers,
    ) -> None:
        with _fanotify_fd(
            FAN_CLASS_NOTIF | FAN_REPORT_DFID_NAME, O_RDONLY | O_CLOEXEC | O_LARGEFILE
        ) as fan_fd:
            # Mark each unique filesystem once
            seen_devs: set[int] = set()
            for p in watch_paths:
                st = Path(p).resolve().stat()
                if st.st_dev not in seen_devs:
                    seen_devs.add(st.st_dev)
                    ret = lib.fanotify_mark(
                        fan_fd,
                        FAN_MARK_ADD | FAN_MARK_FILESYSTEM,
                        _FAN_MASK,
                        AT_FDCWD,
                        p.encode(),
                    )
                    if ret < 0:
                        raise OSError(ffi.errno, f"fanotify_mark failed for {p}")

            loop = asyncio.get_event_loop()
            ready = asyncio.Event()
            loop.add_reader(fan_fd, ready.set)
            watch_roots = [Path(p).resolve() for p in watch_paths]
            # cookie → (path, is_dir); fanotify move cookies are per-event pairs
            pending_from: dict[int, tuple[Path, bool]] = {}
            _move_seq = 0

            try:
                while True:
                    await ready.wait()
                    ready.clear()
                    try:
                        data = os.read(fan_fd, _READ_SIZE)
                    except OSError:
                        _L.exception("fanotify read failed")
                        continue

                    for mask, _cookie, path in _parse_events(data):
                        if path is None:
                            continue
                        if not any(path.is_relative_to(r) for r in watch_roots):
                            continue
                        is_dir = bool(mask & FAN_ONDIR)
                        try:
                            await _dispatch(
                                mask,
                                path,
                                is_dir,
                                pending_from,
                                handlers,
                            )
                        except Exception:
                            _L.exception(
                                "event handler failed: mask=%#x path=%s", mask, path
                            )
            finally:
                loop.remove_reader(fan_fd)


async def _dispatch(
    mask: int,
    path: Path,
    is_dir: bool,
    pending_from: dict[int, tuple[Path, bool]],
    handlers: WatcherHandlers,
) -> None:
    """Dispatch a single fanotify event to the appropriate handler."""
    if mask & FAN_MOVED_FROM:
        # fanotify doesn't provide reliable cookies across move pairs in all
        # kernel versions; use path-keyed tracking with a simple heuristic:
        # store most recent MOVED_FROM and match on next MOVED_TO.
        pending_from[0] = (path, is_dir)

    elif mask & FAN_MOVED_TO:
        if 0 in pending_from:
            src_path, src_is_dir = pending_from.pop(0)
            tracked = await handlers.on_move(src_path, path, is_dir)
            if not tracked:
                # Source was untracked — treat destination as new arrival
                if is_dir:
                    await handlers.on_dir_created(path, True)
                else:
                    await handlers.on_new_file(path)
        else:
            # No matching MOVED_FROM — treat as new arrival
            if is_dir:
                await handlers.on_dir_created(path, True)
            else:
                await handlers.on_new_file(path)

    elif mask & FAN_CREATE:
        if is_dir:
            await handlers.on_dir_created(path, False)
        # else: ignore — file may be partial; metadata arrives on CLOSE_WRITE

    elif mask & FAN_DELETE:
        await handlers.on_delete(path, is_dir)

    elif mask & FAN_CLOSE_WRITE:
        await handlers.on_close_write(path)
