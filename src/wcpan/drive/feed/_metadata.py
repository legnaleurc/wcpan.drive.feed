import hashlib
from logging import getLogger
from pathlib import Path

from ._types import FileMetadata


_L = getLogger(__name__)


def compute_file_metadata(path: Path) -> FileMetadata:
    import magic

    _L.debug("computing metadata: %s", path)
    size = path.stat().st_size
    mime_type = magic.from_file(path, mime=True)  # type: ignore
    _L.debug("mime type: %s size: %d path: %s", mime_type, size, path)
    hash_ = _compute_md5(path)
    _L.debug("hash: %s path: %s", hash_, path)
    is_image, is_video, w, h, ms = _get_media_info(path, mime_type)
    _L.debug(
        "media dims: is_image=%s is_video=%s w=%d h=%d ms=%d path: %s",
        is_image,
        is_video,
        w,
        h,
        ms,
        path,
    )
    return FileMetadata(
        mime_type=mime_type,
        hash=hash_,
        size=size,
        is_image=is_image,
        is_video=is_video,
        width=w,
        height=h,
        ms_duration=ms,
    )


def _compute_md5(path: Path) -> str:
    CHUNK_SIZE = 64 * 1024
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _get_media_info(path: Path, mime_type: str) -> tuple[bool, bool, int, int, int]:
    from pymediainfo import MediaInfo

    if mime_type.startswith("image/"):
        try:
            info = MediaInfo.parse(
                path, mediainfo_options={"File_TestContinuousFileNames": "0"}
            )
            track = info.image_tracks[0]
            width = track.width
            height = track.height
            if isinstance(width, int) and isinstance(height, int):
                return True, False, width, height, 0
        except Exception:
            _L.debug("media dims failed for %s", path, exc_info=True)
        return True, False, 0, 0, 0

    if mime_type.startswith("video/"):
        try:
            info = MediaInfo.parse(path)
            container = info.general_tracks[0]
            video = info.video_tracks[0]
            width = video.width
            height = video.height
            ms_duration = container.duration
            if isinstance(ms_duration, str):
                ms_duration = int(float(ms_duration))
            if (
                isinstance(width, int)
                and isinstance(height, int)
                and isinstance(ms_duration, int)
            ):
                return False, True, width, height, ms_duration
        except Exception:
            _L.debug("media dims failed for %s", path, exc_info=True)
        return False, True, 0, 0, 0

    return False, False, 0, 0, 0
