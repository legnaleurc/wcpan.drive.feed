# wcpan.drive.feed

An HTTP server that watches local filesystem paths with inotify, computes file metadata lazily, and exposes a token-based change log API — similar to Google Drive's changes API. This enables clients to efficiently poll for incremental changes without full directory scans.

## Features

- Watches directories recursively via inotify or fanotify (Linux only)
- Computes MD5 hash, MIME type, and image/video dimensions per file
- Token-based change log API: clients request only changes since their last cursor
- Merges intermediate events (e.g. multiple modifies → single update; create+delete → skip)
- No authentication (designed for intranet use)

## Requirements

- Linux (inotify or fanotify)
- Python 3.13+
- `libmagic1` and `mediainfo` system packages

## Installation

```bash
# Install system dependencies
apt-get install libmagic1 mediainfo

# Install Python package with your chosen watcher backend
pip install "wcpan-drive-feed[inotify]"   # inotify backend
pip install "wcpan-drive-feed[fanotify]"  # fanotify backend
```

## Configuration

Create a YAML config file:

```yaml
host: "0.0.0.0"
port: 8080
database_url: "/data/db/server.db"
watches:
  media: /mnt/media
  photos: /mnt/photos
watcher:
  backend: inotify  # or "fanotify"
# Optional:
# exclude:
#   - "*.tmp"
# log_path: /data/logs/server.log
```

The `watcher.backend` field is required and must be either `"inotify"` or `"fanotify"`.

## Running

```bash
wcpan.drive.feed --config=/path/to/server.yaml
```

The `--config` flag defaults to `/data/server.yaml`.

## API

### `GET /api/v1/cursor`

Returns the current change log cursor (the latest change ID).

```json
{ "cursor": 42 }
```

### `GET /api/v1/changes?cursor=<int>`

Returns all changes since the given cursor. Missing `cursor` defaults to `0`. Invalid cursor returns HTTP 400.

```json
{
  "cursor": 57,
  "changes": [
    {
      "removed": false,
      "node": {
        "id": "...",
        "parent_id": "...",
        "name": "image.jpg",
        "is_directory": false,
        "ctime": "2026-03-04T12:00:00+00:00",
        "mtime": "2026-03-04T12:00:00+00:00",
        "mime_type": "image/jpeg",
        "hash": "d41d8cd98f00b204e9800998ecf8427e",
        "size": 204800,
        "is_image": true,
        "is_video": false,
        "width": 1920,
        "height": 1080,
        "ms_duration": 0
      }
    },
    {
      "removed": true,
      "node_id": "..."
    }
  ]
}
```

Nodes appear in two phases: first with `hash: ""` immediately after detection, then again with the full hash/MIME/dimensions after metadata computation completes. Clients should treat a second update for the same node as the final state.

### `GET /api/v1/root`

Returns the virtual super-root node (parent of all watch-root directories).

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "parent_id": null,
  "name": "",
  "is_directory": true,
  "ctime": "2026-03-04T12:00:00+00:00",
  "mtime": "2026-03-04T12:00:00+00:00",
  "mime_type": "",
  "hash": "",
  "size": 0,
  "is_image": false,
  "is_video": false,
  "width": 0,
  "height": 0,
  "ms_duration": 0
}
```

### `GET /api/v1/nodes/{id}/path`

Returns the absolute filesystem path for a node.

```json
{ "path": "/mnt/media/photos/image.jpg" }
```

Returns HTTP 404 if the node does not exist, HTTP 400 if the node is the super-root.

## Development

```bash
# Run tests
make test

# Format code
make format

# Lint
make lint
```

## How It Works

1. On startup, the server initializes the SQLite database (auto-creates schema; aborts on version mismatch), then scans all configured watch paths and inserts/updates nodes.
2. The configured watcher backend (`asyncinotify` or a custom `fanotify` backend) monitors each path for filesystem events (create, modify, delete, move).
3. File metadata (MD5, MIME type, image/video dimensions) is computed in a `ThreadPoolExecutor` to avoid blocking the event loop.
4. Every change is appended to a `changes` table. The `/changes` endpoint merges overlapping events per node — the last event wins — so clients always see the effective state.
5. The server returns HTTP 503 while the startup scan and queue drain are in progress.
