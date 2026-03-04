# wcpan.drive.feed

An HTTP server that watches local filesystem paths with inotify, computes file metadata lazily, and exposes a token-based change log API — similar to Google Drive's changes API. This enables clients to efficiently poll for incremental changes without full directory scans.

## Features

- Watches directories recursively via inotify (Linux only)
- Computes MD5 hash, MIME type, and image/video dimensions per file
- Token-based change log API: clients request only changes since their last cursor
- Merges intermediate events (e.g. multiple modifies → single update; create+delete → skip)
- No authentication (designed for intranet use)

## Requirements

- Linux (inotify)
- Python 3.12+
- `libmagic1` and `mediainfo` system packages

## Installation

```bash
# Install system dependencies
apt-get install libmagic1 mediainfo

# Install Python dependencies
make venv
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
```

## Running

```bash
uv run wcpan.drive.feed --config /path/to/server.yaml
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

1. On startup, the server checks the database schema version. A new database is initialized automatically; a version mismatch raises an error and stops the server. Then it scans all configured watch paths and inserts/updates nodes in the local SQLite database.
2. watchdog monitors each path for filesystem events (create, modify, delete, move).
3. File metadata (MD5, MIME type, image/video dimensions) is computed in a `ProcessPoolExecutor` to avoid blocking the event loop.
4. Every change is appended to a `changes` table. The `/changes` endpoint merges overlapping events per node — the last event wins — so clients always see the effective state.
