import argparse
import sys
from pathlib import Path

import yaml

from ._db import get_ancestor_chain, read_only, read_write
from ._metadata import _get_media_info


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill media info for video nodes")
    parser.add_argument(
        "--config",
        default="/data/server.yaml",
        help="Path to YAML config file (default: /data/server.yaml)",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    database_url: str = raw["database_url"]
    watches: dict[str, str] = dict(raw.get("watches", {}))

    with read_only(database_url) as cursor:
        cursor.execute(
            "SELECT node_id, mime_type FROM nodes "
            "WHERE is_video = 1 AND width = 0 AND height = 0 AND ms_duration = 0"
        )
        rows = cursor.fetchall()

    node_ids = [(row["node_id"], row["mime_type"]) for row in rows]
    print(f"Found {len(node_ids)} video nodes with missing media info")

    updated = 0
    skipped = 0
    failed = 0

    for node_id, mime_type in node_ids:
        chain = get_ancestor_chain(database_url, node_id)
        if chain is None:
            print(f"  FAILED  {node_id}: ancestor chain not found")
            failed += 1
            continue

        namespace_root = chain[-1]
        watch_base_str = watches.get(namespace_root.name)
        if watch_base_str is None:
            print(
                f"  SKIP    {node_id}: namespace '{namespace_root.name}' not in watches"
            )
            skipped += 1
            continue

        watch_base = Path(watch_base_str).resolve()
        relative_parts = [c.name for c in reversed(chain[:-1])]
        actual_path = watch_base.joinpath(*relative_parts)

        try:
            _, _, w, h, ms = _get_media_info(actual_path, mime_type)
        except Exception as exc:
            print(f"  FAILED  {actual_path}: {exc}")
            failed += 1
            continue

        if w == 0 and h == 0 and ms == 0:
            print(f"  SKIP    {actual_path}: still no media info")
            skipped += 1
            continue

        with read_write(database_url) as cursor:
            cursor.execute(
                "UPDATE nodes SET width = ?, height = ?, ms_duration = ? WHERE node_id = ?",
                (w, h, ms, node_id),
            )
            cursor.execute(
                "INSERT INTO changes (node_id, is_removed) VALUES (?, 0)",
                (node_id,),
            )

        print(f"  UPDATED {actual_path}: {w}x{h} {ms}ms")
        updated += 1

    print(f"\nDone: {updated} updated, {skipped} skipped, {failed} failed")
