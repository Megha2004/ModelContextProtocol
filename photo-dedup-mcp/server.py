#!/usr/bin/env python3
"""
Photo Duplicate Finder — MCP Server (Non-blocking version)
"""

import asyncio
import shutil
from pathlib import Path
from typing import Any
from concurrent.futures import ThreadPoolExecutor

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from PIL import Image
Image.MAX_IMAGE_PIXELS = None

import imagehash

SIMILARITY_THRESHOLD = 10
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp', '.bmp', '.gif', '.tiff'}

app = Server("photo-dedup")
executor = ThreadPoolExecutor(max_workers=8)


def get_image_files(folder: Path):
    files = []
    for f in folder.rglob("*"):
        if f.suffix.lower() in IMAGE_EXTENSIONS and f.is_file():
            if "duplicates" not in str(f):
                files.append(f)
    return files


def hash_one(img_path: Path):
    try:
        with Image.open(img_path) as img:
            img.draft('RGB', (64, 64))
            return img_path, imagehash.phash(img)
    except Exception:
        return img_path, None


def _do_scan(folder: Path, threshold: int):
    """Run the full scan in a thread (blocking work)."""
    image_files = get_image_files(folder)
    if not image_files:
        return [], []

    # Hash all images using threads
    hashes = {}
    futures = [executor.submit(hash_one, f) for f in image_files]
    for future in futures:
        path, h = future.result()
        if h is not None:
            hashes[path] = h

    # Find duplicate groups
    paths = list(hashes.keys())
    visited = set()
    duplicate_groups = []
    for i, path_a in enumerate(paths):
        if path_a in visited:
            continue
        group = [path_a]
        for path_b in paths[i+1:]:
            if path_b in visited:
                continue
            if hashes[path_a] - hashes[path_b] <= threshold:
                group.append(path_b)
                visited.add(path_b)
        if len(group) > 1:
            visited.add(path_a)
            duplicate_groups.append(group)

    return image_files, duplicate_groups


@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="find_duplicates",
            description="Scan a folder and report visually similar/duplicate photos. Does NOT move or delete anything.",
            inputSchema={
                "type": "object",
                "properties": {
                    "folder": {"type": "string", "description": "Full path to the photos folder"},
                    "threshold": {"type": "integer", "description": "Similarity threshold 0-20 (0=identical, 10=very similar). Default: 10"}
                },
                "required": ["folder"]
            }
        ),
        Tool(
            name="move_duplicates",
            description="Find duplicate photos and MOVE them to a 'duplicates' subfolder. Keeps the largest file in each group.",
            inputSchema={
                "type": "object",
                "properties": {
                    "folder": {"type": "string", "description": "Full path to the photos folder"},
                    "threshold": {"type": "integer", "description": "Similarity threshold 0-20 (0=identical, 10=very similar). Default: 10"}
                },
                "required": ["folder"]
            }
        )
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    folder = Path(arguments["folder"])
    threshold = int(arguments.get("threshold", SIMILARITY_THRESHOLD))

    if not folder.exists() or not folder.is_dir():
        return [TextContent(type="text", text=f"❌ Folder not found: {folder}")]

    # Run blocking scan in thread so MCP doesn't freeze
    loop = asyncio.get_event_loop()
    image_files, duplicate_groups = await loop.run_in_executor(
        executor, _do_scan, folder, threshold
    )

    if not image_files:
        return [TextContent(type="text", text="❌ No images found in that folder.")]

    if name == "find_duplicates":
        if not duplicate_groups:
            return [TextContent(type="text", text=f"🎉 No duplicates found in {folder}. Your folder is clean!")]

        lines = [
            f"📁 Folder : {folder}",
            f"🖼️  Scanned: {len(image_files)} images",
            f"⚠️  Found {len(duplicate_groups)} duplicate group(s)\n"
        ]
        for i, group in enumerate(duplicate_groups, 1):
            keeper = max(group, key=lambda p: p.stat().st_size)
            lines.append(f"Group {i} ({len(group)} images):")
            lines.append(f"  ✅ Keep     : {keeper.name}")
            for p in group:
                if p != keeper:
                    lines.append(f"  📦 Duplicate: {p.name}")
        lines.append(f"\n💡 Say 'move duplicates in {folder}' to move them out.")
        return [TextContent(type="text", text="\n".join(lines))]

    elif name == "move_duplicates":
        if not duplicate_groups:
            return [TextContent(type="text", text=f"🎉 No duplicates found in {folder}. Nothing to move.")]

        duplicates_folder = folder / "duplicates"
        duplicates_folder.mkdir(exist_ok=True)
        total_moved = 0
        log = []

        for i, group in enumerate(duplicate_groups, 1):
            keeper = max(group, key=lambda p: p.stat().st_size)
            to_move = [p for p in group if p != keeper]
            log.append(f"Group {i}: keeping → {keeper.name}")
            for dup in to_move:
                dest = duplicates_folder / dup.name
                if dest.exists():
                    dest = duplicates_folder / f"{dup.stem}_{i}{dup.suffix}"
                shutil.move(str(dup), str(dest))
                log.append(f"  📦 Moved: {dup.name}")
                total_moved += 1

        lines = [
            f"✅ Done! Moved {total_moved} duplicate(s)",
            f"📦 Location: {duplicates_folder}",
            f"💡 Review that folder and delete manually when ready.\n",
            *log
        ]
        return [TextContent(type="text", text="\n".join(lines))]

    return [TextContent(type="text", text=f"❌ Unknown tool: {name}")]


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())

if __name__ == "__main__":
    asyncio.run(main())