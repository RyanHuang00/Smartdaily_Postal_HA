"""Download package photos to local /config/www/packages/ for permanent archival.

The upstream Smartdaily API returns Google Cloud Storage signed URLs that expire
after ~15 minutes. Once a package is removed from the API list, its photo is lost.
This module saves a local copy on first sight so historical photos remain viewable
via /local/packages/<pd_id>.jpg.
"""

import asyncio
import io
import logging
import os
import re
import tempfile
from typing import List, Set

import aiohttp
from PIL import Image

_LOGGER = logging.getLogger(__name__)

PHOTO_DIR = "/config/www/packages"
DOWNLOAD_TIMEOUT = 15  # seconds per photo
PD_ID_RE = re.compile(r"^[0-9a-f]{15}$")


async def archive_photos(hass, all_packages: List[dict]) -> Set[str]:
    """Download any package photos that aren't already on disk.

    Best-effort: individual failures do not raise. Safe to call repeatedly;
    files already present are skipped without re-downloading. Returns the set of
    pd_ids that have a local file after the archival attempt.
    """
    try:
        await hass.async_add_executor_job(_ensure_dir)
    except Exception as exc:
        _LOGGER.warning("Could not create photo dir %s: %s", PHOTO_DIR, exc)
        return set()

    archived: Set[str] = set()
    timeout = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for entry in all_packages:
            pkg = entry.get("package") or {}
            pd_id = pkg.get("pd_id")
            url = pkg.get("postal_img")
            if not (pd_id and url):
                continue
            if not PD_ID_RE.fullmatch(str(pd_id)):
                _LOGGER.warning("Ignoring package photo with invalid pd_id %r", pd_id)
                continue
            path = os.path.join(PHOTO_DIR, f"{pd_id}.jpg")
            if await hass.async_add_executor_job(_photo_is_decodable, path):
                archived.add(pd_id)
                continue
            if await _download_one(session, url, path, pd_id):
                archived.add(pd_id)
    return archived


async def _download_one(session: aiohttp.ClientSession, url: str, path: str, pd_id: str) -> bool:
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                _LOGGER.debug("Photo %s HTTP %s", pd_id, resp.status)
                return False
            data = await resp.read()
    except Exception as exc:
        _LOGGER.debug("Photo %s fetch failed: %s", pd_id, exc)
        return False

    try:
        await asyncio.to_thread(_atomic_commit_photo, path, data)
        _LOGGER.info("Archived package photo %s (%d bytes)", pd_id, len(data))
        return True
    except Exception as exc:
        _LOGGER.warning("Photo %s write failed: %s", pd_id, exc)
        return False


def _ensure_dir() -> None:
    os.makedirs(PHOTO_DIR, exist_ok=True)


def _verify_image_bytes(data: bytes) -> None:
    """Fully decode an upstream image before it can become the live archive."""
    if not data:
        raise ValueError("empty photo response")
    with Image.open(io.BytesIO(data)) as image:
        image.load()


def _photo_is_decodable(path: str) -> bool:
    """Reject partial/corrupt archives so a later poll can repair them."""
    try:
        with Image.open(path) as image:
            image.load()
        return True
    except Exception:  # Pillow has several format-specific decode exceptions.
        return False


def _atomic_commit_photo(path: str, data: bytes) -> None:
    """Validate and fsync a temporary file before atomically publishing it."""
    _verify_image_bytes(data)
    directory = os.path.dirname(path)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=directory,
            prefix=f".{os.path.basename(path)}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = temp_file.name
            temp_file.write(data)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, path)
        temp_path = None
        if hasattr(os, "O_DIRECTORY"):
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if temp_path is not None:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass
