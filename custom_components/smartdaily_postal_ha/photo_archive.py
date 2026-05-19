"""Download package photos to local /config/www/packages/ for permanent archival.

The upstream Smartdaily API returns Google Cloud Storage signed URLs that expire
after ~15 minutes. Once a package is removed from the API list, its photo is lost.
This module saves a local copy on first sight so historical photos remain viewable
via /local/packages/<pd_id>.jpg.
"""

import logging
import os
from typing import List

import aiohttp
import aiofiles

_LOGGER = logging.getLogger(__name__)

PHOTO_DIR = "/config/www/packages"
DOWNLOAD_TIMEOUT = 15  # seconds per photo


async def archive_photos(hass, all_packages: List[dict]) -> None:
    """Download any package photos that aren't already on disk.

    Best-effort: individual failures do not raise. Safe to call repeatedly;
    files already present are skipped without re-downloading.
    """
    try:
        await hass.async_add_executor_job(_ensure_dir)
    except Exception as exc:
        _LOGGER.warning("Could not create photo dir %s: %s", PHOTO_DIR, exc)
        return

    timeout = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for entry in all_packages:
            pkg = entry.get("package") or {}
            pd_id = pkg.get("pd_id")
            url = pkg.get("postal_img")
            if not (pd_id and url):
                continue
            path = os.path.join(PHOTO_DIR, f"{pd_id}.jpg")
            if await hass.async_add_executor_job(os.path.exists, path):
                continue
            await _download_one(session, url, path, pd_id)


async def _download_one(session: aiohttp.ClientSession, url: str, path: str, pd_id: str) -> None:
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                _LOGGER.debug("Photo %s HTTP %s", pd_id, resp.status)
                return
            data = await resp.read()
    except Exception as exc:
        _LOGGER.debug("Photo %s fetch failed: %s", pd_id, exc)
        return

    try:
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)
        _LOGGER.info("Archived package photo %s (%d bytes)", pd_id, len(data))
    except Exception as exc:
        _LOGGER.warning("Photo %s write failed: %s", pd_id, exc)


def _ensure_dir() -> None:
    os.makedirs(PHOTO_DIR, exist_ok=True)
