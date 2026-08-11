"""Persistent, retry-safe outbox for new-package notification events."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

from homeassistant.helpers.storage import Store

from . import DOMAIN

STORAGE_VERSION = 1
STORAGE_KEY = f"{DOMAIN}.package_notification_outbox"
REPLAY_INTERVAL_SECONDS = 11 * 60
SERVICE_ACK_PACKAGE_NOTIFICATION = "ack_package_notification"


class PackageNotificationOutbox:
    """Persist baselines and unacknowledged package events across HA restarts."""

    def __init__(self, hass) -> None:
        self._store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        self._lock = asyncio.Lock()
        self._known_status: dict[str, dict[str, Any]] = {}
        self._pending: dict[str, dict[str, Any]] = {}

    async def async_load(self) -> None:
        data = await self._store.async_load() or {}
        known = data.get("known_status", {})
        pending = data.get("pending", {})
        if isinstance(known, dict):
            self._known_status = {
                str(scope): dict(statuses)
                for scope, statuses in known.items()
                if isinstance(statuses, dict)
            }
        if isinstance(pending, dict):
            self._pending = {
                str(key): dict(record)
                for key, record in pending.items()
                if isinstance(record, dict)
            }

    def previous_status(self, scope: str) -> dict[str, Any] | None:
        statuses = self._known_status.get(scope)
        return None if statuses is None else dict(statuses)

    async def async_stage(
        self,
        scope: str,
        current_status: dict[str, Any],
        new_events: dict[str, dict[str, Any]],
    ) -> None:
        """Commit the new baseline and events before any event is fired."""
        async with self._lock:
            self._known_status[scope] = dict(current_status)
            for pd_id, event_data in new_events.items():
                key = self._key(scope, pd_id)
                if key in self._pending:
                    continue
                payload = dict(event_data)
                payload["line_retry_key"] = str(uuid.uuid4())
                payload["notification_outbox_managed"] = True
                self._pending[key] = {
                    "scope": scope,
                    "pd_id": pd_id,
                    "event_data": payload,
                    "next_attempt_at": 0,
                }
            await self._async_save()

    async def async_claim_due(self, scope: str) -> list[dict[str, Any]]:
        """Return due events and durably defer their next replay."""
        now = time.time()
        claimed = []
        async with self._lock:
            for record in self._pending.values():
                if record.get("scope") != scope:
                    continue
                if float(record.get("next_attempt_at", 0)) > now:
                    continue
                record["next_attempt_at"] = now + REPLAY_INTERVAL_SECONDS
                event_data = record.get("event_data")
                if isinstance(event_data, dict):
                    claimed.append(dict(event_data))
            if claimed:
                await self._async_save()
        return claimed

    async def async_ack(self, pd_id: str) -> bool:
        """Remove every pending record for a successfully accepted package."""
        async with self._lock:
            keys = [
                key
                for key, record in self._pending.items()
                if record.get("pd_id") == pd_id
            ]
            for key in keys:
                self._pending.pop(key, None)
            if keys:
                await self._async_save()
            return bool(keys)

    async def _async_save(self) -> None:
        await self._store.async_save(
            {
                "known_status": self._known_status,
                "pending": self._pending,
            }
        )

    @staticmethod
    def _key(scope: str, pd_id: str) -> str:
        return f"{scope}|{pd_id}"
