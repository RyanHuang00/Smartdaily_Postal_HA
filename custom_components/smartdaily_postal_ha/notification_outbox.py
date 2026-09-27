"""Persistent, retry-safe outbox for Smartdaily notification events."""

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
SERVICE_ACK_PICKUP_NOTIFICATION = "ack_pickup_notification"
SERVICE_ACK_COLLECTION_NOTIFICATION = "ack_collection_notification"
SERVICE_ACK_BASELINE_RECONCILIATION = "ack_baseline_reconciliation"
KIND_ARRIVAL = "package_arrival"
KIND_PICKUP = "package_pickup"
KIND_COLLECTION = "collection"


class PackageNotificationOutbox:
    """Persist baselines and unacknowledged package events across HA restarts."""

    def __init__(self, hass) -> None:
        self._store = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        self._lock = asyncio.Lock()
        self._known_status: dict[str, dict[str, Any]] = {}
        self._known_collection_ids: dict[str, list[str]] = {}
        self._pending: dict[str, dict[str, Any]] = {}
        self._unverified_baseline: dict[str, list[str]] = {}
        self._unverified_collection_baseline: dict[str, list[str]] = {}
        self._last_successful_poll_at: float | None = None
        self._last_successful_collection_poll_at: float | None = None

    async def async_load(self) -> None:
        data = await self._store.async_load() or {}
        known = data.get("known_status", {})
        known_collections = data.get("known_collection_ids", {})
        pending = data.get("pending", {})
        unverified = data.get("unverified_baseline", {})
        unverified_collections = data.get("unverified_collection_baseline", {})
        self._last_successful_poll_at = data.get("last_successful_poll_at")
        self._last_successful_collection_poll_at = data.get("last_successful_collection_poll_at")
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
            # Older stored records predate age tracking. Start their alert clock
            # now; they remain pending and are never silently discarded.
            for record in self._pending.values():
                record.setdefault("created_at", time.time())
        if isinstance(known_collections, dict):
            self._known_collection_ids = {
                str(scope): [str(item_id) for item_id in ids]
                for scope, ids in known_collections.items()
                if isinstance(ids, list)
            }
        if isinstance(unverified, dict):
            self._unverified_baseline = {
                str(scope): [str(pd_id) for pd_id in ids]
                for scope, ids in unverified.items()
                if isinstance(ids, list)
            }
        if isinstance(unverified_collections, dict):
            self._unverified_collection_baseline = {
                str(scope): [str(item_id) for item_id in ids]
                for scope, ids in unverified_collections.items()
                if isinstance(ids, list)
            }

    def previous_status(self, scope: str) -> dict[str, Any] | None:
        statuses = self._known_status.get(scope)
        return None if statuses is None else dict(statuses)

    def previous_collection_ids(self, scope: str) -> set[str] | None:
        ids = self._known_collection_ids.get(scope)
        return None if ids is None else set(ids)

    async def async_stage(
        self,
        scope: str,
        current_status: dict[str, Any],
        new_events: dict[str, dict[str, Any]],
        baseline_unverified_ids: list[str] | None = None,
        pickup_events: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        """Commit the new baseline and events before any event is fired."""
        async with self._lock:
            self._last_successful_poll_at = time.time()
            if scope not in self._known_status and baseline_unverified_ids:
                self._unverified_baseline[scope] = sorted(set(baseline_unverified_ids))
            if scope in self._unverified_baseline:
                self._unverified_baseline[scope] = [
                    pd_id for pd_id in self._unverified_baseline[scope]
                    if current_status.get(pd_id) != 2
                ]
            self._known_status[scope] = dict(current_status)
            for pd_id, event_data in new_events.items():
                self._stage_event(scope, pd_id, KIND_ARRIVAL, event_data)
            for pd_id, event_data in (pickup_events or {}).items():
                self._stage_event(scope, pd_id, KIND_PICKUP, event_data)
            await self._async_save()

    async def async_stage_collection(
        self,
        device_scope: str,
        current_status: dict[str, bool],
        new_events: dict[str, dict[str, Any]],
    ) -> None:
        """Persist account-wide collection identity and outgoing events."""
        scope = f"collection:{device_scope}"
        async with self._lock:
            self._last_successful_collection_poll_at = time.time()
            if device_scope not in self._known_collection_ids:
                self._unverified_collection_baseline[device_scope] = sorted(
                    item_id for item_id, uncollected in current_status.items() if uncollected
                )
            if device_scope in self._unverified_collection_baseline:
                self._unverified_collection_baseline[device_scope] = [
                    item_id for item_id in self._unverified_collection_baseline[device_scope]
                    if current_status.get(item_id) is not False
                ]
            self._known_collection_ids[device_scope] = sorted(
                set(self._known_collection_ids.get(device_scope, [])) | set(current_status)
            )
            for item_id, event_data in new_events.items():
                self._stage_event(scope, item_id, KIND_COLLECTION, event_data)
            await self._async_save()

    def _stage_event(self, scope: str, item_id: str, kind: str, event_data: dict[str, Any]) -> None:
        key = self._key(scope, item_id) if kind == KIND_ARRIVAL else f"{scope}|{kind}|{item_id}"
        if key in self._pending:
            return
        payload = dict(event_data)
        payload["line_retry_key"] = str(uuid.uuid4())
        payload["line_fallback_retry_key"] = str(uuid.uuid4())
        payload["notification_outbox_managed"] = True
        payload["notification_kind"] = kind
        self._pending[key] = {
            "scope": scope,
            "pd_id": item_id,
            "kind": kind,
            "event_data": payload,
            "next_attempt_at": 0,
            "created_at": time.time(),
        }

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
                is_replay = float(record.get("next_attempt_at", 0)) > 0
                record["next_attempt_at"] = now + REPLAY_INTERVAL_SECONDS
                event_data = record.get("event_data")
                if isinstance(event_data, dict):
                    payload = dict(event_data)
                    payload["notification_replay"] = is_replay
                    payload.setdefault("notification_kind", record.get("kind", KIND_ARRIVAL))
                    claimed.append(payload)
            if claimed:
                await self._async_save()
        return claimed

    async def async_ack(self, pd_id: str, kind: str = KIND_ARRIVAL) -> bool:
        """Acknowledge only one delivered notification kind for an item."""
        async with self._lock:
            keys = [
                key
                for key, record in self._pending.items()
                if record.get("pd_id") == pd_id
                and record.get("kind", KIND_ARRIVAL) == kind
            ]
            for key in keys:
                self._pending.pop(key, None)
            if keys:
                await self._async_save()
            return bool(keys)

    async def async_ack_baseline(self, kind: str, item_id: str) -> bool:
        """Clear a first-poll uncertainty only after a human reconciles it."""
        if kind not in ("package", "collection"):
            return False
        baselines = (
            self._unverified_baseline if kind == "package"
            else self._unverified_collection_baseline
        )
        async with self._lock:
            found = False
            for scope, ids in baselines.items():
                if item_id in ids:
                    baselines[scope] = [known_id for known_id in ids if known_id != item_id]
                    found = True
            if found:
                await self._async_save()
            return found

    def health_snapshot(self) -> dict[str, Any]:
        """Expose durable delivery lag for an independent HA watchdog."""
        now = time.time()
        records = list(self._pending.values())
        oldest = min((float(r["created_at"]) for r in records), default=None)
        baseline_ids = sorted({pd_id for ids in self._unverified_baseline.values() for pd_id in ids})
        collection_baseline_ids = sorted({
            item_id for ids in self._unverified_collection_baseline.values() for item_id in ids
        })
        pending_by_kind = {kind: 0 for kind in (KIND_ARRIVAL, KIND_PICKUP, KIND_COLLECTION)}
        for record in records:
            kind = record.get("kind", KIND_ARRIVAL)
            pending_by_kind[kind] = pending_by_kind.get(kind, 0) + 1
        return {
            "pending_count": len(records),
            "pending_by_kind": pending_by_kind,
            "pending_ids": sorted(str(r["pd_id"]) for r in records),
            "oldest_pending_seconds": max(0, int(now - oldest)) if oldest is not None else 0,
            "last_successful_poll_at": self._last_successful_poll_at,
            "last_successful_collection_poll_at": self._last_successful_collection_poll_at,
            "baseline_unverified_count": len(baseline_ids),
            "baseline_unverified_ids": baseline_ids,
            "baseline_unverified_collection_count": len(collection_baseline_ids),
        }

    async def _async_save(self) -> None:
        await self._store.async_save(
            {
                "known_status": self._known_status,
                "known_collection_ids": self._known_collection_ids,
                "pending": self._pending,
                "unverified_baseline": self._unverified_baseline,
                "unverified_collection_baseline": self._unverified_collection_baseline,
                "last_successful_poll_at": self._last_successful_poll_at,
                "last_successful_collection_poll_at": self._last_successful_collection_poll_at,
            }
        )

    @staticmethod
    def _key(scope: str, pd_id: str) -> str:
        return f"{scope}|{pd_id}"
