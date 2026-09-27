import asyncio
import io

import pytest
from PIL import Image

from homeassistant.helpers.storage import Store
from custom_components.smartdaily_postal_ha import _async_get_outbox, DOMAIN

from custom_components.smartdaily_postal_ha import photo_archive
from custom_components.smartdaily_postal_ha.notification_outbox import (
    PackageNotificationOutbox,
    STORAGE_KEY,
)


def image_bytes(image_format="WEBP", size=(8, 8)):
    output = io.BytesIO()
    Image.new("RGB", size, "red").save(output, format=image_format)
    return output.getvalue()


def test_atomic_archive_never_publishes_invalid_or_partial_photo(tmp_path):
    destination = tmp_path / "260811328168df0.jpg"
    destination.write_bytes(b"previous-valid-sentinel")

    with pytest.raises(OSError):
        photo_archive._atomic_commit_photo(str(destination), b"truncated")

    assert destination.read_bytes() == b"previous-valid-sentinel"
    assert list(tmp_path.glob("*.tmp")) == []


def test_atomic_archive_fsyncs_and_publishes_only_decodable_photo(tmp_path):
    destination = tmp_path / "260811328168df0.jpg"

    photo_archive._atomic_commit_photo(str(destination), image_bytes())

    assert photo_archive._photo_is_decodable(str(destination)) is True
    with Image.open(destination) as image:
        image.load()
        assert image.format == "WEBP"


def test_corrupt_existing_archive_is_not_treated_as_ready(tmp_path):
    destination = tmp_path / "260811328168df0.jpg"
    destination.write_bytes(b"not-an-image")

    assert photo_archive._photo_is_decodable(str(destination)) is False


def test_archive_rejects_path_traversal_pd_id():
    assert photo_archive.PD_ID_RE.fullmatch("260811328168df0")
    assert photo_archive.PD_ID_RE.fullmatch("26092474112uvpp")
    assert photo_archive.PD_ID_RE.fullmatch("aB_9-123456")
    assert photo_archive.PD_ID_RE.fullmatch("../../secrets") is None
    assert photo_archive.PD_ID_RE.fullmatch("short") is None


def test_outbox_survives_restart_replays_and_acknowledges(monkeypatch):
    Store.data.clear()
    now = 1_700_000_000.0
    monkeypatch.setattr(
        "custom_components.smartdaily_postal_ha.notification_outbox.time.time",
        lambda: now,
    )

    async def scenario():
        first = PackageNotificationOutbox(object())
        await first.async_load()
        await first.async_stage("device:community", {"old": 1}, {})

        restarted = PackageNotificationOutbox(object())
        await restarted.async_load()
        assert restarted.previous_status("device:community") == {"old": 1}

        await restarted.async_stage(
            "device:community",
            {"old": 1, "260811328168df0": 1},
            {"260811328168df0": {"pd_id": "260811328168df0"}},
        )
        claimed = await restarted.async_claim_due("device:community")
        assert len(claimed) == 1
        assert claimed[0]["pd_id"] == "260811328168df0"
        assert len(claimed[0]["line_retry_key"]) == 36
        assert claimed[0]["notification_outbox_managed"] is True
        assert claimed[0]["notification_replay"] is False
        health = restarted.health_snapshot()
        assert health["pending_count"] == 1
        assert health["oldest_pending_seconds"] == 0
        assert health["last_successful_poll_at"] == now
        assert await restarted.async_claim_due("device:community") == []

        restarted_again = PackageNotificationOutbox(object())
        await restarted_again.async_load()
        assert await restarted_again.async_ack("260811328168df0") is True

        after_ack = PackageNotificationOutbox(object())
        await after_ack.async_load()
        assert await after_ack.async_claim_due("device:community") == []

    asyncio.run(scenario())


def test_outbox_reports_stale_pending_after_failed_delivery(monkeypatch):
    Store.data.clear()
    now = [1_700_000_000.0]
    monkeypatch.setattr(
        "custom_components.smartdaily_postal_ha.notification_outbox.time.time",
        lambda: now[0],
    )

    async def scenario():
        outbox = PackageNotificationOutbox(object())
        await outbox.async_load()
        await outbox.async_stage("device:community", {"26092474112uvpp": 1}, {
            "26092474112uvpp": {"pd_id": "26092474112uvpp"},
        })
        first = await outbox.async_claim_due("device:community")
        assert first[0]["notification_replay"] is False
        now[0] += 20 * 60
        replay = await outbox.async_claim_due("device:community")
        assert replay[0]["notification_replay"] is True
        health = outbox.health_snapshot()
        assert health["pending_count"] == 1
        assert health["oldest_pending_seconds"] == 20 * 60
        assert health["pending_ids"] == ["26092474112uvpp"]
        assert health["last_successful_poll_at"] == 1_700_000_000.0

    asyncio.run(scenario())


def test_existing_pending_records_get_an_alert_age_without_losing_delivery(monkeypatch):
    Store.data.clear()
    Store.data[STORAGE_KEY] = {
        "known_status": {"device:community": {"26092474112uvpp": 1}},
        "pending": {"device:community|26092474112uvpp": {
            "scope": "device:community",
            "pd_id": "26092474112uvpp",
            "event_data": {"pd_id": "26092474112uvpp", "line_retry_key": "stable"},
            "next_attempt_at": 0,
        }},
    }
    monkeypatch.setattr(
        "custom_components.smartdaily_postal_ha.notification_outbox.time.time",
        lambda: 1_700_000_000.0,
    )

    async def scenario():
        outbox = PackageNotificationOutbox(object())
        await outbox.async_load()
        assert outbox.health_snapshot()["pending_count"] == 1
        assert outbox.health_snapshot()["oldest_pending_seconds"] == 0
        assert (await outbox.async_claim_due("device:community"))[0]["line_retry_key"] == "stable"

    asyncio.run(scenario())


def test_missing_baseline_flags_unclaimed_packages_for_manual_reconciliation():
    Store.data.clear()

    async def scenario():
        outbox = PackageNotificationOutbox(object())
        await outbox.async_load()
        await outbox.async_stage(
            "device:community",
            {"26092474112uvpp": 1},
            {},
            baseline_unverified_ids=["26092474112uvpp"],
        )
        assert outbox.health_snapshot()["baseline_unverified_ids"] == ["26092474112uvpp"]
        assert await outbox.async_claim_due("device:community") == []

        restarted = PackageNotificationOutbox(object())
        await restarted.async_load()
        assert restarted.health_snapshot()["baseline_unverified_count"] == 1
        await restarted.async_stage("device:community", {"26092474112uvpp": 2}, {})
        assert restarted.health_snapshot()["baseline_unverified_count"] == 0

    asyncio.run(scenario())


def test_pickup_and_collection_replay_independently_until_their_own_ack(monkeypatch):
    Store.data.clear()
    now = [1_700_000_000.0]
    monkeypatch.setattr(
        "custom_components.smartdaily_postal_ha.notification_outbox.time.time",
        lambda: now[0],
    )

    async def scenario():
        outbox = PackageNotificationOutbox(object())
        await outbox.async_load()
        await outbox.async_stage(
            "device:community", {"pkg": 2}, {"pkg": {"pd_id": "pkg"}},
            pickup_events={"pkg": {"pd_id": "pkg"}},
        )
        await outbox.async_stage_collection(
            "device", {"community:42:today": True},
            {"community:42:today": {"collection_id": "community:42:today"}},
        )
        claimed = await outbox.async_claim_due("device:community")
        claimed += await outbox.async_claim_due("collection:device")
        assert {event["notification_kind"] for event in claimed} == {
            "package_arrival", "package_pickup", "collection",
        }
        assert len({event["line_retry_key"] for event in claimed}) == 3
        assert await outbox.async_ack("pkg", "package_arrival") is True
        assert outbox.health_snapshot()["pending_by_kind"]["package_pickup"] == 1

        restarted = PackageNotificationOutbox(object())
        await restarted.async_load()
        now[0] += 20 * 60
        replay = await restarted.async_claim_due("device:community")
        assert len(replay) == 1 and replay[0]["notification_kind"] == "package_pickup"
        assert replay[0]["notification_replay"] is True
        assert await restarted.async_ack("pkg", "package_pickup") is True
        assert await restarted.async_ack("community:42:today", "collection") is True
        assert restarted.health_snapshot()["pending_count"] == 0

    asyncio.run(scenario())


def test_collection_baseline_and_poll_health_survive_restart(monkeypatch):
    Store.data.clear()
    now = [1_700_000_000.0]
    monkeypatch.setattr(
        "custom_components.smartdaily_postal_ha.notification_outbox.time.time",
        lambda: now[0],
    )

    async def scenario():
        outbox = PackageNotificationOutbox(object())
        await outbox.async_load()
        assert outbox.previous_collection_ids("device") is None
        await outbox.async_stage_collection("device", {"existing": True}, {})
        assert outbox.health_snapshot()["baseline_unverified_collection_count"] == 1
        now[0] += 60
        restarted = PackageNotificationOutbox(object())
        await restarted.async_load()
        assert restarted.previous_collection_ids("device") == {"existing"}
        assert restarted.health_snapshot()["last_successful_collection_poll_at"] == 1_700_000_000.0
        await restarted.async_stage_collection(
            "device", {"existing": False, "new": True},
            {"new": {"collection_id": "new"}},
        )
        assert restarted.health_snapshot()["baseline_unverified_collection_count"] == 0
        assert restarted.health_snapshot()["pending_by_kind"]["collection"] == 1

    asyncio.run(scenario())


def test_unverified_baseline_requires_explicit_reconciliation_ack():
    Store.data.clear()

    async def scenario():
        outbox = PackageNotificationOutbox(object())
        await outbox.async_load()
        await outbox.async_stage(
            "device:community", {"pkg": 1}, {}, baseline_unverified_ids=["pkg"]
        )
        await outbox.async_stage_collection("device", {"collection": True}, {})
        assert await outbox.async_ack_baseline("package", "pkg") is True
        assert await outbox.async_ack_baseline("collection", "collection") is True
        assert outbox.health_snapshot()["baseline_unverified_count"] == 0
        assert outbox.health_snapshot()["baseline_unverified_collection_count"] == 0

        restarted = PackageNotificationOutbox(object())
        await restarted.async_load()
        assert restarted.health_snapshot()["baseline_unverified_count"] == 0
        assert restarted.health_snapshot()["baseline_unverified_collection_count"] == 0

    asyncio.run(scenario())


def test_ack_services_are_registered_for_each_durable_event_kind():
    Store.data.clear()

    class Services:
        def __init__(self):
            self.handlers = {}

        def has_service(self, domain, service):
            return (domain, service) in self.handlers

        def async_register(self, domain, service, handler):
            self.handlers[(domain, service)] = handler

    class Hass:
        def __init__(self):
            self.data = {}
            self.services = Services()

    async def scenario():
        hass = Hass()
        outbox = await _async_get_outbox(hass)
        assert {service for domain, service in hass.services.handlers if domain == DOMAIN} == {
            "ack_package_notification", "ack_pickup_notification",
            "ack_collection_notification", "ack_baseline_reconciliation",
        }
        await outbox.async_stage("scope", {"pkg": 2}, {}, pickup_events={"pkg": {"pd_id": "pkg"}})
        call = type("Call", (), {"data": {"pd_id": "pkg"}})()
        await hass.services.handlers[(DOMAIN, "ack_pickup_notification")](call)
        assert outbox.health_snapshot()["pending_count"] == 0

    asyncio.run(scenario())
