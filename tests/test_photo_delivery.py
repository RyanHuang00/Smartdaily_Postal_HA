import asyncio
import io

import pytest
from PIL import Image

from homeassistant.helpers.storage import Store

from custom_components.smartdaily_postal_ha import photo_archive
from custom_components.smartdaily_postal_ha.notification_outbox import (
    PackageNotificationOutbox,
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
    assert photo_archive.PD_ID_RE.fullmatch("../../secrets") is None


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
        assert await restarted.async_claim_due("device:community") == []

        restarted_again = PackageNotificationOutbox(object())
        await restarted_again.async_load()
        assert await restarted_again.async_ack("260811328168df0") is True

        after_ack = PackageNotificationOutbox(object())
        await after_ack.async_load()
        assert await after_ack.async_claim_due("device:community") == []

    asyncio.run(scenario())
