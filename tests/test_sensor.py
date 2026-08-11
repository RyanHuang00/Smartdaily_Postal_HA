import asyncio
import pytest
from custom_components.smartdaily_postal_ha.sensor import (
    EVENT_NEW_COLLECTION,
    PackageTrackerSensor,
    PackageSlotSensor,
    SmartdailyDataUpdateCoordinator,
    normalize_collection_response,
    parse_time,
)


# Test module-level parse_time function
def test_parse_time_just_now():
    result = parse_time("剛剛")
    assert result is not None
    assert len(result) == 16  # 格式 yyyy/MM/dd HH:MM

def test_parse_time_yesterday():
    result = parse_time("昨天 12:34")
    assert result is not None
    assert len(result) == 16

def test_parse_time_hours_ago():
    result = parse_time("2小時以前")
    assert result is not None
    assert len(result) == 16

def test_parse_time_minutes_ago():
    result = parse_time("15分鐘以前")
    assert result is not None
    assert len(result) == 16

def test_parse_time_standard():
    # 測試標準格式（假設為 UTC 時間）
    result = parse_time("2024/05/20 10:00")
    assert result is not None
    assert len(result) == 16


# Test PackageTrackerSensor backward compatibility
class MockCoordinator:
    """Mock coordinator for testing."""
    def __init__(self, data=None):
        self.data = data
        self.last_update_success = True


def make_sensor():
    """建立一個最小化的 sensor 實例"""
    coordinator = MockCoordinator()
    return PackageTrackerSensor(coordinator, "test_device", "test_com")


def make_slot_sensor(slot=1, data=None):
    """建立一個 PackageSlotSensor 實例"""
    coordinator = MockCoordinator(data)
    return PackageSlotSensor(coordinator, "test_device", "test_com", slot)


def test_sensor_init():
    sensor = make_sensor()
    assert sensor._name == "My Package Tracker"
    assert sensor._com_id == "test_com"
    assert sensor._device_id == "test_device"


def test_sensor_parse_time_wrapper():
    """Test that instance method parse_time still works for backward compatibility."""
    sensor = make_sensor()
    result = sensor.parse_time("剛剛")
    assert result is not None
    assert len(result) == 16


# Test PackageSlotSensor
def test_slot_sensor_init():
    sensor = make_slot_sensor(slot=2)
    assert sensor._name == "包裹 2"
    assert sensor._slot == 2
    assert sensor._unique_id == "test_device_test_com_slot_2"


def test_slot_sensor_state_no_package():
    sensor = make_slot_sensor(slot=1, data={"unclaimed_packages": []})
    assert sensor.state == "無包裹"


def test_slot_sensor_state_with_package():
    data = {
        "unclaimed_packages": [
            {"package": {"p_name": "測試包裹", "p_status": 1}, "parsed_time": "2024/05/20 10:00"}
        ]
    }
    sensor = make_slot_sensor(slot=1, data=data)
    assert sensor.state == "測試包裹"


def test_slot_sensor_state_slot_out_of_range():
    data = {
        "unclaimed_packages": [
            {"package": {"p_name": "測試包裹", "p_status": 1}, "parsed_time": "2024/05/20 10:00"}
        ]
    }
    sensor = make_slot_sensor(slot=3, data=data)  # 只有 1 個包裹，slot 3 應該無包裹
    assert sensor.state == "無包裹"


def test_slot_sensor_attributes_no_package():
    sensor = make_slot_sensor(slot=1, data={"unclaimed_packages": []})
    attrs = sensor.extra_state_attributes
    assert attrs["slot"] == 1
    assert attrs["has_package"] is False


def test_slot_sensor_attributes_with_package():
    data = {
        "unclaimed_packages": [
            {
                "package": {
                    "pd_id": "123",
                    "p_name": "測試包裹",
                    "p_status": 1,
                    "create_date": "2024/05/20 10:00",
                    "postal_typeText": "一般包裹",
                    "transport_code": "ABC123",
                    "p_note": "備註",
                    "postal_img": "https://example.com/img.jpg",
                },
                "parsed_time": "2024/05/20 10:00"
            }
        ]
    }
    sensor = make_slot_sensor(slot=1, data=data)
    attrs = sensor.extra_state_attributes
    assert attrs["slot"] == 1
    assert attrs["has_package"] is True
    assert attrs["pd_id"] == "123"
    assert attrs["p_name"] == "測試包裹"
    assert attrs["postal_img"] == "https://example.com/img.jpg"


def test_new_package_event_waits_for_photo_archive(monkeypatch):
    order = []
    events = []

    class FakeBus:
        def async_fire(self, event_type, event_data):
            order.append("fire")
            events.append((event_type, event_data))

    class FakeHass:
        bus = FakeBus()

        async def async_add_executor_job(self, func, *args):
            return func(*args)

        def async_create_task(self, coro):
            coro.close()

    new_pid = "abc123abc123abc"
    coordinator = SmartdailyDataUpdateCoordinator(FakeHass(), "device", "community")
    coordinator._previous_pd_status = {"old123old123old": 1}

    result = {
        "all_packages": [
            {
                "package": {
                    "pd_id": new_pid,
                    "p_status": 1,
                    "postal_img": "https://example.com/package.jpg",
                },
                "parsed_time": "2026/06/26 10:00",
            }
        ],
        "unclaimed_count": 1,
    }

    monkeypatch.setattr(coordinator, "_fetch_data", lambda: result)

    async def fake_archive_photos(hass, packages):
        order.append("archive")
        assert packages == result["all_packages"]
        return {new_pid}

    monkeypatch.setattr(
        "custom_components.smartdaily_postal_ha.sensor.archive_photos",
        fake_archive_photos,
    )

    asyncio.run(coordinator._async_update_data())

    assert order == ["archive", "fire"]
    assert events[0][1]["pd_id"] == new_pid
    assert events[0][1]["photo_local_ready"] is True


class RecordingBus:
    def __init__(self):
        self.events = []

    def async_fire(self, event_type, event_data):
        self.events.append((event_type, event_data))


class CollectionFakeHass:
    def __init__(self):
        self.bus = RecordingBus()

    async def async_add_executor_job(self, func, *args):
        return func(*args)

    def async_create_task(self, coro):
        # Photo archiving is unrelated to collection event tests.
        coro.close()


def collection_item(serial_num, is_end="no", **extra):
    serial_text = str(serial_num)
    item = {
        "serial_num": serial_num,
        "sdate": f"2026/07/{serial_text[-2:]} 10:00",
        "is_end": is_end,
        "from_name": "管理室",
        "note": "原始欄位需保留",
    }
    item.update(extra)
    return item


def collection_poll(raw_items=None, success=True):
    items = []
    if success:
        valid, items = normalize_collection_response(
            {"Data": raw_items or []}, "community"
        )
        assert valid is True
    return {
        "all_packages": [],
        "unclaimed_count": 0,
        "collection_fetch_success": success,
        "collection_items": items,
        "collection_uncollected_count": sum(
            str(item.get("is_end", "")).strip().lower() == "no"
            for item in items
        ),
    }


def run_collection_polls(monkeypatch, coordinator, *polls):
    results = iter(polls)
    monkeypatch.setattr(coordinator, "_fetch_data", lambda: next(results))
    for _ in polls:
        asyncio.run(coordinator._async_update_data())


def collection_events(hass):
    return [
        data
        for event_type, data in hass.bus.events
        if event_type == EVENT_NEW_COLLECTION
    ]


def test_collection_first_successful_poll_only_builds_baseline(monkeypatch):
    hass = CollectionFakeHass()
    coordinator = SmartdailyDataUpdateCoordinator(hass, "device", "community")

    run_collection_polls(
        monkeypatch,
        coordinator,
        collection_poll([collection_item("01")]),
    )

    assert collection_events(hass) == []
    assert coordinator._known_collection_ids == {
        "account:01:2026/07/01 10:00"
    }


def test_collection_new_unclaimed_item_fires_once(monkeypatch):
    hass = CollectionFakeHass()
    coordinator = SmartdailyDataUpdateCoordinator(hass, "device", "community")
    old_item = collection_item("01")
    new_item = collection_item("02", CollectionImage="https://example.com/item.jpg")

    run_collection_polls(
        monkeypatch,
        coordinator,
        collection_poll([old_item]),
        collection_poll([old_item, new_item]),
        collection_poll([old_item, new_item]),
    )

    events = collection_events(hass)
    assert len(events) == 1
    assert events[0]["serial_num"] == "02"
    assert events[0]["collection_id"] == "account:02:2026/07/02 10:00"
    assert events[0]["uncollected_count"] == 2
    assert events[0]["note"] == "原始欄位需保留"
    assert events[0]["CollectionImage"] == "https://example.com/item.jpg"


def test_collection_reorder_and_temporary_disappearance_do_not_duplicate(monkeypatch):
    hass = CollectionFakeHass()
    coordinator = SmartdailyDataUpdateCoordinator(hass, "device", "community")
    first = collection_item("01")
    second = collection_item("02")

    run_collection_polls(
        monkeypatch,
        coordinator,
        collection_poll([first, second]),
        collection_poll([second, first]),
        collection_poll([]),
        collection_poll([first, second]),
    )

    assert collection_events(hass) == []


def test_collection_failed_poll_does_not_establish_or_replace_baseline(monkeypatch):
    hass = CollectionFakeHass()
    coordinator = SmartdailyDataUpdateCoordinator(hass, "device", "community")
    first = collection_item("01")
    second = collection_item("02")

    run_collection_polls(
        monkeypatch,
        coordinator,
        collection_poll(success=False),
        collection_poll([first]),
        collection_poll(success=False),
        collection_poll([first, second]),
    )

    events = collection_events(hass)
    assert [event["serial_num"] for event in events] == ["02"]


def test_collection_only_new_unclaimed_items_fire(monkeypatch):
    hass = CollectionFakeHass()
    coordinator = SmartdailyDataUpdateCoordinator(hass, "device", "community")
    existing = collection_item("01")
    claimed = collection_item("02", is_end="yes")
    unclaimed = collection_item("03", is_end=" NO ")

    run_collection_polls(
        monkeypatch,
        coordinator,
        collection_poll([existing]),
        collection_poll([existing, claimed, unclaimed]),
        collection_poll([existing, dict(claimed, is_end="no"), unclaimed]),
    )

    events = collection_events(hass)
    assert [event["serial_num"] for event in events] == ["03"]
    assert events[0]["is_end"] == " NO "


def test_normalize_collection_response_empty_and_malformed():
    assert normalize_collection_response({"Data": []}, "community") == (True, [])

    malformed_payloads = [None, {}, {"Data": None}, {"Data": "not-a-list"}]
    for payload in malformed_payloads:
        assert normalize_collection_response(payload, "community") == (False, [])

    valid, items = normalize_collection_response(
        {
            "Data": [
                None,
                "not-an-item",
                {"is_end": "no"},
                collection_item("04", is_end=" NO "),
            ]
        },
        "community",
    )
    assert valid is True
    assert len(items) == 1
    assert items[0]["collection_id"] == "account:04:2026/07/04 10:00"
    assert items[0]["is_end"] == " NO "


def test_normalize_collection_id_accepts_numeric_serial_and_empty_sdate():
    valid, items = normalize_collection_response(
        {"Data": [{"serial_num": 12345, "sdate": None, "is_end": "no"}]},
        "community",
    )

    assert valid is True
    assert items == [
        {
            "serial_num": 12345,
            "sdate": None,
            "is_end": "no",
            "collection_id": "account:12345:",
        }
    ]


def test_collection_identity_prefers_upstream_community():
    valid, items = normalize_collection_response(
        {
            "Data": [
                {
                    "serial_num": "88",
                    "sdate": "2026/07/20 10:00",
                    "is_end": "no",
                    "community": "Shared Community",
                }
            ]
        },
        "configured-community-id",
    )

    assert valid is True
    assert items[0]["collection_id"] == (
        "Shared Community:88:2026/07/20 10:00"
    )


def test_collection_config_entries_share_one_event_baseline(monkeypatch):
    hass = CollectionFakeHass()
    shared_state = {"known_ids": None}
    first = SmartdailyDataUpdateCoordinator(
        hass, "same-device", "community-a", collection_state=shared_state
    )
    second = SmartdailyDataUpdateCoordinator(
        hass, "same-device", "community-b", collection_state=shared_state
    )
    existing = collection_item("01", community="Shared Community")
    new_item = collection_item("02", community="Shared Community")

    run_collection_polls(monkeypatch, first, collection_poll([existing]))
    run_collection_polls(monkeypatch, second, collection_poll([existing]))
    run_collection_polls(
        monkeypatch, first, collection_poll([existing, new_item])
    )
    run_collection_polls(
        monkeypatch, second, collection_poll([existing, new_item])
    )

    events = collection_events(hass)
    assert [event["serial_num"] for event in events] == ["02"]
    assert first._known_collection_ids is second._known_collection_ids


def test_collection_http_failure_does_not_fail_package_fetch(monkeypatch):
    class FakeResponse:
        def __init__(self, status_code, payload=None):
            self.status_code = status_code
            self._payload = payload

        def json(self):
            return self._payload

    hass = CollectionFakeHass()
    coordinator = SmartdailyDataUpdateCoordinator(hass, "device", "community")
    monkeypatch.setattr(coordinator, "_update_token", lambda: None)

    def fake_get(url, headers, timeout=None):
        if "/Postal/getUserPostalList" in url:
            return FakeResponse(200, {"Data": []})
        assert timeout == 15
        return FakeResponse(503)

    monkeypatch.setattr(
        "custom_components.smartdaily_postal_ha.sensor.requests.get", fake_get
    )

    result = coordinator._fetch_data()

    assert result["all_packages"] == []
    assert result["collection_fetch_success"] is False
    assert result["collection_items"] == []
