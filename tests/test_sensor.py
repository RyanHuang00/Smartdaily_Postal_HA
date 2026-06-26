import asyncio
import pytest
from custom_components.smartdaily_postal_ha.sensor import (
    PackageTrackerSensor,
    PackageSlotSensor,
    SmartdailyDataUpdateCoordinator,
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

    asyncio.get_event_loop().run_until_complete(coordinator._async_update_data())

    assert order == ["archive", "fire"]
    assert events[0][1]["pd_id"] == new_pid
    assert events[0][1]["photo_local_ready"] is True
