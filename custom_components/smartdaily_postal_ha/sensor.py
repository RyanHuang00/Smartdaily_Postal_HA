"""Sensor platform for the package tracker component."""

import asyncio
from datetime import datetime, timedelta
import re
import aiohttp
import requests
import pytz
from homeassistant.core import _LOGGER
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.const import CONF_API_KEY
from homeassistant.helpers.entity import Entity
from homeassistant.util import Throttle
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
    UpdateFailed,
)

from .photo_archive import archive_photos

DOMAIN = "smartdaily_postal_ha"
SCAN_INTERVAL = timedelta(minutes=5)
MIN_TIME_BETWEEN_UPDATES = timedelta(hours=12)
MAX_PACKAGE_SLOTS = 4  # 最多顯示 4 個包裹 slot
HISTORY_LIMIT = 30  # PackageHistorySensor attribute cap (~16KB attribute soft limit)
EVENT_NEW_PACKAGE = f"{DOMAIN}_new_package"
EVENT_PACKAGE_PICKED_UP = f"{DOMAIN}_package_picked_up"
EVENT_NEW_COLLECTION = f"{DOMAIN}_new_collection"
STATUS_PICKED_UP = 2  # observed mapping: 1 = 未領取, 2 = 已取件
COLLECTION_URL = "https://api.smartdaily.com.tw/api/Collection/getCollectionPayment"


def _collection_scalar(value):
    """Return a stable string for a scalar collection field."""
    if value is None or isinstance(value, (dict, list, tuple, set)):
        return ""
    return str(value).strip()


def _collection_id(item, _configured_com_id=None):
    """Build an ID that is stable across config entries for the same account."""
    serial_num = _collection_scalar(item.get("serial_num"))
    if not serial_num:
        return None

    # getCollectionPayment is account-scoped and does not accept com_id. The
    # same response can therefore be seen by multiple community config entries.
    # Prefer the response's own community label and use an account-wide fallback
    # so one record has the same identity in every coordinator.
    scope = _collection_scalar(item.get("community")) or "account"
    # serial_num appears to be the API's primary identifier. Including the
    # storage date prevents a reused serial number in another delivery from
    # being mistaken for an item already seen during this HA session.
    sdate = _collection_scalar(item.get("sdate"))
    return f"{scope}:{serial_num}:{sdate}"


def _is_uncollected(item):
    """Interpret is_end defensively without altering the original API field."""
    return _collection_scalar(item.get("is_end")).lower() == "no"


def normalize_collection_response(payload, com_id):
    """Validate and normalize a getCollectionPayment response.

    Return ``(True, items)`` for a structurally valid response, including an
    empty Data list. Return ``(False, [])`` for malformed payloads so callers
    can distinguish an actual empty result from a failed poll and preserve the
    notification baseline.
    """
    if not isinstance(payload, dict) or not isinstance(payload.get("Data"), list):
        return False, []

    normalized_by_id = {}
    for raw_item in payload["Data"]:
        if not isinstance(raw_item, dict):
            continue

        collection_id = _collection_id(raw_item, com_id)
        if collection_id is None:
            # Without serial_num there is no safe way to deduplicate events.
            _LOGGER.warning("Ignoring collection item without serial_num")
            continue

        # Keep every original API field/value intact in the future event
        # payload; normalization is used only for identity and comparisons.
        item = dict(raw_item)
        item["collection_id"] = collection_id
        # Deduplicate malformed API responses while retaining API order.
        normalized_by_id[collection_id] = item

    return True, list(normalized_by_id.values())


def parse_time(time_str):
    """Parse time string to standard format."""
    if "剛剛" in time_str:
        now = datetime.now(pytz.timezone("Asia/Taipei"))
        now_at_hour = now.replace(minute=0, second=0, microsecond=0)
        return now_at_hour.strftime("%Y/%m/%d %H:%M")
    elif "昨天" in time_str:
        time_part = time_str.split(" ")[1]
        try:
            yesterday_time = datetime.strptime(time_part, "%H:%M")
            yesterday = datetime.now(pytz.timezone("Asia/Taipei")) - timedelta(days=1)
            combined_datetime = yesterday.replace(
                hour=yesterday_time.hour, minute=yesterday_time.minute
            )
            return combined_datetime.strftime("%Y/%m/%d %H:%M")
        except ValueError:
            return None
    else:
        match = re.match(r"(\d+)小時以前", time_str)
        if match:
            hours_ago = int(match.group(1))
            now = datetime.now(pytz.timezone("Asia/Taipei"))
            now_at_hour = now.replace(minute=0, second=0, microsecond=0)
            estimated_time = now_at_hour - timedelta(hours=hours_ago)
            return estimated_time.strftime("%Y/%m/%d %H:%M")
        else:
            match = re.match(r"(\d+)分鐘以前", time_str)
            if match:
                minutes_ago = int(match.group(1))
                now = datetime.now(pytz.timezone("Asia/Taipei"))
                estimated_time = now - timedelta(minutes=minutes_ago)
                return estimated_time.strftime("%Y/%m/%d %H:%M")
            else:
                try:
                    utc_time = datetime.strptime(time_str, "%Y/%m/%d %H:%M")
                    return (
                        pytz.utc.localize(utc_time)
                        .astimezone(pytz.timezone("Asia/Taipei"))
                        .strftime("%Y/%m/%d %H:%M")
                    )
                except ValueError:
                    return None


class SmartdailyDataUpdateCoordinator(DataUpdateCoordinator):
    """Class to manage fetching Smartdaily data."""

    def __init__(self, hass, device_id, com_id, collection_state=None):
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name="Smartdaily Postal",
            update_interval=SCAN_INTERVAL,
        )
        self._device_id = device_id
        self._com_id = com_id
        self._kingnet_auth = ""
        # Maps pd_id -> p_status from the previous poll. None on first run so the
        # integration doesn't spam events for the entire backlog on startup; also
        # used to detect status transitions (未領取 1 -> 已取件 2).
        self._previous_pd_status = None
        # Config entries for the same DeviceID share this session-scoped state.
        # The Collection endpoint is account-scoped, so sharing prevents the
        # same row from firing once per selected community. IDs only ever grow;
        # None means no successful baseline yet.
        self._collection_state = (
            collection_state if collection_state is not None else {}
        )
        self._collection_state.setdefault("known_ids", None)

    @property
    def _known_collection_ids(self):
        """Return shared collection IDs while preserving the existing interface."""
        return self._collection_state["known_ids"]

    @_known_collection_ids.setter
    def _known_collection_ids(self, value):
        self._collection_state["known_ids"] = value

    def _update_token(self):
        """Update the KingnetAuth token."""
        headers_update_token = {
            "Connection": "keep-alive",
            "Accept": "application/json, text/plain, */*"
        }
        response = requests.get(
            "https://api.smartdaily.com.tw/api/Valid/getHashCodeV2?code="
            + self._device_id,
            headers=headers_update_token,
        )
        if response.status_code == 200:
            data = response.json()
            self._kingnet_auth = "CommunityUser " + data["Data"]["token"]
        else:
            _LOGGER.error("Token update failed, status code: %s", response.status_code)

    async def _async_update_data(self):
        """Fetch data from API, then run async post-processing."""
        try:
            result = await self.hass.async_add_executor_job(self._fetch_data)
        except Exception as err:
            raise UpdateFailed(f"Error fetching data: {err}")

        all_packages = result.get("all_packages", [])

        # Build current pd_id -> p_status mapping.
        curr_status = {
            p["package"].get("pd_id"): p["package"].get("p_status")
            for p in all_packages
            if p["package"].get("pd_id")
        }

        # Skip event firing on the first run (self._previous_pd_status is None) so
        # the integration doesn't spam the entire backlog on startup. After the
        # first poll we have a baseline and can detect deltas safely.
        if self._previous_pd_status is not None:
            prev_ids = set(self._previous_pd_status.keys())
            curr_ids = set(curr_status.keys())
            new_ids = curr_ids - prev_ids

            unclaimed_count = result.get("unclaimed_count", 0)
            archived_new_ids = set()

            if new_ids:
                new_package_entries = [
                    entry
                    for entry in all_packages
                    if (entry.get("package") or {}).get("pd_id") in new_ids
                ]
                # LINE fetches Flex hero images immediately after receiving the
                # message. Archive new package photos before firing the event so
                # the photo proxy does not return a transient 404.
                archived_new_ids = await archive_photos(self.hass, new_package_entries)

            # New packages: pd_id appears for the first time.
            for pid in new_ids:
                pkg = next(
                    (p["package"] for p in all_packages if p["package"].get("pd_id") == pid),
                    None,
                )
                if pkg:
                    _LOGGER.info("New package detected: %s", pid)
                    event_data = dict(pkg)
                    event_data["unclaimed_count"] = unclaimed_count
                    event_data["photo_local_ready"] = pid in archived_new_ids
                    self.hass.bus.async_fire(EVENT_NEW_PACKAGE, event_data)

            # Pickup transitions: same pd_id, p_status changed to 已取件 (2).
            for pid in curr_ids & prev_ids:
                old_status = self._previous_pd_status[pid]
                new_status = curr_status[pid]
                if old_status != new_status and new_status == STATUS_PICKED_UP:
                    pkg = next(
                        (p["package"] for p in all_packages if p["package"].get("pd_id") == pid),
                        None,
                    )
                    if pkg:
                        _LOGGER.info(
                            "Package picked up: %s (p_status %s -> %s)", pid, old_status, new_status
                        )
                        event_data = dict(pkg)
                        event_data["previous_status"] = old_status
                        event_data["new_status"] = new_status
                        event_data["unclaimed_count"] = unclaimed_count
                        self.hass.bus.async_fire(EVENT_PACKAGE_PICKED_UP, event_data)

        self._previous_pd_status = curr_status

        # Collection is a best-effort companion request. A failed/malformed
        # response must neither establish nor change the event baseline.
        if result.get("collection_fetch_success"):
            collection_items = result.get("collection_items", [])
            current_collection_ids = {
                item.get("collection_id")
                for item in collection_items
                if item.get("collection_id")
            }

            if self._known_collection_ids is None:
                # First successful poll is baseline-only to avoid replaying the
                # user's existing uncollected items after HA starts.
                self._known_collection_ids = set(current_collection_ids)
            else:
                new_collection_ids = current_collection_ids - self._known_collection_ids
                uncollected_count = result.get("collection_uncollected_count", 0)

                for item in collection_items:
                    collection_id = item.get("collection_id")
                    if (
                        collection_id in new_collection_ids
                        and _is_uncollected(item)
                    ):
                        _LOGGER.info("New uncollected item detected: %s", collection_id)
                        event_data = dict(item)
                        event_data["uncollected_count"] = uncollected_count
                        self.hass.bus.async_fire(EVENT_NEW_COLLECTION, event_data)

                # Keep all seen IDs, including claimed items, so later API
                # changes cannot make an old record look newly delivered.
                self._known_collection_ids.update(current_collection_ids)

        # Best-effort photo archive. Don't block the update on it; if it's slow
        # or fails, the sensor still returns fresh data.
        self.hass.async_create_task(archive_photos(self.hass, all_packages))

        return result

    def _fetch_collections(self, headers):
        """Fetch collection items without allowing this optional API to fail a poll."""
        try:
            response = requests.get(COLLECTION_URL, headers=headers, timeout=15)
            if response.status_code != 200:
                _LOGGER.warning(
                    "Collection API request failed, status code: %s",
                    response.status_code,
                )
                return False, []

            valid, items = normalize_collection_response(response.json(), self._com_id)
            if not valid:
                _LOGGER.warning("Collection API returned a malformed payload")
            return valid, items
        except Exception as err:  # Collection data is deliberately best-effort.
            _LOGGER.warning("Collection API request failed: %s", err)
            return False, []

    def _fetch_data(self):
        """Fetch data from API (blocking)."""
        self._update_token()

        url = f"https://api.smartdaily.com.tw/api/Postal/getUserPostalList?com_id={self._com_id}"
        headers = {
            "Connection": "keep-alive",
            "KingnetAuth": self._kingnet_auth,
            "Accept": "application/json, text/plain, */*"
        }

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise UpdateFailed(f"API request failed: {response.status_code}")

        data = response.json()

        collection_fetch_success, collection_items = self._fetch_collections(headers)

        # Process packages
        latest_package = None
        latest_time = None
        unclaimed_packages = []
        all_packages = []

        for package in data.get("Data", []):
            package_time_str = package.get("create_date", "")
            package_time = parse_time(package_time_str)

            # 收集未領取包裹
            if package.get("p_status") == 1:
                unclaimed_packages.append({
                    "package": package,
                    "parsed_time": package_time
                })

            all_packages.append({
                "package": package,
                "parsed_time": package_time
            })

            if package_time is None:
                continue

            if latest_time is None or package_time > latest_time:
                latest_time = package_time
                latest_package = package

        # 按時間排序未領取包裹（最新的在前）
        unclaimed_packages.sort(
            key=lambda x: x["parsed_time"] if x["parsed_time"] else "",
            reverse=True
        )

        return {
            "latest_package": latest_package,
            "unclaimed_packages": unclaimed_packages,
            "unclaimed_count": len(unclaimed_packages),
            "all_packages": all_packages,
            "collection_fetch_success": collection_fetch_success,
            "collection_items": collection_items,
            "collection_uncollected_count": sum(
                _is_uncollected(item) for item in collection_items
            ),
        }


async def async_setup_entry(hass, config_entry, async_add_entities):
    """Set up the sensor based on a config entry."""
    device_id = config_entry.data.get("DeviceID")
    com_id = config_entry.data.get("com_id")

    # Initialize domain data if not exists
    if DOMAIN not in hass.data:
        hass.data[DOMAIN] = {}

    # getCollectionPayment is scoped by DeviceID rather than com_id. Reuse one
    # event baseline for every community entry belonging to that DeviceID.
    collection_states = hass.data[DOMAIN].setdefault("_collection_states", {})
    collection_state = collection_states.setdefault(
        _collection_scalar(device_id) or "account",
        {"known_ids": None},
    )

    # Create coordinator
    coordinator = SmartdailyDataUpdateCoordinator(
        hass,
        device_id,
        com_id,
        collection_state=collection_state,
    )

    # Fetch initial data
    await coordinator.async_config_entry_first_refresh()

    # Store coordinator
    hass.data[DOMAIN][config_entry.entry_id] = coordinator

    # Create entities
    entities = [
        PackageTrackerSensor(coordinator, device_id, com_id),
        PackageHistorySensor(coordinator, device_id, com_id),
    ]

    # Add package slot sensors (1-4)
    for slot in range(1, MAX_PACKAGE_SLOTS + 1):
        entities.append(PackageSlotSensor(coordinator, device_id, com_id, slot))

    async_add_entities(entities, True)


class PackageTrackerSensor(CoordinatorEntity, Entity):
    """Representation of a Package Tracker Sensor (main sensor showing latest package)."""

    def __init__(self, coordinator, device_id, com_id):
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._device_id = device_id
        self._com_id = com_id
        self._unique_id = f"{device_id}_{com_id}"
        self._name = "My Package Tracker"
        self._attr_has_entity_name = False

    @property
    def unique_id(self):
        """Return a unique ID."""
        return self._unique_id

    @property
    def device_info(self):
        """Return information about the device."""
        return {
            "identifiers": {(DOMAIN, self._unique_id)},
            "name": "智生活包裹追蹤",
            "manufacturer": "今網智生活",
        }

    @property
    def icon(self):
        """Return the icon of the sensor based on its state."""
        if self.state is None:
            return "mdi:package-variant-remove"
        elif self.state == "未領取":
            return "mdi:package-variant-closed"
        elif self.state == "已取件":
            return "mdi:package-variant-closed-check"
        else:
            return "mdi:package-variant"

    @property
    def name(self):
        """Return the name of the sensor."""
        return self._name

    @property
    def state(self):
        """Return the state of the sensor."""
        if self.coordinator.data is None:
            return None

        latest_package = self.coordinator.data.get("latest_package")
        if latest_package is None:
            return None

        return "已取件" if latest_package.get("p_status") == 2 else "未領取"

    @property
    def extra_state_attributes(self):
        """Return other attributes of the sensor."""
        if self.coordinator.data is None:
            return {}

        latest_package = self.coordinator.data.get("latest_package")
        if latest_package is None:
            return {}

        # Process postal_img (保留完整 URL，包含 Google Cloud Storage 簽名參數)
        postal_img_url = latest_package.get("postal_img", "")
        if not postal_img_url:
            postal_img_url = "Unavailable"

        # Update global image URL for camera
        if DOMAIN in self.hass.data:
            if postal_img_url == "Unavailable":
                self.hass.data[DOMAIN]["parcel_image_url"] = "https://img.smartdaily.com.tw/wordpress/smartdaily/homepage/LOGO.png"
            else:
                self.hass.data[DOMAIN]["parcel_image_url"] = postal_img_url

        return {
            "pd_id": latest_package.get("pd_id"),
            "create_date": parse_time(latest_package.get("create_date", "")),
            "p_name": latest_package.get("p_name"),
            "postal_typeText": latest_package.get("postal_typeText"),
            "transport_code": latest_package.get("transport_code"),
            "privacy": latest_package.get("privacy") == "privacy",
            "p_note": latest_package.get("p_note"),
            "postal_logisticsText": latest_package.get("postal_logisticsText", "Unavailable"),
            "postal_img": postal_img_url,
            "unclaimed_packages_count": self.coordinator.data.get("unclaimed_count", 0)
        }

    # Keep parse_time as instance method for backward compatibility with tests
    def parse_time(self, time_str):
        """Parse time string (wrapper for module function)."""
        return parse_time(time_str)


class PackageSlotSensor(CoordinatorEntity, Entity):
    """Representation of a Package Slot Sensor (showing individual unclaimed package)."""

    def __init__(self, coordinator, device_id, com_id, slot):
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._device_id = device_id
        self._com_id = com_id
        self._slot = slot
        self._unique_id = f"{device_id}_{com_id}_slot_{slot}"
        self._name = f"包裹 {slot}"
        self._attr_has_entity_name = False

    @property
    def unique_id(self):
        """Return a unique ID."""
        return self._unique_id

    @property
    def device_info(self):
        """Return information about the device."""
        return {
            "identifiers": {(DOMAIN, f"{self._device_id}_{self._com_id}")},
            "name": "智生活包裹追蹤",
            "manufacturer": "今網智生活",
        }

    @property
    def icon(self):
        """Return the icon of the sensor based on its state."""
        package = self._get_package()
        if package is None:
            return "mdi:package-variant-remove"
        return "mdi:package-variant-closed"

    @property
    def name(self):
        """Return the name of the sensor."""
        return self._name

    def _get_package(self):
        """Get the package for this slot."""
        if self.coordinator.data is None:
            return None

        unclaimed = self.coordinator.data.get("unclaimed_packages", [])
        slot_index = self._slot - 1  # slot 1 = index 0

        if slot_index < len(unclaimed):
            return unclaimed[slot_index].get("package")
        return None

    @property
    def state(self):
        """Return the state of the sensor."""
        package = self._get_package()
        if package is None:
            return "無包裹"
        return package.get("p_name", "未知")

    @property
    def extra_state_attributes(self):
        """Return other attributes of the sensor."""
        package = self._get_package()
        if package is None:
            return {
                "slot": self._slot,
                "has_package": False
            }

        # Process postal_img (保留完整 URL，包含 Google Cloud Storage 簽名參數)
        postal_img_url = package.get("postal_img", "")
        if not postal_img_url:
            postal_img_url = "Unavailable"

        return {
            "slot": self._slot,
            "has_package": True,
            "pd_id": package.get("pd_id"),
            "create_date": parse_time(package.get("create_date", "")),
            "p_name": package.get("p_name"),
            "postal_typeText": package.get("postal_typeText"),
            "transport_code": package.get("transport_code"),
            "privacy": package.get("privacy") == "privacy",
            "p_note": package.get("p_note"),
            "postal_logisticsText": package.get("postal_logisticsText", "Unavailable"),
            "postal_img": postal_img_url,
        }

    @property
    def available(self):
        """Return True if entity is available."""
        return self.coordinator.last_update_success


class PackageHistorySensor(CoordinatorEntity, Entity):
    """Exposes the full package history (claimed + unclaimed) as a single sensor.

    Adds what the upstream integration already collects but never surfaces:
    coordinator.data["all_packages"]. Each entry is enriched with a local photo
    path under /local/packages/<pd_id>.jpg so Lovelace can render thumbnails
    that survive the upstream URL expiry.
    """

    def __init__(self, coordinator, device_id, com_id):
        super().__init__(coordinator)
        self._device_id = device_id
        self._com_id = com_id
        self._unique_id = f"{device_id}_{com_id}_history"
        self._name = "包裹歷史"
        self._attr_has_entity_name = False

    @property
    def unique_id(self):
        return self._unique_id

    @property
    def name(self):
        return self._name

    @property
    def icon(self):
        return "mdi:package-variant"

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, f"{self._device_id}_{self._com_id}")},
            "name": "智生活包裹追蹤",
            "manufacturer": "今網智生活",
        }

    @property
    def state(self):
        if self.coordinator.data is None:
            return 0
        return len(self.coordinator.data.get("all_packages", []))

    @property
    def extra_state_attributes(self):
        if self.coordinator.data is None:
            return {"packages": [], "total_count": 0}

        all_packages = self.coordinator.data.get("all_packages", [])
        sorted_packages = sorted(
            all_packages,
            key=lambda x: x["parsed_time"] if x["parsed_time"] else "",
            reverse=True,
        )
        recent = sorted_packages[:HISTORY_LIMIT]
        return {
            "packages": [self._enrich(entry) for entry in recent],
            "total_count": len(all_packages),
            "limit": HISTORY_LIMIT,
        }

    @staticmethod
    def _enrich(entry):
        pkg = entry.get("package", {}) or {}
        parsed = entry.get("parsed_time")
        pd_id = pkg.get("pd_id")
        return {
            "pd_id": pd_id,
            "create_date": parsed or pkg.get("create_date"),
            "p_name": pkg.get("p_name"),
            "logistics": pkg.get("postal_logisticsText"),
            "transport_code": pkg.get("transport_code"),
            "type": pkg.get("postal_typeText"),
            "note": pkg.get("p_note"),
            "status": "已取件" if pkg.get("p_status") == 2 else "未領取",
            "photo_local": f"/local/packages/{pd_id}.jpg" if pd_id else None,
            "photo_remote": pkg.get("postal_img"),
        }

    @property
    def available(self):
        return self.coordinator.last_update_success
