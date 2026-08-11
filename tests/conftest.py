"""Small Home Assistant stubs for unit tests.

The real Home Assistant package is not needed for these focused tests, and
installing a compatible HA build into this old Python 3.7 environment is brittle.
"""

import logging
import sys
import types


def _install_module(name):
    module = types.ModuleType(name)
    sys.modules[name] = module
    return module


homeassistant = _install_module("homeassistant")
core = _install_module("homeassistant.core")
core._LOGGER = logging.getLogger("homeassistant")

const = _install_module("homeassistant.const")
const.CONF_API_KEY = "api_key"

helpers = _install_module("homeassistant.helpers")
event = _install_module("homeassistant.helpers.event")
event.async_track_time_interval = lambda *args, **kwargs: None

storage = _install_module("homeassistant.helpers.storage")


class Store:
    """In-memory stand-in with persistence across Store instances."""

    data = {}

    def __init__(self, hass, version, key):
        self.key = key

    async def async_load(self):
        return self.data.get(self.key)

    async def async_save(self, value):
        self.data[self.key] = value


storage.Store = Store

entity = _install_module("homeassistant.helpers.entity")


class Entity:
    pass


entity.Entity = Entity

util = _install_module("homeassistant.util")
util.Throttle = lambda *args, **kwargs: (lambda func: func)

update_coordinator = _install_module("homeassistant.helpers.update_coordinator")


class UpdateFailed(Exception):
    pass


class DataUpdateCoordinator:
    def __init__(self, hass, logger, name=None, update_interval=None):
        self.hass = hass
        self.logger = logger
        self.name = name
        self.update_interval = update_interval
        self.data = None
        self.last_update_success = True


class CoordinatorEntity:
    def __init__(self, coordinator):
        self.coordinator = coordinator
        self.hass = getattr(coordinator, "hass", None)


update_coordinator.CoordinatorEntity = CoordinatorEntity
update_coordinator.DataUpdateCoordinator = DataUpdateCoordinator
update_coordinator.UpdateFailed = UpdateFailed
