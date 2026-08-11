
DOMAIN = "smartdaily_postal_ha"


async def _async_get_outbox(hass):
    """Load the shared notification outbox and register its ack service once."""
    from .notification_outbox import (
        PackageNotificationOutbox,
        SERVICE_ACK_PACKAGE_NOTIFICATION,
    )

    domain_data = hass.data.setdefault(DOMAIN, {})
    outbox = domain_data.get("package_notification_outbox")
    if outbox is None:
        outbox = PackageNotificationOutbox(hass)
        await outbox.async_load()
        domain_data["package_notification_outbox"] = outbox

    if not hass.services.has_service(DOMAIN, SERVICE_ACK_PACKAGE_NOTIFICATION):
        async def async_ack(call):
            await outbox.async_ack(str(call.data["pd_id"]))

        hass.services.async_register(
            DOMAIN,
            SERVICE_ACK_PACKAGE_NOTIFICATION,
            async_ack,
        )
    return outbox

async def async_setup_entry(hass, config_entry):
    """Set up smartdaily_postal_ha from a config entry."""
    await _async_get_outbox(hass)
    # Forward the setup to the sensor and camera platforms
    await hass.config_entries.async_forward_entry_setups(config_entry, ["sensor", "camera"])
    return True
