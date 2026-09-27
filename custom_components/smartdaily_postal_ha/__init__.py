
DOMAIN = "smartdaily_postal_ha"


async def _async_get_outbox(hass):
    """Load the shared notification outbox and register its ack service once."""
    from .notification_outbox import (
        PackageNotificationOutbox,
        SERVICE_ACK_PACKAGE_NOTIFICATION,
        SERVICE_ACK_PICKUP_NOTIFICATION,
        SERVICE_ACK_COLLECTION_NOTIFICATION,
        SERVICE_ACK_BASELINE_RECONCILIATION,
        KIND_PICKUP,
        KIND_COLLECTION,
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
    if not hass.services.has_service(DOMAIN, SERVICE_ACK_PICKUP_NOTIFICATION):
        async def async_ack_pickup(call):
            await outbox.async_ack(str(call.data["pd_id"]), KIND_PICKUP)

        hass.services.async_register(DOMAIN, SERVICE_ACK_PICKUP_NOTIFICATION, async_ack_pickup)
    if not hass.services.has_service(DOMAIN, SERVICE_ACK_COLLECTION_NOTIFICATION):
        async def async_ack_collection(call):
            await outbox.async_ack(str(call.data["collection_id"]), KIND_COLLECTION)

        hass.services.async_register(DOMAIN, SERVICE_ACK_COLLECTION_NOTIFICATION, async_ack_collection)
    if not hass.services.has_service(DOMAIN, SERVICE_ACK_BASELINE_RECONCILIATION):
        async def async_ack_baseline(call):
            await outbox.async_ack_baseline(
                str(call.data["kind"]), str(call.data["item_id"])
            )

        hass.services.async_register(
            DOMAIN, SERVICE_ACK_BASELINE_RECONCILIATION, async_ack_baseline
        )
    return outbox

async def async_setup_entry(hass, config_entry):
    """Set up smartdaily_postal_ha from a config entry."""
    await _async_get_outbox(hass)
    # Forward the setup to the sensor and camera platforms
    await hass.config_entries.async_forward_entry_setups(config_entry, ["sensor", "camera"])
    return True
