from datetime import datetime, timedelta, timezone

from udata_hydra import config


def calculate_next_check_date(
    has_check_changed: bool, last_check: dict | None, last_modified_at: datetime | None
) -> datetime:
    """Calculate the datetime of the next check, depending on the last check data.

    Args:
        has_check_changed: Whether the check has changed since the last check.
        last_check: The last check data as a dict, or None if there is no last check.
        last_modified_at: The last modification date of the ressource as analysed by the earliest change detection methods, or None if it could not be determined.

    Returns:
        The datetime of the next check.
    """

    now: datetime = datetime.now(timezone.utc)

    if not last_check or has_check_changed:
        # No last check or check itself has changed, next check will happen in the earliest delay without looking at the ressource last modification date
        next_check_at: datetime = now + timedelta(hours=config.CHECK_DELAYS[0])

    else:
        # Check has not changed since last check, we need to look at the ressource last modification date
        if last_modified_at:
            since_last_modif: timedelta = now - last_modified_at
        else:
            # If no last modification date, we use the last check date
            since_last_modif: timedelta = now - last_check["created_at"]

        if since_last_modif > timedelta(hours=config.CHECK_DELAYS[-1]):
            # 1) Last check or last ressource modification happened change after the maximum delay, next check will be after the same maximum delay
            next_check_at = now + timedelta(hours=config.CHECK_DELAYS[-1])
        else:
            # 2) Last check or last ressource modification happened before CHECK_DELAYS[i], next check will happen after CHECK_DELAYS[i]
            for delay in config.CHECK_DELAYS:
                if since_last_modif <= timedelta(hours=delay):
                    next_check_at = now + timedelta(hours=delay)
                    break

    return next_check_at
