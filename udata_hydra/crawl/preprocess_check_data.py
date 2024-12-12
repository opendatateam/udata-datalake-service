import json
from datetime import datetime, timezone

from asyncpg import Record

from udata_hydra.crawl.calculate_next_check import calculate_next_check_date
from udata_hydra.crawl.helpers import get_content_type_from_header, is_valid_status
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.utils import queue, send


async def preprocess_check_data(check_data: dict) -> tuple[dict, dict | None]:
    """Preprocess a check data.

    Insert a new check in the DB with the provided check data before analysis, and update the resource status and priority.
    If the check data has changed compared to the previous check, it will also send the check to udata, and provide a first estimated next_check date to the new check.

    Args:
        check_data: the check data to insert in the DB.

    Returns:
        The updated check data as it has just been inserted in the DB.
        The previous check data, or None if it's the first check.
    """

    check_data["resource_id"] = str(check_data["resource_id"])

    last_check: dict | None = None
    last_check_record: Record | None = await Check.get_by_resource_id(check_data["resource_id"])
    if last_check_record:
        last_check = dict(last_check_record)

    has_changed: bool = await has_check_changed(check_data, last_check)
    if has_changed:
        await send_check_to_udata(check_data)

    # Update resource following check:
    # Reset resource status so that it's not forbidden to be checked again.
    # Reset priority so that it's not prioritised anymore.
    await Resource.update(
        resource_id=check_data["resource_id"], data={"status": None, "priority": False}
    )

    check_data["next_check_at"] = calculate_next_check_date(has_changed, last_check, None)
    new_check: Record = await Check.insert(check_data)

    return dict(new_check), last_check


async def has_check_changed(check_data: dict, last_check_data: dict | None) -> bool:
    """Check if the check has changed compared to the last one"""

    is_first_check: bool = last_check_data is None
    status_has_changed = last_check_data and check_data.get("status") != last_check_data.get(
        "status"
    )
    status_no_longer_available = (
        last_check_data
        and is_valid_status(last_check_data.get("status"))
        and not is_valid_status(check_data.get("status"))
    )
    timeout_has_changed = last_check_data and check_data.get("timeout") != last_check_data.get(
        "timeout"
    )
    current_headers = check_data.get("headers", {})
    last_check_headers = (
        json.loads(last_check_data.get("headers"))
        if last_check_data and last_check_data.get("headers")
        else {}
    )
    content_has_changed = last_check_data and (
        current_headers.get("content-length") != last_check_headers.get("content-length")
        or current_headers.get("content-type") != last_check_headers.get("content-type")
    )

    # TODO: Instead of computing criterions here, store payload and compare with previous one.
    # It would make debugging easier.
    criterions = {
        "is_first_check": is_first_check,
        "status_has_changed": status_has_changed,
        "status_no_longer_available": status_no_longer_available,
        "timeout_has_changed": timeout_has_changed,
        "content_has_changed": content_has_changed,
    }

    return any(criterions.values())


async def send_check_to_udata(check_data: dict) -> None:
    """Enqueue the sending of the check to udata"""

    res = await Resource.get(resource_id=check_data["resource_id"], column_name="dataset_id")

    queue.enqueue(
        send,
        dataset_id=res["dataset_id"],
        resource_id=check_data["resource_id"],
        document={
            "check:available": is_valid_status(check_data.get("status")),
            "check:status": check_data.get("status"),
            "check:timeout": check_data["timeout"],
            "check:date": datetime.now(timezone.utc).isoformat(),
            "check:error": check_data.get("error"),
            "check:headers:content-type": await get_content_type_from_header(
                check_data.get("headers", {})
            ),
            "check:headers:content-length": int(
                check_data.get("headers", {}).get("content-length", 0)
            )
            or None,
        },
        _priority="high",
    )
