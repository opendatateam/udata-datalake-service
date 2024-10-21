from datetime import date

import aiohttp
from aiohttp import web
from asyncpg import Record
from pydantic import ValidationError

from udata_hydra import config, context
from udata_hydra.crawl.check_resources import check_resource
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.schemas import CheckGroupBy, CheckSchema
from udata_hydra.utils import get_request_params


async def get_latest_check(request: web.Request) -> web.Response:
    """Get the latest check for a given URL or resource_id"""
    url, resource_id = get_request_params(request, params_names=["url", "resource_id"])
    record: Record | None = await Check.get_latest(url, resource_id)
    if not record:
        raise web.HTTPNotFound()
    if record["deleted"]:
        raise web.HTTPGone()

    check = CheckSchema.model_validate(record)

    return web.json_response(text=check.model_dump_json())


async def get_all_checks(request: web.Request) -> web.Response:
    url, resource_id = get_request_params(request, params_names=["url", "resource_id"])
    records: list[Record] | None = await Check.get_all(url, resource_id)
    if not records:
        raise web.HTTPNotFound()

    return web.json_response([CheckSchema.model_validate(r) for r in records])


async def get_checks_aggregate(request: web.Request) -> web.Response:
    created_at: str = request.query.get("created_at")
    if not created_at:
        raise web.HTTPBadRequest(
            text="Missing mandatory 'created_at' param. You can use created_at=today to filter on today checks."
        )

    if created_at == "today":
        created_at_date: date = date.today()
    else:
        created_at_date: date = date.fromisoformat(created_at)

    column: str = request.query.get("group_by")
    if not column:
        raise web.HTTPBadRequest(text="Missing mandatory 'group_by' param.")
    records: list[Record] | None = await Check.get_group_by_for_date(column, created_at_date)
    if not records:
        raise web.HTTPNotFound()

    return web.json_response([CheckSchema.model_validate(r) for r in records])


async def create_check(request: web.Request) -> web.Response:
    """Create a new check"""

    # Get resource_id from request
    try:
        payload: dict = await request.json()
        resource_id: str = payload["resource_id"]
    except ValidationError as err:
        raise web.HTTPBadRequest(text=err.json())
    except KeyError as e:
        raise web.HTTPBadRequest(text=f"Missing key: {str(e)}")

    # Get URL from resource_id
    try:
        record: Record | None = await Resource.get(resource_id, "url")
        url: str = resource.url
    except Exception:
        raise web.HTTPNotFound(text=f"Couldn't find URL for resource {resource_id}")

    context.monitor().set_status(f'Crawling url "{url}"...')

    async with aiohttp.ClientSession(
        timeout=None, headers={"user-agent": config.USER_AGENT}
    ) as session:
        status: str = await check_resource(
            url=url, resource_id=resource_id, session=session, worker_priority="high"
        )
        context.monitor().refresh(status)

    record: Record | None = await Check.get_latest(url, resource_id)
    if not record:
        raise web.HTTPBadRequest(text=f"Check not created, status: {status}")

    check = CheckSchema.model_validate(record)

    return web.json_response(text=check.model_dump_json())
