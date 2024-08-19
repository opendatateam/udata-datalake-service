import json

from aiohttp import web
from marshmallow import ValidationError

from udata_hydra.db.resource import Resource
from udata_hydra.schemas import ResourceSchema


async def get_resource(request: web.Request) -> web.Response:
    """Endpoint to get a resource from the DB
    Respond with a 200 status code and a JSON body with the resource data
    If resource is not found, respond with a 404 status code
    """

    try:
        resource_id: str = request.match_info["resource_id"]
    except Exception as e:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(e)}))

    resource: dict = await Resource.get(resource_id)
    if not resource:
        raise web.HTTPNotFound()

    return web.json_response(ResourceSchema().dump(dict(resource)))


async def create_resource(request: web.Request) -> web.Response:
    """Endpoint to receive a resource creation event from a source
    Will create a new resource in the DB "catalog" table and mark it as priority for next crawling
    Respond with a 200 status code and a JSON body with a message key set to "created"
    If error, respond with a 400 status code
    """
    try:
        payload = await request.json()
        valid_payload: dict = ResourceSchema().load(payload)
    except ValidationError as err:
        raise web.HTTPBadRequest(text=json.dumps(err.messages))

    resource: dict = valid_payload["document"]
    if not resource:
        raise web.HTTPBadRequest(text="Missing document body")

    dataset_id = valid_payload["dataset_id"]
    resource_id = valid_payload["resource_id"]

    await Resource.insert(
        dataset_id=dataset_id,
        resource_id=resource_id,
        url=resource["url"],
        priority=True,
    )

    return web.json_response({"message": "created"})


async def update_resource(request: web.Request) -> web.Response:
    """Endpoint to receive a resource update event from a source
    Will update an existing resource in the DB "catalog" table and mark it as priority for next crawling
    Respond with a 200 status code and a JSON body with a message key set to "updated"
    If error, respond with a 400 status code
    """

    try:
        payload = await request.json()
        valid_payload: dict = ResourceSchema().load(payload)
    except ValidationError as err:
        raise web.HTTPBadRequest(text=json.dumps(err.messages))

    resource: dict = valid_payload["document"]
    if not resource:
        raise web.HTTPBadRequest(text="Missing document body")

    dataset_id: str = valid_payload["dataset_id"]
    resource_id = valid_payload["resource_id"]  # TODO: get resource_id from URL

    await Resource.update_or_insert(dataset_id, resource_id, resource["url"])

    return web.json_response({"message": "updated"})


async def delete_resource(request: web.Request) -> web.Response:
    try:
        resource_id: str = request.match_info["resource_id"]
    except Exception as e:
        raise web.HTTPBadRequest(text=json.dumps({"error": str(e)}))

    # Mark resource as deleted in catalog table
    await Resource.delete(resource_id)

    return web.json_response({"message": "deleted"})
