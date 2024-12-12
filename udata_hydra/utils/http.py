import json
import logging
from urllib.parse import urlparse

import aiohttp
from aiohttp import web

from udata_hydra import config
from udata_hydra.utils import IOException

log = logging.getLogger("udata-hydra")


def is_valid_uri(uri: str) -> bool:
    try:
        result = urlparse(uri)
        return all([result.scheme, result.netloc])
    except AttributeError:
        return False


def get_request_params(request, params_names: list[str]) -> list:
    """Get GET parameters from request"""
    data = [request.query.get(p) for p in params_names]
    if not any(data):
        raise web.HTTPBadRequest()
    return data


async def send(dataset_id: str, resource_id: str, document: dict) -> None:
    log.debug(
        f"Sending payload to udata {dataset_id}/{resource_id}: {json.dumps(document, indent=4)}"
    )

    if not config.WEBHOOK_ENABLED:
        log.debug("Webhook disabled, skipping send")
        return

    if not config.UDATA_URI or not config.UDATA_URI_API_KEY:
        log.error("UDATA_* config is not set, not sending callbacks")
        return

    uri = f"{config.UDATA_URI}/datasets/{dataset_id}/resources/{resource_id}/extras/"
    headers = {
        "content-type": "application/json",
        "X-API-KEY": config.UDATA_URI_API_KEY,
    }

    async with aiohttp.ClientSession() as session:
        async with session.put(uri, json=document, headers=headers) as resp:
            # we're raising since we should be in a worker thread
            if resp.status == 404:
                pass
            elif resp.status == 410:
                raise IOException(
                    "Resource has been deleted on udata", resource_id=resource_id, url=uri
                )
            if resp.status == 502:
                raise IOException("Udata is unreachable", resource_id=resource_id, url=uri)
            else:
                resp.raise_for_status()
