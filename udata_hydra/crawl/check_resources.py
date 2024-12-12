import asyncio
import logging
import time
from collections import defaultdict
from urllib.parse import urlparse

import aiohttp
from asyncpg import Record

from udata_hydra import config, context
from udata_hydra.crawl.helpers import (
    convert_headers,
    fix_surrogates,
    has_nice_head,
    is_domain_backoff,
)
from udata_hydra.crawl.preprocess_check_data import preprocess_check_data
from udata_hydra.db.resource import Resource
from udata_hydra.utils import queue

RESOURCE_RESPONSE_STATUSES = {
    "OK": "ok",
    "TIMEOUT": "timeout",
    "ERROR": "error",
    "BACKOFF": "backoff",
}

log = logging.getLogger("udata-hydra")


results: defaultdict = defaultdict(int)


async def check_batch_resources(to_parse: list[Record]) -> None:
    """Check a batch of resources"""
    context.monitor().set_status("Checking resources...")
    tasks: list = []
    async with aiohttp.ClientSession(
        timeout=None, headers={"user-agent": config.USER_AGENT}
    ) as session:
        for row in to_parse:
            tasks.append(
                check_resource(
                    url=row["url"],
                    resource_id=row["resource_id"],
                    session=session,
                    worker_priority="low",
                )
            )
        for task in asyncio.as_completed(tasks):
            result = await task
            results[result] += 1
            context.monitor().refresh(results)


async def check_resource(
    url: str,
    resource_id: str,
    session,
    sleep: float = 0,
    method: str = "head",
    worker_priority: str = "default",
    force_analysis: bool = False,
) -> str:
    log.debug(f"check {url}, sleep {sleep}, method {method}")

    # Import here to avoid circular import issues
    from udata_hydra.analysis.resource import analyse_resource

    # Update resource status to CRAWLING_URL
    await Resource.update(resource_id, data={"status": "CRAWLING_URL"})

    if sleep:
        await asyncio.sleep(sleep)

    url_parsed = urlparse(url)
    domain = url_parsed.netloc

    if not domain:
        log.warning(f"[warning] not netloc in url, skipping {url}")
        # Process the check data. If it has changed, it will be sent to udata
        await preprocess_check_data(
            {
                "resource_id": resource_id,
                "url": url,
                "error": "Not netloc in url",
                "timeout": False,
            }
        )
        return RESOURCE_RESPONSE_STATUSES["ERROR"]

    should_backoff, reason = await is_domain_backoff(domain)
    if should_backoff:
        log.info(f"backoff {domain} ({reason})")
        # skip this URL, it will come back in a next batch
        await Resource.update(resource_id, data={"status": "BACKOFF", "priority": False})
        return RESOURCE_RESPONSE_STATUSES["BACKOFF"]

    try:
        start = time.time()
        timeout = aiohttp.ClientTimeout(total=5)
        _method = getattr(session, method)
        async with _method(url, timeout=timeout, allow_redirects=True) as resp:
            end = time.time()
            if method != "get" and not has_nice_head(resp):
                return await check_resource(
                    url,
                    resource_id,
                    session,
                    force_analysis=force_analysis,
                    method="get",
                    worker_priority=worker_priority,
                )
            resp.raise_for_status()

            # Preprocess the check data. If it has changed, it will be sent to udata
            new_check, last_check = await preprocess_check_data(
                {
                    "resource_id": resource_id,
                    "url": url,
                    "domain": domain,
                    "status": resp.status,
                    "headers": convert_headers(resp.headers),
                    "timeout": False,
                    "response_time": end - start,
                },
            )

            # Update resource status to TO_ANALYSE_RESOURCE
            await Resource.update(resource_id, data={"status": "TO_ANALYSE_RESOURCE"})

            # Enqueue the resource for analysis
            queue.enqueue(
                analyse_resource,
                new_check["id"],
                last_check,
                force_analysis,
                _priority=worker_priority,
            )

            return RESOURCE_RESPONSE_STATUSES["OK"]

    except asyncio.exceptions.TimeoutError:
        # Process the check data. If it has changed, it will be sent to udata
        await preprocess_check_data(
            {
                "resource_id": resource_id,
                "url": url,
                "domain": domain,
                "timeout": True,
            }
        )

        # Reset resource status so that it's not forbidden to be checked again
        await Resource.update(resource_id=resource_id, data={"status": None})

        return RESOURCE_RESPONSE_STATUSES["TIMEOUT"]

    # TODO: debug AssertionError, should be caught in DB now
    # File "[...]aiohttp/connector.py", line 991, in _create_direct_connection
    # assert port is not None
    # UnicodeError: encoding with 'idna' codec failed (UnicodeError: label too long)
    # eg http://%20Localisation%20des%20acc%C3%A8s%20des%20offices%20de%20tourisme
    except (
        aiohttp.client_exceptions.ClientError,
        AssertionError,
        UnicodeError,
    ) as e:
        # if we get a 404, it might be that the resource's URL has changed since last catalog load
        # we compare the actual URL to the one we have here to handle these cases
        if getattr(e, "status", None) == 404 and config.UDATA_URI:
            handled = await handle_wrong_resource_url(
                resource_id=resource_id,
                session=session,
                url=url,
                force_analysis=force_analysis,
                worker_priority=worker_priority,
            )
            if handled is not None:
                return handled

        error = getattr(e, "message", None) or str(e)
        # Process the check data. If it has changed, it will be sent to udata
        await preprocess_check_data(
            {
                "resource_id": resource_id,
                "url": url,
                "domain": domain,
                "timeout": False,
                "error": fix_surrogates(error),
                "headers": convert_headers(getattr(e, "headers", {})),
                "status": getattr(e, "status", None),
            }
        )

        log.warning(f"Crawling error for url {url}", exc_info=e)

        # Reset resource status so that it's not forbidden to be checked again
        await Resource.update(resource_id=resource_id, data={"status": None})

        return RESOURCE_RESPONSE_STATUSES["ERROR"]


async def handle_wrong_resource_url(
    resource_id: str,
    session,
    url: str,
    force_analysis: bool,
    worker_priority: str,
):
    stable_resource_url = f"{config.UDATA_URI.replace('api/2', 'fr')}/datasets/r/{resource_id}"
    async with session.head(stable_resource_url) as resp:
        resp.raise_for_status()
        actual_url = resp.headers.get("location")
    if actual_url and url != actual_url:
        await Resource.update(resource_id=resource_id, data={"url": actual_url})
        return await check_resource(
            actual_url,
            resource_id,
            session,
            force_analysis=force_analysis,
            method="head",
            worker_priority=worker_priority,
        )
    return
