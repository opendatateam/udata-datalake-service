from datetime import datetime, timedelta, timezone
from typing import Any

from multidict import CIMultiDictProxy

from udata_hydra import config, context


async def get_content_type_from_header(headers: dict) -> str:
    """
    Parse content-type header to retrieve only the mime type
    """
    content_type = headers.get("content-type")
    if not content_type or ";" not in content_type:
        return content_type
    try:
        content_type, _ = content_type.split(";")
    except ValueError:
        # Weird e.g.: text/html;h5ai=0.20;charset=UTF-8
        content_type, _, _ = content_type.split(";")
    return content_type


def convert_headers(headers: CIMultiDictProxy) -> dict:
    """Convert headers from aiohttp CIMultiDict type to dict type.

    :warning: this will only take the first value for a given header key but multidict is not json serializable
    """
    if not headers:
        return {}
    _headers = {}
    for k in headers.keys():
        value = fix_surrogates(headers[k])
        _headers[k.lower()] = value
    return _headers


def fix_surrogates(value: Any) -> str:
    """FIX Unicode low surrogate must follow a high surrogate.
    eg in 'TREMI_2017-R\xe9sultats enqu\xeate bruts.csv'
    """
    if not isinstance(value, str):
        value = str(value)
    return value.encode("utf-8", "surrogateescape").decode("utf-8", "replace")


def has_nice_head(resp) -> bool:
    """Check if a HEAD response looks useful to us"""
    if not is_valid_status(resp.status):
        return False
    if not any([k in resp.headers for k in ("content-length", "last-modified")]):
        return False
    return True


def is_valid_status(status: str) -> bool | None:
    if not status:
        return False
    status_nb = int(status)
    if status_nb == 429:
        # We can't say the status since it's our client's fault
        return None
    return status_nb >= 200 and status_nb < 400


async def is_domain_backoff(domain: str) -> tuple[bool, str]:
    """Check if we should not crawl on this domain, in order to avoid 429 errors/bans as much as we can. We backoff if:
    - we have hit a 429
    - we have hit the rate limit on our side

    Returns:
        A boolean indicating if it should backoff or not
        A string with the message why we should backoff
    """
    backoff: tuple = (False, "")

    if domain in config.NO_BACKOFF_DOMAINS:
        return backoff

    since_backoff_period = datetime.now(timezone.utc) - timedelta(seconds=config.BACKOFF_PERIOD)

    pool = await context.pool()
    async with pool.acquire() as connection:
        # check if we trigger BACKOFF_NB_REQ for BACKOFF_PERIOD on this domain
        res = await connection.fetchrow(
            """
            SELECT COUNT(*) FROM checks
            WHERE domain = $1
            AND created_at >= $2
        """,
            domain,
            since_backoff_period,
        )
        backoff = (
            res["count"] >= config.BACKOFF_NB_REQ,
            f"Too many requests: {res['count']}",
        )

        if not backoff[0]:
            # check if we hit a ratelimit or received a 429 on this domain since COOL_OFF_PERIOD
            since_cool_off_period = datetime.now(timezone.utc) - timedelta(
                seconds=config.COOL_OFF_PERIOD
            )
            q = """
                SELECT
                    headers->>'x-ratelimit-remaining' as ratelimit_remaining,
                    headers->>'x-ratelimit-limit' as ratelimit_limit,
                    status,
                    created_at
                FROM checks
                WHERE domain = $1
                AND created_at >= $2
                ORDER BY created_at DESC
                LIMIT 1
            """
            res = await connection.fetchrow(q, domain, since_cool_off_period)
            if res:
                if res["status"] == 429:
                    # we have made too many requests already and haven't cooled off yet
                    # TODO: we could also user Retry-after, but it isn't returned correctly on 429 we're getting
                    return True, "429 status code has been returned on the latest call"
                try:
                    remain, limit = (
                        float(res["ratelimit_remaining"]),
                        float(res["ratelimit_limit"]),
                    )
                except (ValueError, TypeError):
                    pass
                else:
                    if limit == -1:
                        return False, ""
                    if remain == 0 or limit == 0:
                        # we have really messed up
                        backoff = True, "X-ratelimit reached"
                    elif remain / limit <= 0.1 and res["created_at"] > since_backoff_period:
                        # less than 10% left from our quota, we're backing off until backoff period
                        backoff = True, "X-ratelimit reached"

    return backoff
