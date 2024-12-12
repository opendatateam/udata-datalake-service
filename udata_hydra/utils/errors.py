import logging
from datetime import datetime, timezone

import sentry_sdk
from asyncpg import Record

from udata_hydra import config, context
from udata_hydra.db.check import Check

log = logging.getLogger("udata-hydra")


class ExceptionWithSentryDetails(Exception):
    """
    Custom exception which enriches Sentry with tags if available.
    """

    def __init__(
        self,
        message: str | None = None,
        step: str | None = None,
        resource_id: str | None = None,
        url: str | None = None,
        check_id: int | None = None,
        table_name: str | None = None,
        *args,
    ) -> None:
        self.step = step
        self.message = message
        if sentry_sdk.Hub.current.client:
            with sentry_sdk.new_scope() as scope:
                # scope.set_level("warning")
                scope.set_tags(
                    {
                        "resource_id": resource_id or "unknown",
                        "url": url or "unknown",
                        "check_id": check_id or "unknown",
                        "table_name": table_name or "unknown",
                    }
                )
                sentry_sdk.capture_exception(self)
        super().__init__(message, *args)


class ParseException(ExceptionWithSentryDetails):
    """Exception raised when an error occurs during parsing."""

    pass


class IOException(ExceptionWithSentryDetails):
    """Exception raised when an error occurs during IO operations."""

    pass


async def handle_parse_exception(e: ParseException, table_name: str, check: Record | None) -> None:
    """Specific ParseException handling. Store error if in a check context. Also cleanup :table_name: if needed."""
    db = await context.pool("csv")
    await db.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    if check:
        if config.SENTRY_DSN:
            with sentry_sdk.new_scope():
                event_id = sentry_sdk.capture_exception(e)
        # e.__cause__ let us access the "inherited" error of ParseException (raise e from cause)
        # it's called explicit exception chaining and it's very cool, look it up (PEP 3134)!
        err = f"{e.step}:sentry:{event_id}" if config.SENTRY_DSN else f"{e.step}:{str(e.__cause__)}"
        await Check.update(
            check["id"],
            {"parsing_error": err, "parsing_finished_at": datetime.now(timezone.utc)},
        )
        log.error("Parsing error", exc_info=e)
    else:
        raise e
