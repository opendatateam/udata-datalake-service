import asyncio

from asyncpg import Record

from udata_hydra import config, context
from udata_hydra.crawl.check_resources import check_batch_resources
from udata_hydra.crawl.select_batch import select_batch_resources_to_check
from udata_hydra.logger import setup_logging
from udata_hydra.utils import queue  # noqa

log = setup_logging()


async def start_checks(iterations: int = -1) -> None:
    """Launch check batches

    :iterations: for testing purposes (break infinite loop)
    """
    try:
        context.monitor().init(
            CHECK_DELAYS=config.CHECK_DELAYS,
            BATCH_SIZE=config.BATCH_SIZE,
            BACKOFF_NB_REQ=config.BACKOFF_NB_REQ,
            BACKOFF_PERIOD=config.BACKOFF_PERIOD,
        )

        while iterations != 0:
            batch: list[Record] = await select_batch_resources_to_check()

            if batch and len(batch):
                await check_batch_resources(batch)

            else:
                context.monitor().set_status("No resources to check for now.")

            await asyncio.sleep(config.SLEEP_BETWEEN_BATCHES)
            iterations -= 1

    finally:
        pool = await context.pool()
        await pool.close()


def run() -> None:
    """Main function

    :iterations: for testing purposes (break infinite loop)
    """
    try:
        asyncio.get_event_loop().run_until_complete(start_checks())
    except KeyboardInterrupt:
        pass
    finally:
        context.monitor().teardown()


if __name__ == "__main__":
    run()
