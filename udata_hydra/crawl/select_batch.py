from datetime import datetime, timezone

from asyncpg import Record

from udata_hydra import config, context
from udata_hydra.db.resource import Resource


async def select_rows_based_on_query(connection, q: str, *args) -> list[Record]:
    """
    A transaction wrapper around a select query q pass as param with *args.
    It first creates a temporary table based on this query,
    then fetches the selected rows and drops the temporary table.
    It finally returns the selected rows.
    """
    temporary_table = "check_urls"
    create_temp_select_table_query = (
        f"""CREATE TEMPORARY TABLE {temporary_table} AS {q} FOR UPDATE;"""
    )
    async with connection.transaction():
        await connection.execute("BEGIN;")
        await connection.execute(create_temp_select_table_query, *args)
        to_check = await connection.fetch(f"SELECT * FROM {temporary_table};")
        await connection.execute("COMMIT;")
    await connection.execute(f"DROP TABLE {temporary_table};")
    return to_check


async def select_batch_resources_to_check() -> list[Record]:
    """Select a batch of resources to check from the catalog
    - It first selects resources with priority=True
    - ...then resources without last check
    - and if the total number of selected resources is still less than the batch size, it will also add resources with outdated last check in the batch
    """
    context.monitor().set_status("Getting a batch from catalog...")

    pool = await context.pool()
    async with pool.acquire() as connection:
        excluded = Resource.get_excluded_clause()

        # first resources that are prioritised
        q = f"""
            SELECT * FROM (
                SELECT catalog.url, dataset_id, resource_id
                FROM catalog
                WHERE {excluded}
                AND priority = True
            ) s
            ORDER BY random() LIMIT {config.BATCH_SIZE}
        """
        to_check: list[Record] = await select_rows_based_on_query(connection, q)

        # then resources with no last check
        # (either because they have never been checked before, or because the last check has been deleted)
        if len(to_check) < config.BATCH_SIZE:
            q = f"""
                SELECT * FROM (
                    SELECT catalog.url, dataset_id, resource_id
                    FROM catalog
                    WHERE catalog.last_check IS NULL
                    AND {excluded}
                    AND priority = False
                ) s
                ORDER BY random() LIMIT {config.BATCH_SIZE}
            """
            to_check += await select_rows_based_on_query(connection, q)

        # if not enough for our batch size, handle resources with planned new check
        if len(to_check) < config.BATCH_SIZE:
            now = datetime.now(timezone.utc)
            limit = config.BATCH_SIZE - len(to_check)
            q = f"""
            SELECT * FROM (
                SELECT catalog.url, dataset_id, catalog.resource_id
                FROM catalog, checks
                WHERE catalog.last_check IS NOT NULL
                AND catalog.last_check = checks.id
                AND (checks.next_check_at <= $1 OR checks.next_check_at IS NULL)
                AND {excluded}
                AND catalog.priority = False
            ) s
            ORDER BY random() LIMIT {limit}
            """
            to_check += await select_rows_based_on_query(connection, q, now)

    return to_check
