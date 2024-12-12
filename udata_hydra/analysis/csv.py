import csv as stdcsv
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Iterator

from asyncpg import Record
from csv_detective.detection import engine_to_file
from csv_detective.explore_csv import routine as csv_detective_routine
from progressist import ProgressBar
from slugify import slugify
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import asyncpg
from sqlalchemy.schema import CreateIndex, CreateTable, Index
from str2bool import str2bool
from str2float import str2float

from udata_hydra import config, context
from udata_hydra.analysis import helpers
from udata_hydra.db import compute_insert_query
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.utils import (
    ParseException,
    Reader,
    Timer,
    download_resource,
    handle_parse_exception,
    queue,
    send,
)
from udata_hydra.utils.minio import MinIOClient
from udata_hydra.utils.parquet import save_as_parquet

log = logging.getLogger("udata-hydra")

# Increase CSV field size limit to maximum possible
# https://stackoverflow.com/a/15063941
field_size_limit = sys.maxsize
while True:
    try:
        stdcsv.field_size_limit(field_size_limit)
        break
    except OverflowError:
        field_size_limit = int(field_size_limit / 10)

PYTHON_TYPE_TO_PG = {
    "string": String,
    "float": Float,
    "int": BigInteger,
    "bool": Boolean,
    "json": JSON,
    "date": Date,
    "datetime": DateTime,
}

PYTHON_TYPE_TO_PY = {
    "string": str,
    "float": float,
    "int": int,
    "bool": bool,
    "json": helpers.to_json,
    "date": helpers.to_date,
    "datetime": helpers.to_datetime,
}

RESERVED_COLS = ("__id", "cmin", "cmax", "collation", "ctid", "tableoid", "xmin", "xmax")
minio_client = MinIOClient()


async def notify_udata(check_id: int) -> None:
    """Notify udata of the result of a parsing"""
    # Get the check again to get its updated data
    check: Record | None = await Check.get_by_id(check_id, with_deleted=True)
    resource_id = check["resource_id"]
    db = await context.pool()
    record = await db.fetchrow("SELECT dataset_id FROM catalog WHERE resource_id = $1", resource_id)
    if record:
        payload = {
            "resource_id": resource_id,
            "dataset_id": record["dataset_id"],
            "document": {
                "analysis:parsing:error": check["parsing_error"],
                "analysis:parsing:started_at": check["parsing_started_at"].isoformat()
                if check["parsing_started_at"]
                else None,
                "analysis:parsing:finished_at": check["parsing_finished_at"].isoformat()
                if check["parsing_finished_at"]
                else None,
            },
        }
        if config.CSV_TO_PARQUET:
            payload["document"]["analysis:parsing:parquet_url"] = check.get("parquet_url")
            payload["document"]["analysis:parsing:parquet_size"] = check.get("parquet_size")
        queue.enqueue(send, _priority="high", **payload)


async def analyse_csv(
    check_id: int | None = None,
    url: str | None = None,
    file_path: str | None = None,
    debug_insert: bool = False,
) -> None:
    """Launch csv analysis from a check or an URL (debug), using previously downloaded file at file_path if any"""
    if not config.CSV_ANALYSIS:
        log.debug("CSV_ANALYSIS turned off, skipping.")
        return

    # Get check and resource_id. Try to get the check from the check ID, then from the URL
    check: Record | None = await Check.get_by_id(check_id, with_deleted=True) if check_id else None
    if not check:
        checks: list[Record] | None = await Check.get_by_url(url) if url else None
        if checks and len(checks) > 1:
            log.warning(f"Multiple checks found for URL {url}, using the latest one")
        check = checks[0] if checks else None
    if not check:
        log.error("No check found or URL provided")
        return
    resource_id: str = str(check["resource_id"])
    url = check["url"]

    # Update resource status to ANALYSING_CSV
    await Resource.update(resource_id, {"status": "ANALYSING_CSV"})

    # Check if the resource is in the exceptions table
    # If it is, get the table_indexes to use them later
    exception: Record | None = await ResourceException.get_by_resource_id(resource_id)
    table_indexes: dict | None = None
    if exception and exception.get("table_indexes"):
        table_indexes = json.loads(exception["table_indexes"])

    timer = Timer("analyse-csv")
    assert any(_ is not None for _ in (check["id"], url))

    headers = json.loads(check.get("headers") or "{}")
    tmp_file = (
        open(file_path, "rb")
        if file_path
        else await download_resource(
            url=url,
            headers=headers,
            max_size_allowed=None if exception else int(config.MAX_FILESIZE_ALLOWED["csv"]),
        )
    )
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    timer.mark("download-file")

    try:
        await Check.update(check["id"], {"parsing_started_at": datetime.now(timezone.utc)})

        # Launch csv-detective against given file
        try:
            csv_inspection: dict | None = csv_detective_routine(
                csv_file_path=tmp_file.name, output_profile=True, num_rows=-1, save_results=False
            )
        except Exception as e:
            raise ParseException(
                step="csv_detective", resource_id=resource_id, url=url, check_id=check_id
            ) from e

        timer.mark("csv-inspection")

        await csv_to_db(
            file_path=tmp_file.name,
            inspection=csv_inspection,
            table_name=table_name,
            table_indexes=table_indexes,
            resource_id=resource_id,
            debug_insert=debug_insert,
        )
        timer.mark("csv-to-db")

        parquet_args: tuple[str, int] | None = await csv_to_parquet(
            file_path=tmp_file.name,
            inspection=csv_inspection,
            table_name=table_name,
            resource_id=resource_id,
        )
        timer.mark("csv-to-parquet")
        await Check.update(
            check["id"],
            {
                "parsing_table": table_name,
                "parsing_finished_at": datetime.now(timezone.utc),
                "parquet_url": parquet_args[0] if parquet_args else None,
                "parquet_size": parquet_args[1] if parquet_args else None,
            },
        )  # TODO: return the outdated check so that we don't have to re-request it in notify_data again
        await csv_to_db_index(table_name, csv_inspection, check)

    except ParseException as e:
        await handle_parse_exception(e, table_name, check)
    finally:
        await notify_udata(check["id"])
        timer.stop()
        tmp_file.close()
        os.remove(tmp_file.name)

        # Reset resource status to None
        await Resource.update(resource_id, {"status": None})


def smart_cast(_type: str, value, failsafe: bool = False) -> Any:
    try:
        if value is None or value == "":
            return None
        if _type == "bool":
            return str2bool(value)
        return PYTHON_TYPE_TO_PY[_type](value)
    except ValueError as e:
        if _type == "int":
            _value = str2float(value, default=None)
            if _value:
                return int(_value)
        elif _type == "float":
            return str2float(value, default=None)
        if not failsafe:
            raise e
        log.warning(f'Could not convert "{value}" to {_type}, defaulting to null')
        return None


def compute_create_table_query(
    table_name: str, columns: dict, indexes: dict[str, str] | None = None
) -> str:
    """Use sqlalchemy to build a CREATE TABLE statement that should not be vulnerable to injections"""

    metadata = MetaData()
    table = Table(table_name, metadata, Column("__id", Integer, primary_key=True))

    for col_name, col_type in columns.items():
        table.append_column(Column(col_name, PYTHON_TYPE_TO_PG.get(col_type, String)))

    if indexes:
        for col_name, index_type in indexes.items():
            if index_type not in config.SQL_INDEXES_TYPES_SUPPORTED:
                log.error(
                    f'Index type "{index_type}" is unknown or not supported yet! Index for colum {col_name} was not created.'
                )
                continue

            else:
                if index_type == "index":
                    index_name = f"{table_name}_{slugify(col_name)}_idx"
                    try:
                        table.append_constraint(Index(index_name, col_name))
                    except KeyError:
                        raise KeyError(
                            f'Error creating index "{index_name}" on column "{col_name}". Does the column "{col_name}" exist in the table?'
                        )
                # TODO: other index types. Not easy with sqlalchemy, maybe use raw sql?

    compiled_query = CreateTable(table).compile(dialect=asyncpg.dialect())
    query: str = compiled_query.string

    # Add the index creation queries to the main query
    for index in table.indexes:
        log.debug(f'Creating {index_type} on column "{col_name}"')
        query_idx = CreateIndex(index).compile(dialect=asyncpg.dialect())
        query: str = query + ";" + query_idx.string

    # compiled query will want to write "%% mon pourcent" VARCHAR but will fail when querying "% mon pourcent"
    # also, "% mon pourcent" works well in pg as a column
    # TODO: dirty hack, maybe find an alternative
    query = query.replace("%%", "%")
    return query


def generate_records(file_path: str, inspection: dict, columns: dict) -> Iterator[list]:
    # because we need the iterator twice, not possible to
    # handle parquet and db through the same iteration
    with Reader(file_path, inspection) as reader:
        for line in reader:
            if line:
                yield [smart_cast(t, v, failsafe=True) for t, v in zip(columns.values(), line)]


async def csv_to_parquet(
    file_path: str,
    inspection: dict,
    table_name: str,
    resource_id: str | None = None,
) -> tuple[str, int] | None:
    """
    Convert a csv file to parquet using inspection data.

    Args:
        file_path: CSV file path to convert.
        inspection: CSV detective report.
        table_name: used to name the parquet file.

    Returns:
        parquet_url: URL of the parquet file.
        parquet_size: size of the parquet file.
    """
    if not config.CSV_TO_PARQUET:
        log.debug("CSV_TO_PARQUET turned off, skipping parquet export.")
        return

    if int(inspection.get("total_lines", 0)) < config.MIN_LINES_FOR_PARQUET:
        log.debug(
            f"Skipping parquet export for {table_name} because it has less than {config.MIN_LINES_FOR_PARQUET} lines."
        )
        return

    log.debug(
        f"Converting from {engine_to_file.get(inspection.get('engine', ''), 'CSV')} "
        f"to parquet for {table_name} and sending to Minio."
    )

    if resource_id:
        # Update resource status to CONVERTING_TO_PARQUET
        await Resource.update(resource_id, {"status": "CONVERTING_TO_PARQUET"})

    columns = {c: v["python_type"] for c, v in inspection["columns"].items()}
    # save the file as parquet and store it on Minio instance
    parquet_file, _ = save_as_parquet(
        records=generate_records(file_path, inspection, columns),
        columns=columns,
        output_filename=table_name,
    )
    parquet_size: int = os.path.getsize(parquet_file)
    parquet_url: str = minio_client.send_file(parquet_file)
    return parquet_url, parquet_size


async def csv_to_db(
    file_path: str,
    inspection: dict,
    table_name: str,
    table_indexes: dict[str, str] | None = None,
    resource_id: str | None = None,
    debug_insert: bool = False,
) -> None:
    """
    Convert a csv file to database table using inspection data. It should (re)create one table:
    - `table_name` with data from `file_path`

    :file_path: CSV file path to convert
    :inspection: CSV detective report
    :table_name: used to create tables
    :debug_insert: insert record one by one instead of using postgresql COPY
    """
    if not config.CSV_TO_DB:
        log.debug("CSV_TO_DB turned off, skipping.")
        return

    log.debug(
        f"Converting from {engine_to_file.get(inspection.get('engine', ''), 'CSV')} "
        f"to db for {table_name}"
    )

    if resource_id:
        # Update resource status to INSERTING_IN_DB
        await Resource.update(resource_id, {"status": "INSERTING_IN_DB"})

    # build a `column_name: type` mapping and explicitely rename reserved column names
    columns = {
        f"{c}__hydra_renamed" if c.lower() in RESERVED_COLS else c: v["python_type"]
        for c, v in inspection["columns"].items()
    }

    q = f'DROP TABLE IF EXISTS "{table_name}"'
    db = await context.pool("csv")
    await db.execute(q)

    # Create table
    q = compute_create_table_query(table_name=table_name, columns=columns, indexes=table_indexes)
    try:
        await db.execute(q)
    except Exception as e:
        raise ParseException(
            step="create_table_query", resource_id=resource_id, table_name=table_name
        ) from e

    # this use postgresql COPY from an iterator, it's fast but might be difficult to debug
    if not debug_insert:
        # NB: also see copy_to_table for a file source
        try:
            await db.copy_records_to_table(
                table_name,
                records=generate_records(file_path, inspection, columns),
                columns=columns.keys(),
            )
        except Exception as e:  # I know what I'm doing, pinky swear
            raise ParseException(
                step="copy_records_to_table", resource_id=resource_id, table_name=table_name
            ) from e
    # this inserts rows from iterator one by one, slow but useful for debugging
    else:
        bar = ProgressBar(total=inspection["total_lines"])
        for r in bar.iter(generate_records(file_path, inspection, columns)):
            data = {k: v for k, v in zip(columns.keys(), r)}
            # NB: possible sql injection here, but should not be used in prod
            q = compute_insert_query(table_name=table_name, data=data, returning="__id")
            await db.execute(q, *data.values())


async def csv_to_db_index(table_name: str, inspection: dict, check: Record) -> None:
    """Store meta info about a converted CSV table in `DATABASE_URL_CSV.tables_index`"""
    db = await context.pool("csv")
    q = "INSERT INTO tables_index(parsing_table, csv_detective, resource_id, url) VALUES($1, $2, $3, $4)"
    await db.execute(
        q,
        table_name,
        json.dumps(inspection),
        check.get("resource_id"),
        check.get("url"),
    )


async def delete_table(table_name: str) -> None:
    pool = await context.pool("csv")
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            await conn.execute("DELETE FROM tables_index WHERE parsing_table = $1", table_name)
