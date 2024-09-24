import hashlib
import json
import logging

import pytest
from asyncpg import Record

from tests.conftest import RESOURCE_EXCEPTION_ID, RESOURCE_EXCEPTION_TABLE_INDEXES
from udata_hydra import config
from udata_hydra.analysis.csv import analyse_csv
from udata_hydra.db.resource import Resource
from udata_hydra.utils.db import get_columns_with_indexes

pytestmark = pytest.mark.asyncio


log = logging.getLogger("udata-hydra")


async def test_exception_analysis(
    setup_catalog_with_resource_exception, rmock, db, fake_check, produce_mock
):
    """
    Tests that exception resources (files that are too large to be normally processed) are indeed processed.
    """
    # Change config to accept large files
    save_config = config.MAX_FILESIZE_ALLOWED
    config.override(MAX_FILESIZE_ALLOWED={"csv": 5000})

    # Create a previous fake check for the resource
    check = await fake_check(resource_id=RESOURCE_EXCEPTION_ID)
    filename, expected_count = ("20190618-annuaire-diagnostiqueurs.csv", 45522)
    url = check["url"]
    table_name = hashlib.md5(url.encode("utf-8")).hexdigest()
    with open(f"tests/data/{filename}", "rb") as f:
        data = f.read()
    rmock.get(url, status=200, body=data)

    # Check resource status before analysis
    resource = await Resource.get(RESOURCE_EXCEPTION_ID)
    assert resource["status"] is None

    # Analyse the CSV
    await analyse_csv(check=check)

    # Check resource status after analysis
    resource = await Resource.get(RESOURCE_EXCEPTION_ID)
    assert resource["status"] is None

    # Check the table has been created in CSV DB, with the expected number of rows, and get the columns
    row: Record = await db.fetchrow(f'SELECT *, count(*) over () AS count FROM "{table_name}"')
    assert row["count"] == expected_count

    # Check if indexes have been created for the table
    expected_columns_with_indexes = list(RESOURCE_EXCEPTION_TABLE_INDEXES.keys())
    expected_columns_with_indexes.append("__id")
    indexes: list[Record] | None = await get_columns_with_indexes(table_name)
    assert indexes
    for idx in indexes:
        assert idx["table_name"] == table_name
        assert idx["column_name"] in expected_columns_with_indexes

    # Check the profile has been saved in the tables_index
    profile = await db.fetchrow(
        "SELECT csv_detective FROM tables_index WHERE resource_id = $1", check["resource_id"]
    )
    profile = json.loads(profile["csv_detective"])
    for attr in ("header", "columns", "formats", "profile"):
        assert profile[attr]
    assert profile["total_lines"] == expected_count
    config.override(MAX_FILESIZE_ALLOWED=save_config)
