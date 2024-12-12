from datetime import datetime, timedelta

import nest_asyncio
import pytest
from minicli import run

from tests.conftest import RESOURCE_ID, RESOURCE_URL
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


async def test_analysis_csv(setup_catalog, rmock, catalog_content, db, fake_check, produce_mock):
    # Analyse using check_id
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body=catalog_content)
    run("analyse-csv", check_id=str(check["id"]))
    # Analyse using URL
    check = await fake_check()
    url = check["url"]
    rmock.get(url, status=200, body=catalog_content)
    run("analyse-csv", url=RESOURCE_URL)


async def test_purge_checks(setup_catalog, db, fake_check):
    await fake_check(created_at=datetime.now() - timedelta(days=50))
    await fake_check(created_at=datetime.now() - timedelta(days=30))
    await fake_check(created_at=datetime.now() - timedelta(days=10))
    run("purge_checks", retention_days=40)
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 2
    run("purge_checks", retention_days=20)
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 1


async def test_purge_csv_tables(setup_catalog, db, fake_check):
    # pretend we have a csv_analysis with a converted table for this url
    check = await fake_check(parsing_table=True)
    md5 = check["parsing_table"]
    await db.execute(f'CREATE TABLE "{md5}"(id serial)')
    # check table is there before purge
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None
    # pretend the resource is deleted
    await db.execute("UPDATE catalog SET deleted = TRUE")
    # purge
    run("purge_csv_tables")
    # check table is gone
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is None
    res = await db.fetchrow("SELECT * FROM tables_index WHERE parsing_table = $1", md5)
    assert res is None


async def test_purge_csv_tables_url_used_by_other_resource(setup_catalog, db, fake_check):
    """We should not delete csv table if the url is used by a resource still active"""
    # pretend we have a csv_analysis with a converted table for this url
    check = await fake_check(parsing_table=True)
    md5 = check["parsing_table"]
    await db.execute(f'CREATE TABLE "{md5}"(id serial)')
    # check table is there before purge
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None
    # pretend the resource is deleted
    await db.execute("UPDATE catalog SET deleted = TRUE")
    # insert another resource with same url
    await db.execute(
        "INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority) VALUES($1, $2, $3, $4, $5)",
        "6115eed4acb337ce13b83db3",
        "7a0c10a0-8e6f-403f-a987-2e223b22ee33",
        check["url"],
        False,
        False,
    )
    # purge
    run("purge_csv_tables")
    # check table is _not_ gone
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None


async def test_purge_csv_tables_url_used_by_deleted_resource_only(setup_catalog, db, fake_check):
    """We should delete csv table if all resource with this url are marked as deleted"""
    # pretend we have a csv_analysis with a converted table for this url
    check = await fake_check(parsing_table=True)
    md5 = check["parsing_table"]
    await db.execute(f'CREATE TABLE "{md5}"(id serial)')
    # check table is there before purge
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None
    # pretend the resource is deleted
    await db.execute("UPDATE catalog SET deleted = TRUE")
    # insert another deleted resource with same url
    await db.execute(
        "INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority) VALUES($1, $2, $3, $4, $5)",
        "6115eed4acb337ce13b83db3",
        "7a0c10a0-8e6f-403f-a987-2e223b22ee33",
        check["url"],
        True,
        False,
    )
    # purge
    run("purge_csv_tables")
    # check table is gone
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is None
    res = await db.fetchrow("SELECT * FROM tables_index WHERE parsing_table = $1", md5)
    assert res is None


async def test_purge_csv_tables_url_not_in_catalog(setup_catalog, db, fake_check):
    """We should delete csv table if the url is not the catalog anymore"""
    # pretend we have a csv_analysis with a converted table for this url
    check = await fake_check(parsing_table=True)
    md5 = check["parsing_table"]
    await db.execute(f'CREATE TABLE "{md5}"(id serial)')
    # check table is there before purge
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is not None
    # pretend the resource URL have been updated
    await db.execute("UPDATE catalog SET url = 'https://example.com/resource-0'")
    # purge
    run("purge_csv_tables")
    # check table is gone
    res = await db.fetchrow("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1", md5)
    assert res is None
    res = await db.fetchrow("SELECT * FROM tables_index WHERE parsing_table = $1", md5)
    assert res is None


async def test_load_catalog_url_has_changed(setup_catalog, rmock, db, catalog_content):
    # the resource url has changed in comparison to load_catalog
    catalog_content = catalog_content.replace(
        b"https://example.com/resource-1", b"https://example.com/resource-0"
    )
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content)
    run("load_catalog", url=catalog)

    # check that we still only have one entry for this resource in the catalog
    res = await db.fetch("SELECT * FROM catalog WHERE resource_id = $1", f"{RESOURCE_ID}")
    assert len(res) == 1
    assert res[0]["url"] == "https://example.com/resource-0"
    assert res[0]["deleted"] is False


async def test_insert_resource_in_catalog(rmock):
    new_dataset_id = "a" * 24
    new_resource_url = "https://new-url.xyz"
    rmock.get(
        f"https://www.data.gouv.fr/api/2/datasets/resources/{RESOURCE_ID}/",
        status=200,
        payload={
            "dataset_id": new_dataset_id,
            "resource": {
                "id": RESOURCE_ID,
                "url": new_resource_url,
            },
        },
    )
    run("insert_resource_into_catalog", resource_id=RESOURCE_ID)
    resource = await Resource.get(RESOURCE_ID)
    assert resource["dataset_id"] == new_dataset_id
    assert resource["url"] == new_resource_url
