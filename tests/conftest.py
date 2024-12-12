import asyncio
import hashlib
import logging
import os
import uuid
from datetime import datetime

import asyncpg
import nest_asyncio
import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer
from aioresponses import aioresponses
from minicli import run

import udata_hydra.cli  # noqa - this register the cli cmds
from udata_hydra import config
from udata_hydra.app import app_factory
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource
from udata_hydra.db.resource_exception import ResourceException
from udata_hydra.logger import stop_sentry

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/postgres")
RESOURCE_ID = "c4e3a9fb-4415-488e-ba57-d05269b27adf"
RESOURCE_EXCEPTION_ID = "d4e3a9fb-4415-488e-ba57-d05269b27adf"
RESOURCE_EXCEPTION_TABLE_INDEXES = {"Nom": "index", "N° de certificat": "index"}
RESOURCE_URL = "https://example.com/resource-1"
DATASET_ID = "601ddcfc85a59c3a45c2435a"
NOT_EXISTING_RESOURCE_ID = "5d0b2b91-b21b-4120-83ef-83f818ba2451"
pytestmark = pytest.mark.asyncio

nest_asyncio.apply()

log = logging.getLogger("udata-hydra")


def dummy(return_value=None):
    """
    Creates a generic function which returns what is asked
    A kind of MagicMock but pickle-able for workers
    You should use this when mocking an enqueued function
    """

    async def fn(*args, **kwargs):
        return return_value

    return fn


@pytest.fixture
def is_harvested(request):
    return "catalog_harvested" in [m.name for m in request.node.iter_markers()]


@pytest.fixture
def api_headers() -> dict:
    return {"Authorization": f"Bearer {config.API_KEY}"}


@pytest.fixture
def api_headers_wrong_token() -> dict:
    return {"Authorization": "Bearer stupid-token"}


# this really really really should run first (or "prod" db will get erased)
@pytest.fixture(autouse=True, scope="session")
def setup():
    config.override(
        DATABASE_URL=DATABASE_URL,
        # use same database for tests
        DATABASE_URL_CSV=DATABASE_URL,
        UDATA_URI="https://udata.example.com",
        UDATA_URI_API_KEY="sup3rs3cr3t",
        TESTING=True,
        SLEEP_BETWEEN_BATCHES=0,
        WEBHOOK_ENABLED=True,
        SENTRY_DSN=None,
    )
    # prevent sentry from sending events in tests (config override is not enough)
    stop_sentry()


@pytest_asyncio.fixture(autouse=True)
async def mock_pool(mocker, event_loop):
    """This avoids having different pools attached to different event loops"""
    m = mocker.patch("udata_hydra.context.pool")
    pool = await asyncpg.create_pool(dsn=DATABASE_URL, max_size=50, loop=event_loop)
    m.return_value = pool
    yield
    await pool.close()


@pytest_asyncio.fixture(autouse=True)
async def patch_enqueue(mocker, event_loop):
    """
    Patch our enqueue helper
    This bypasses rq totally by executing the function in the same event loop
    This also has the advantage of bubbling up errors in queued functions
    """

    def _execute(fn, *args, **kwargs):
        kwargs.pop("_priority")
        result = fn(*args, **kwargs)
        if asyncio.iscoroutine(result):
            loop = event_loop
            coro_result = loop.run_until_complete(result)
            return coro_result
        return result

    mocker.patch("udata_hydra.utils.queue.enqueue", _execute)


@pytest_asyncio.fixture
async def client():
    app = await app_factory()
    async with TestClient(TestServer(app)) as client:
        yield client


@pytest.fixture
def catalog_content(is_harvested):
    filename = "catalog" if not is_harvested else "catalog_harvested"
    with open(f"tests/data/{filename}.csv", "rb") as cfile:
        return cfile.read()


@pytest.fixture
def clean_db():
    run("drop_dbs", dbs=["main"])
    run("migrate")
    yield


@pytest.fixture
def setup_catalog(catalog_content, rmock):
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content)
    run("drop_dbs", dbs=["main"])
    run("migrate")
    run("load_catalog", url=catalog)


@pytest.fixture
async def setup_catalog_with_resource_exception(setup_catalog):
    """Setup a catalog with a resource that is too large to be processed
    Columns for the resource RESOURCE_ID_EXCEPTION:
    ['__id', 'Nom', 'Prenom', 'Societe', 'Adresse', 'CP', 'Ville', 'Tel1', 'Tel2', 'email', 'Organisme', 'Org Cofrac', 'Type de certificat', 'N° de certificat', 'Date début validité', 'Date fin validité']
    """
    await Resource.insert(
        dataset_id=DATASET_ID, resource_id=RESOURCE_EXCEPTION_ID, url="http://example.com/"
    )
    await ResourceException.insert(
        resource_id=RESOURCE_EXCEPTION_ID,
        table_indexes=RESOURCE_EXCEPTION_TABLE_INDEXES,
        comment="This is a test comment.",
    )


@pytest.fixture
def produce_mock(mocker):
    mocker.patch("udata_hydra.crawl.preprocess_check_data.send", dummy())
    mocker.patch("udata_hydra.analysis.resource.send", dummy())
    mocker.patch("udata_hydra.analysis.csv.send", dummy())


@pytest.fixture
def analysis_mock(mocker):
    """Disable analyse_resource while crawling"""
    mocker.patch(
        "udata_hydra.analysis.resource.analyse_resource",
        dummy({"error": None, "checksum": None, "filesize": None, "mime_type": None}),
    )


@pytest.fixture
def rmock():
    # passthrough for local requests (aiohttp TestServer)
    with aioresponses(passthrough=["http://127.0.0.1"]) as m:
        yield m


@pytest_asyncio.fixture
async def db():
    conn = await asyncpg.connect(dsn=DATABASE_URL)
    yield conn
    await conn.close()


@pytest_asyncio.fixture
async def insert_fake_resource():
    async def _insert_fake_resource(database, status: str | None = None):
        await Resource.insert(
            dataset_id=DATASET_ID,
            resource_id=RESOURCE_ID,
            url=RESOURCE_URL,
            status=status,
            priority=True,
        )

    return _insert_fake_resource


@pytest_asyncio.fixture
def fake_resource_id():
    return uuid.uuid4


@pytest_asyncio.fixture
async def fake_check():
    async def _fake_check(
        status=200,
        error=None,
        timeout=False,
        resource=1,
        created_at=None,
        headers={"x-do": "you"},
        checksum=None,
        resource_id=RESOURCE_ID,
        detected_last_modified_at=None,
        next_check_at=None,
        parsing_table=False,
        parquet_url=False,
        domain="example.com",
    ) -> dict:
        url = f"https://example.com/resource-{resource}"
        data = {
            "url": url,
            "domain": domain,
            "status": status,
            "headers": headers,
            "timeout": timeout,
            "response_time": 0.1,
            "resource_id": resource_id,
            "error": error,
            "checksum": checksum,
            "detected_last_modified_at": detected_last_modified_at,
            "next_check_at": next_check_at,
            "parsing_table": hashlib.md5(url.encode("utf-8")).hexdigest()
            if parsing_table
            else None,
            "parquet_url": "https://example.org/file.parquet" if parquet_url else None,
            "parquet_size": 2048 if parquet_url else None,
        }
        check = await Check.insert(data)
        data["id"] = check["id"]
        if created_at:
            await Check.update(check["id"], {"created_at": created_at})
            data["created_at"] = created_at
        return data

    return _fake_check


@pytest.fixture
def udata_url():
    return f"{config.UDATA_URI}/datasets/{DATASET_ID}/resources/{RESOURCE_ID}/extras/"


@pytest.fixture
def udata_resource_payload():
    return {
        "resource_id": "f8fb4c7b-3fc6-4448-b34f-81a9991f18ec",
        "dataset_id": "61fd30cb29ea95c7bc0e1211",
        "document": {
            "id": "f8fb4c7b-3fc6-4448-b34f-81a9991f18ec",
            "url": "http://dev.local/",
            "title": "random title",
            "description": "random description",
            "filetype": "file",
            "type": "documentation",
            "mime": "text/plain",
            "schema": None,
            "filesize": 1024,
            "checksum_type": "sha1",
            "checksum_value": "b7b1cd8230881b18b6b487d550039949867ec7c5",
            "created_at": datetime.now().isoformat(),
            "last_modified": datetime.now().isoformat(),
        },
    }
