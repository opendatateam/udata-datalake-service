"""
NB: we can't use pytest-aiohttp helpers because
it will interfere with the rest of our async code
"""

import hashlib
import json
from datetime import datetime

import pytest
from aiohttp import RequestInfo
from aiohttp.client_exceptions import ClientError, ClientResponseError
from yarl import URL

from tests.conftest import DATASET_ID, RESOURCE_ID, RESOURCE_URL
from udata_hydra.db.resource import Resource

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "query",
    [
        "url=https://example.com/resource-1",
        f"resource_id={RESOURCE_ID}",
    ],
)
async def test_get_latest_check(setup_catalog, client, query, fake_check, fake_resource_id):
    await fake_check(parsing_table=True, parquet_url=True)

    # Test invalid query
    stupid_query: str = "stupid=stupid"
    resp = await client.get(f"/api/checks/latest?{stupid_query}")
    assert resp.status == 400

    # Test not existing resource url
    not_existing_url_query: str = "url=https://example.com/not-existing-resource"
    resp = await client.get(f"/api/checks/latest?{not_existing_url_query}")
    assert resp.status == 404

    # Test not existing resource_id
    not_existing_resource_id_query: str = f"resource_id={fake_resource_id()}"
    resp = await client.get(f"/api/checks/latest?{not_existing_resource_id_query}")
    assert resp.status == 404

    # Test existing resource
    resp = await client.get(f"/api/checks/latest?{query}")
    assert resp.status == 200
    data: dict = await resp.json()
    assert data.pop("created_at")
    assert data.pop("id")
    url = RESOURCE_URL
    assert data == {
        "response_time": 0.1,
        "deleted": False,
        "resource_id": RESOURCE_ID,
        "catalog_id": 1,
        "domain": "example.com",
        "error": None,
        "url": url,
        "headers": {"x-do": "you"},
        "timeout": False,
        "next_check_at": None,
        "dataset_id": DATASET_ID,
        "status": 200,
        "parsing_error": None,
        "parsing_finished_at": None,
        "parsing_started_at": None,
        "parsing_table": hashlib.md5(url.encode("utf-8")).hexdigest(),
        "parquet_url": "https://example.org/file.parquet",
        "parquet_size": 2048,
    }

    # Test deleted resource
    await Resource.update(resource_id=RESOURCE_ID, data={"deleted": True})
    resp = await client.get(f"/api/checks/latest?{query}")
    assert resp.status == 410


@pytest.mark.parametrize(
    "query",
    [
        "url=https://example.com/resource-1",
        f"resource_id={RESOURCE_ID}",
    ],
)
async def test_get_all_checks(setup_catalog, client, query, fake_check):
    resp = await client.get(f"/api/checks/all?{query}")
    assert resp.status == 404

    await fake_check(status=500, error="no-can-do")
    await fake_check()
    resp = await client.get(f"/api/checks/all?{query}")
    assert resp.status == 200
    data: list = await resp.json()
    assert len(data) == 2
    first, second = data
    assert first["status"] == 200
    assert second["status"] == 500
    assert second["error"] == "no-can-do"


@pytest.mark.parametrize(
    "query,value_template",
    [
        ("group_by=domain&created_at=today", "{0}.com"),
        ("group_by=headers->>'content-type'&created_at=2024-09-05", "application/{0}"),
    ],
)
async def test_api_get_checks_aggregate(setup_catalog, client, query, value_template, fake_check):
    resp = await client.get(f"/api/checks/aggregate?{query}")
    assert resp.status == 404

    occurences = [10, 3, 1]

    for i, occurence in enumerate(occurences):
        for _ in range(occurence):
            await fake_check(created_at=datetime.now(), domain=value_template.format(i))

    for i, occurence in enumerate(occurences):
        for _ in range(occurence):
            await fake_check(
                created_at=datetime(2024, 9, 5, 10, 0, 0),
                headers={"content-type": value_template.format(i)},
            )

    resp = await client.get(f"/api/checks/aggregate?{query}")
    assert resp.status == 200
    data: list = await resp.json()
    assert len(data) == len(occurences)

    for i in range(len(data)):
        assert data[i]["value"] == value_template.format(i)
        assert data[i]["count"] == occurences[i]


async def test_create_check_wrongly(
    setup_catalog,
    client,
    fake_check,
    fake_resource_id,
    api_headers,
):
    await fake_check()
    post_data = {"stupid_data": "stupid"}
    resp = await client.post("/api/checks/", headers=api_headers, json=post_data)
    assert resp.status == 400

    post_data = {"resource_id": str(fake_resource_id())}
    resp = await client.post("/api/checks/", headers=api_headers, json=post_data)
    assert resp.status == 404


@pytest.mark.parametrize(
    "resource",
    [
        # resource_id, status, timeout, exception
        (RESOURCE_ID, 201, False, None),
        (RESOURCE_ID, 500, False, None),
        (RESOURCE_ID, None, False, ClientError("client error")),
        (RESOURCE_ID, None, False, AssertionError),
        (RESOURCE_ID, None, False, UnicodeError),
        (RESOURCE_ID, None, True, TimeoutError),
        (
            RESOURCE_ID,
            429,
            False,
            ClientResponseError(
                RequestInfo(url="", method="", headers={}, real_url=""),
                history=(),
                message="client error",
                status=429,
            ),
        ),
    ],
)
async def test_create_check(
    setup_catalog,
    client,
    rmock,
    event_loop,
    db,
    resource,
    analysis_mock,
    udata_url,
    api_headers,
    api_headers_wrong_token,
):
    resource_id, resource_status, resource_timeout, resource_exception = resource
    rurl = RESOURCE_URL
    params = {
        "status": resource_status,
        "headers": {"Content-LENGTH": "10", "X-Do": "you"},
        "exception": resource_exception,
    }
    rmock.head(rurl, **params)
    # mock for head fallback
    rmock.get(rurl, **params)
    rmock.put(udata_url)

    # Test API call with no token
    resp = await client.post("/api/checks/", json={"resource_id": resource_id})
    assert resp.status == 401

    # Test API call with invalid token
    resp = await client.post(
        "/api/checks/", headers=api_headers_wrong_token, json={"resource_id": resource_id}
    )
    assert resp.status == 403

    # Test the API responses cases
    api_response = await client.post(
        "/api/checks/", headers=api_headers, json={"resource_id": resource_id}
    )
    assert api_response.status == 201
    data: dict = await api_response.json()
    assert data["resource_id"] == resource_id
    assert data["url"] == rurl
    assert data["status"] == resource_status
    assert data["timeout"] == resource_timeout

    # Test check results in DB
    res = await db.fetchrow("SELECT * FROM checks WHERE url = $1", rurl)
    assert res["url"] == rurl
    assert res["status"] == resource_status
    if not resource_exception:
        assert json.loads(res["headers"]) == {
            "x-do": "you",
            # added by aioresponses :shrug:
            "content-type": "application/json",
            "content-length": "10",
        }
    assert res["timeout"] == resource_timeout
    if isinstance(resource_exception, ClientError):
        assert res["error"] == "client error"
    elif resource_status == 500:
        assert res["error"] == "Internal Server Error"
    else:
        assert not res["error"]

    # Test webhook results from mock
    webhook = rmock.requests[("PUT", URL(udata_url))][0].kwargs["json"]
    assert webhook.get("check:date")
    datetime.fromisoformat(webhook["check:date"])
    if resource_exception or resource_status == 500:
        if resource_status == 429:
            # In the case of a 429 status code, the error is on the crawler side and we can't give an availability status.
            # We expect check:available to be None.
            assert webhook.get("check:available") is None
        else:
            assert webhook.get("check:available") is False
    else:
        assert webhook.get("check:available")
        assert webhook.get("check:headers:content-type") == "application/json"
        assert webhook.get("check:headers:content-length") == 10
    if resource_timeout:
        assert webhook.get("check:timeout")
    else:
        assert webhook.get("check:timeout") is False
