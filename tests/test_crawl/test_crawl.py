import hashlib
import json
import sys
import tempfile
from asyncio.exceptions import TimeoutError
from datetime import datetime, timedelta, timezone

import nest_asyncio
import pytest
from aiohttp import RequestInfo
from aiohttp.client_exceptions import ClientError, ClientResponseError
from aioresponses import CallbackResult
from asyncpg import Record
from dateparser import parse as date_parser
from humanfriendly import parse_timespan
from yarl import URL

from tests.conftest import RESOURCE_ID, RESOURCE_URL
from udata_hydra import config
from udata_hydra.analysis.resource import analyse_resource
from udata_hydra.crawl import start_checks
from udata_hydra.crawl.process_check_data import get_content_type_from_header
from udata_hydra.db.check import Check
from udata_hydra.db.resource import Resource

# TODO: make file content configurable
SIMPLE_CSV_CONTENT = """code_insee,number
95211,102
36522,48"""

pytestmark = pytest.mark.asyncio
# allows nested async to test async with async :mindblown:
nest_asyncio.apply()


async def mock_download_resource(url, headers, max_size_allowed):
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    tmp_file.write(SIMPLE_CSV_CONTENT.encode("utf-8"))
    tmp_file.close()
    return tmp_file


@pytest.mark.parametrize(
    "resource",
    [
        # status, timeout, exception
        (200, False, None),
        (500, False, None),
        (None, False, ClientError("client error")),
        (None, False, AssertionError),
        (None, False, UnicodeError),
        (None, True, TimeoutError),
        (
            429,
            False,
            ClientResponseError(
                RequestInfo(url="", method="", headers={}),
                history=(),
                message="client error",
                status=429,
            ),
        ),
    ],
)
async def test_crawl(setup_catalog, rmock, event_loop, db, resource, analysis_mock, udata_url):
    status, timeout, exception = resource
    rurl = RESOURCE_URL
    params = {
        "status": status,
        "headers": {"Content-LENGTH": "10", "X-Do": "you"},
        "exception": exception,
    }
    rmock.head(rurl, **params)
    # mock for head fallback
    rmock.get(rurl, **params)
    rmock.put(udata_url)
    event_loop.run_until_complete(start_checks(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests

    # test check results in DB
    res = await db.fetchrow("SELECT * FROM checks WHERE url = $1", rurl)
    assert res["url"] == rurl
    assert res["status"] == status
    if not exception:
        assert json.loads(res["headers"]) == {
            "x-do": "you",
            # added by aioresponses :shrug:
            "content-type": "application/json",
            "content-length": "10",
        }
    assert res["timeout"] == timeout
    if isinstance(exception, ClientError):
        assert res["error"] == "client error"
    elif status == 500:
        assert res["error"] == "Internal Server Error"
    else:
        assert not res["error"]

    # test webhook results from mock
    webhook = rmock.requests[("PUT", URL(udata_url))][0].kwargs["json"]
    assert webhook.get("check:date")
    datetime.fromisoformat(webhook["check:date"])
    if exception or status == 500:
        if status == 429:
            # In the case of a 429 status code, the error is on the crawler side and we can't give an availability status.
            # We expect check:available to be None.
            assert webhook.get("check:available") is None
        else:
            assert webhook.get("check:available") is False
    else:
        assert webhook.get("check:available")
        assert webhook.get("check:headers:content-type") == "application/json"
        assert webhook.get("check:headers:content-length") == 10
    if timeout:
        assert webhook.get("check:timeout")
    else:
        assert webhook.get("check:timeout") is False


async def test_excluded_clause(setup_catalog, mocker, event_loop, rmock, produce_mock):
    mocker.patch("udata_hydra.config.SLEEP_BETWEEN_BATCHES", 0)
    mocker.patch("udata_hydra.config.EXCLUDED_PATTERNS", ["http%example%"])
    rurl = RESOURCE_URL
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(start_checks(iterations=1))
    # url has not been called due to excluded clause
    assert ("GET", URL(rurl)) not in rmock.requests


async def test_outdated_check(setup_catalog, rmock, fake_check, event_loop, produce_mock):
    await fake_check(created_at=datetime.now() - timedelta(weeks=52))
    rurl = RESOURCE_URL
    rmock.head(rurl, status=200)
    event_loop.run_until_complete(start_checks(iterations=1))
    # url has been called because check is outdated
    assert ("HEAD", URL(rurl)) in rmock.requests


async def test_not_outdated_check(
    setup_catalog, rmock, fake_check, event_loop, mocker, produce_mock
):
    mocker.patch("udata_hydra.config.SLEEP_BETWEEN_BATCHES", 0)
    await fake_check()
    rurl = RESOURCE_URL
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(start_checks(iterations=1))
    # url has not been called because check is fresh
    assert ("GET", URL(rurl)) not in rmock.requests


async def test_switch_head_to_get(setup_catalog, event_loop, rmock, produce_mock):
    rurl = RESOURCE_URL
    rmock.head(rurl, status=501)
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(start_checks(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests
    assert ("GET", URL(rurl)) in rmock.requests


async def test_switch_head_to_get_headers(setup_catalog, event_loop, rmock, produce_mock):
    rurl = RESOURCE_URL
    rmock.head(rurl, status=200, headers={})
    rmock.get(rurl, status=200)
    event_loop.run_until_complete(start_checks(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests
    assert ("GET", URL(rurl)) in rmock.requests


async def test_no_switch_head_to_get(setup_catalog, event_loop, rmock, produce_mock, analysis_mock):
    rurl = RESOURCE_URL
    rmock.head(rurl, status=200, headers={"content-length": "1"})
    event_loop.run_until_complete(start_checks(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests
    assert ("GET", URL(rurl)) not in rmock.requests


async def test_analyse_resource(setup_catalog, mocker, fake_check):
    mocker.patch("udata_hydra.analysis.resource.download_resource", mock_download_resource)
    # disable webhook, tested in following test
    mocker.patch("udata_hydra.config.WEBHOOK_ENABLED", False)

    check = await fake_check()
    await analyse_resource(check["id"], False)
    result: Record | None = await Check.get_by_id(check["id"])

    assert result["error"] is None
    assert result["checksum"] == hashlib.sha1(SIMPLE_CSV_CONTENT.encode("utf-8")).hexdigest()
    assert result["filesize"] == len(SIMPLE_CSV_CONTENT)
    assert result["mime_type"] == "text/plain"


async def test_analyse_resource_send_udata(setup_catalog, mocker, rmock, fake_check, udata_url):
    mocker.patch("udata_hydra.analysis.resource.download_resource", mock_download_resource)
    rmock.put(udata_url, status=200, repeat=True)

    check = await fake_check()
    await analyse_resource(check["id"], True)

    req = rmock.requests[("PUT", URL(udata_url))]
    assert len(req) == 1
    document = req[0].kwargs["json"]
    assert document["analysis:content-length"] == len(SIMPLE_CSV_CONTENT)
    assert document["analysis:mime-type"] == "text/plain"


async def test_analyse_resource_send_udata_no_change(
    setup_catalog, mocker, rmock, fake_check, udata_url
):
    mocker.patch("udata_hydra.analysis.resource.download_resource", mock_download_resource)
    rmock.put(udata_url, status=200, repeat=True)

    # previous check with same checksum
    await fake_check(checksum=hashlib.sha1(SIMPLE_CSV_CONTENT.encode("utf-8")).hexdigest())
    check = await fake_check()
    await analyse_resource(check["id"], False)

    # udata has not been called
    assert ("PUT", URL(udata_url)) not in rmock.requests


async def test_analyse_resource_from_crawl(setup_catalog, rmock, event_loop, db, udata_url):
    """
    Looks a lot like an E2E test:
    - process catalog
    - check resource
    - download and analysis resource
    - trigger udata callbacks
    """

    rurl = RESOURCE_URL

    # mock for check
    rmock.head(rurl, status=200, headers={"Content-Length": "200"})
    # mock for download
    rmock.get(rurl, status=200, body=SIMPLE_CSV_CONTENT.encode("utf-8"))
    # mock for check and analysis results
    rmock.put(udata_url, status=200, repeat=True)

    event_loop.run_until_complete(start_checks(iterations=1))

    assert len(rmock.requests[("PUT", URL(udata_url))]) == 2
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 1
    assert res[0]["url"] == rurl
    assert res[0]["checksum"] is not None
    assert res[0]["status"] is not None


async def test_change_analysis_last_modified_header(setup_catalog, rmock, event_loop, udata_url):
    rmock.head(RESOURCE_URL, headers={"last-modified": "Thu, 09 Jan 2020 09:33:37 GMT"})
    rmock.get(RESOURCE_URL)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(start_checks(iterations=1))
    requests = rmock.requests[("PUT", URL(udata_url))]
    # last request is the one for analysis
    data = requests[-1].kwargs["json"]
    assert data["analysis:last-modified-at"] == "2020-01-09T09:33:37+00:00"
    assert data["analysis:last-modified-detection"] == "last-modified-header"


async def test_change_analysis_content_length_header(
    setup_catalog, rmock, event_loop, fake_check, db, udata_url
):
    # different content-length than mock response
    await fake_check(headers={"content-length": "1"})
    # force check execution at next run
    await db.execute("UPDATE catalog SET priority = TRUE WHERE resource_id = $1", RESOURCE_ID)
    rmock.head(RESOURCE_URL, headers={"content-length": "2"})
    rmock.get(RESOURCE_URL)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(start_checks(iterations=1))
    requests = rmock.requests[("PUT", URL(udata_url))]
    # last request is the one for analysis
    data = requests[-1].kwargs["json"]
    modified_date = datetime.fromisoformat(data["analysis:last-modified-at"])
    now = datetime.now(timezone.utc)
    # modified date should be pretty close from now, let's say 30 seconds
    assert (modified_date - now).total_seconds() < 30
    assert data["analysis:last-modified-detection"] == "content-length-header"


async def test_change_analysis_checksum(
    setup_catalog, mocker, fake_check, db, rmock, event_loop, udata_url
):
    # different checksum than mock file
    await fake_check(
        created_at=datetime.now() - timedelta(days=10),
        checksum="136bd31d53340d234957650e042172705bf32984",
    )
    mocker.patch("udata_hydra.analysis.resource.download_resource", mock_download_resource)
    rmock.head(RESOURCE_URL)
    rmock.get(RESOURCE_URL)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(start_checks(iterations=1))
    requests = rmock.requests[("PUT", URL(udata_url))]
    # last request is the one for analysis
    data = requests[-1].kwargs["json"]
    modified_date = datetime.fromisoformat(data["analysis:last-modified-at"])
    now = datetime.now(timezone.utc)
    # modified date should be pretty close from now, let's say 30 seconds
    assert (modified_date - now).total_seconds() < 30
    assert data["analysis:last-modified-detection"] == "computed-checksum"


@pytest.mark.catalog_harvested
async def test_change_analysis_harvested(
    setup_catalog, mocker, rmock, fake_check, db, event_loop, udata_url
):
    await fake_check(detected_last_modified_at=datetime.now() - timedelta(days=10))
    # force check execution at next run
    await db.execute("UPDATE catalog SET priority = TRUE WHERE resource_id = $1", RESOURCE_ID)
    mocker.patch("udata_hydra.analysis.resource.download_resource", mock_download_resource)
    rmock.head("https://example.com/harvested", headers={"content-length": "2"}, repeat=True)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(start_checks(iterations=1))
    requests = rmock.requests[("PUT", URL(udata_url))]
    # last request is the one for analysis
    data = requests[-1].kwargs["json"]
    assert data["analysis:last-modified-at"] == "2022-12-06T05:00:32.647000+00:00"
    assert data["analysis:last-modified-detection"] == "harvest-resource-metadata"


@pytest.mark.catalog_harvested
async def test_no_change_analysis_harvested(
    setup_catalog, mocker, rmock, fake_check, db, event_loop, udata_url
):
    last_modfied_at = datetime.fromisoformat("2022-12-06T05:00:32.647000").replace(
        tzinfo=timezone.utc
    )
    await fake_check(
        headers={"content-type": "application/json"},
        created_at=datetime.now() - timedelta(days=10),
        detected_last_modified_at=last_modfied_at,
    )  # same date as harvest.modified_at in catalog
    rmock.head("https://example.com/harvested", headers={"content-type": "application/json"})
    rmock.get("https://example.com/harvested")
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(start_checks(iterations=1))
    assert ("PUT", URL(udata_url)) not in rmock.requests


@pytest.mark.parametrize(
    "re_check",
    [
        # days since last check, re-check
        (6, False),
        (8, True),
    ],
)
async def test_re_check_depending_on_default_delay(
    setup_catalog, rmock, event_loop, db, analysis_mock, udata_url, fake_check, re_check
):
    days_since_last_check, re_check_expected = re_check
    previous_check_date: datetime = datetime.now() - timedelta(days=days_since_last_check)
    await fake_check(created_at=previous_check_date, detected_last_modified_at=None)

    # Run the checker
    rmock.head(RESOURCE_URL, status=200)
    rmock.get(RESOURCE_URL, status=200)
    rmock.put(udata_url)
    event_loop.run_until_complete(start_checks(iterations=1))

    # Another check should/shouldn't have been created depending on the delay
    checks: list[Record] | None = await db.fetch(
        "SELECT * FROM checks WHERE url = $1", RESOURCE_URL
    )
    if re_check_expected:
        assert ("HEAD", URL(RESOURCE_URL)) in rmock.requests
        assert len(checks) == 2
        assert checks[-1]["url"] == RESOURCE_URL
    else:
        assert ("HEAD", URL(RESOURCE_URL)) not in rmock.requests
        assert len(checks) == 1


@pytest.mark.parametrize(
    "re_check",
    [
        # days since last check, days since last modified, re-check
        (0.5, 0.5, False),
        (1, 1, False),
        (1, 2, True),
        # TODO: add more cases
    ],
)
async def test_re_check_depending_on_variable_delays(
    setup_catalog, rmock, event_loop, db, analysis_mock, udata_url, fake_check, re_check
):
    days_after_last_check, days_since_last_modified, re_check_expected = re_check
    previous_check_date: datetime = datetime.now() - timedelta(days=days_after_last_check)
    previous_check_last_modified: datetime = datetime.now() - timedelta(
        days=days_since_last_modified
    )
    await fake_check(
        created_at=previous_check_date,
        detected_last_modified_at=previous_check_last_modified,
    )

    # Run the checker
    rmock.head(RESOURCE_URL, status=200)
    rmock.get(RESOURCE_URL, status=200)
    rmock.put(udata_url)
    event_loop.run_until_complete(start_checks(iterations=1))

    # Another check should/shouldn't have been created depending on the delay
    checks: list[Record] | None = await db.fetch(
        "SELECT * FROM checks WHERE url = $1", RESOURCE_URL
    )
    if re_check_expected:
        assert ("HEAD", URL(RESOURCE_URL)) in rmock.requests
        assert len(checks) == 2
        assert checks[-1]["url"] == RESOURCE_URL
    else:
        assert ("HEAD", URL(RESOURCE_URL)) not in rmock.requests
        assert len(checks) == 1


async def test_change_analysis_last_modified_header_twice(
    setup_catalog, rmock, event_loop, fake_check, udata_url
):
    _date = "Thu, 09 Jan 2020 09:33:37 GMT"
    await fake_check(
        headers={"last-modified": _date, "content-type": "application/json"},
        created_at=datetime.now() - timedelta(days=10),
    )
    rmock.head(
        RESOURCE_URL,
        headers={"last-modified": _date, "content-type": "application/json"},
    )
    rmock.get(RESOURCE_URL)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(start_checks(iterations=1))
    # udata has not been called: not first check, outdated check, and last-modified stayed the same
    assert ("PUT", URL(udata_url)) not in rmock.requests


async def test_change_analysis_last_modified_header_twice_tz(
    setup_catalog, rmock, event_loop, fake_check, udata_url
):
    _date_1 = "Thu, 09 Jan 2020 09:33:37 GMT+1"
    _date_2 = "Thu, 09 Jan 2020 09:33:37 GMT+4"
    await fake_check(
        detected_last_modified_at=date_parser(_date_1),
        created_at=datetime.now() - timedelta(days=10),
        headers={"content-type": "application/json"},
    )
    rmock.head(
        RESOURCE_URL,
        headers={"last-modified": _date_2, "content-type": "application/json"},
    )
    rmock.get(RESOURCE_URL)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(start_checks(iterations=1))
    # udata has been called: last-modified has changed (different timezones)
    assert ("PUT", URL(udata_url)) in rmock.requests
    webhook = rmock.requests[("PUT", URL(udata_url))][0].kwargs["json"]
    assert webhook.get("analysis:last-modified-at") == date_parser(_date_2).isoformat()


async def test_check_changed_content_length_header(
    setup_catalog, rmock, event_loop, fake_check, udata_url
):
    await fake_check(
        created_at=datetime.now() - timedelta(days=10),
        headers={"content-type": "application/json", "content-length": "10"},
    )
    rmock.head(
        RESOURCE_URL,
        headers={"content-length": "15", "content-type": "application/json"},
    )
    rmock.get(RESOURCE_URL)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(start_checks(iterations=1))
    # udata has been called in compute_check_has_changed: content-length has changed
    assert ("PUT", URL(udata_url)) in rmock.requests
    webhook = rmock.requests[("PUT", URL(udata_url))][0].kwargs["json"]
    assert webhook.get("check:headers:content-length") == 15


async def test_no_check_changed_content_length_header(
    setup_catalog, rmock, event_loop, fake_check, udata_url
):
    await fake_check(
        created_at=datetime.now() - timedelta(days=10),
        headers={"content-type": "application/json", "content-length": "10"},
    )
    rmock.head(
        RESOURCE_URL,
        headers={"content-length": "10", "content-type": "application/json"},
    )
    rmock.get(RESOURCE_URL)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(start_checks(iterations=1))
    # udata has not been called: not first check, outdated check, and content-length stayed the same
    assert ("PUT", URL(udata_url)) not in rmock.requests


async def test_check_changed_content_type_header(
    setup_catalog, rmock, event_loop, fake_check, udata_url
):
    await fake_check(
        created_at=datetime.now() - timedelta(days=10),
        headers={"content-type": "application/json", "content-length": "10"},
    )
    rmock.head(
        RESOURCE_URL,
        headers={"content-length": "10", "content-type": "text/csv"},
    )
    rmock.get(RESOURCE_URL)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(start_checks(iterations=1))
    # udata has been called in compute_check_has_changed: content-type has changed
    assert ("PUT", URL(udata_url)) in rmock.requests
    webhook = rmock.requests[("PUT", URL(udata_url))][0].kwargs["json"]
    assert webhook.get("check:headers:content-type") == "text/csv"


async def test_no_check_changed_content_type_header(
    setup_catalog, rmock, event_loop, fake_check, udata_url
):
    await fake_check(
        created_at=datetime.now() - timedelta(days=10),
        headers={"content-type": "application/json", "content-length": "10"},
    )
    rmock.head(
        RESOURCE_URL,
        headers={"content-length": "10", "content-type": "application/json"},
    )
    rmock.get(RESOURCE_URL)
    rmock.put(udata_url, repeat=True)
    event_loop.run_until_complete(start_checks(iterations=1))
    # udata has not been called: not first check, outdated check, and content-type stayed the same
    assert ("PUT", URL(udata_url)) not in rmock.requests


async def test_crawl_and_analysis_user_agent(setup_catalog, rmock, event_loop, produce_mock):
    # very complicated stuff, thanks https://github.com/pnuckowski/aioresponses/issues/111#issuecomment-896585061
    def callback(url, **kwargs):
        assert config.USER_AGENT == sys._getframe(3).f_locals["orig_self"].headers["user-agent"]
        # add content-length to avoid switching from HEAD to GET when crawling
        return CallbackResult(status=200, payload={}, headers={"content-length": "1"})

    rurl = RESOURCE_URL
    rmock.head(rurl, callback=callback)
    rmock.get(rurl, callback=callback)
    event_loop.run_until_complete(start_checks(iterations=1))


async def test_check_triggered_by_udata_entrypoint_clean_catalog(
    client,
    udata_resource_payload,
    event_loop,
    db,
    rmock,
    analysis_mock,
    clean_db,
    produce_mock,
    api_headers,
):
    rurl = udata_resource_payload["document"]["url"]
    rmock.head(rurl, headers={"content-length": "1"})
    res = await client.post(path="/api/resources", headers=api_headers, json=udata_resource_payload)
    assert res.status == 201
    res = await db.fetch("SELECT * FROM catalog")
    assert len(res) == 1
    event_loop.run_until_complete(start_checks(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 1


async def test_check_triggered_by_udata_entrypoint_existing_catalog(
    setup_catalog,
    client,
    udata_resource_payload,
    event_loop,
    db,
    rmock,
    analysis_mock,
    produce_mock,
    api_headers,
):
    rurl = udata_resource_payload["document"]["url"]
    rmock.head(rurl, headers={"content-length": "1"})
    res = await client.post(path="/api/resources", headers=api_headers, json=udata_resource_payload)
    assert res.status == 201
    res = await db.fetch("SELECT * FROM catalog")
    assert len(res) == 2
    event_loop.run_until_complete(start_checks(iterations=1))
    assert ("HEAD", URL(rurl)) in rmock.requests
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 2


async def test_check_triggers_csv_analysis(rmock, event_loop, db, produce_mock, setup_catalog):
    """Crawl a CSV file, analyse and apify it, downloads only once"""
    rurl = RESOURCE_URL
    # mock for check
    rmock.head(rurl, status=200, headers={"content-length": "1", "content-type": "application/csv"})
    # mock for analysis download
    rmock.get(
        rurl,
        status=200,
        headers={"content-type": "application/csv"},
        body=SIMPLE_CSV_CONTENT.encode("utf-8"),
    )
    event_loop.run_until_complete(start_checks(iterations=1))
    # GET called only once: HEAD is ok (no need for crawl) and analysis steps share the downloaded file
    assert len(rmock.requests[("GET", URL(rurl))]) == 1
    res = await db.fetch("SELECT * FROM checks")
    assert len(res) == 1
    assert res[0]["parsing_table"] is not None
    res = await db.fetch(f'SELECT * FROM "{res[0]["parsing_table"]}"')
    assert len(res) == 2


async def test_recheck_download_only_once(
    rmock, fake_check, event_loop, db, produce_mock, setup_catalog
):
    """On recheck of a (CSV) file, if it hasn't change, downloads only once"""
    await fake_check(
        resource_id=RESOURCE_ID, headers={"last-modified": "Thu, 09 Jan 2020 09:33:37 GMT"}
    )
    rurl = RESOURCE_URL
    # mock for check, with same last-modified header
    rmock.head(
        rurl,
        status=200,
        headers={
            "last-modified": "Thu, 09 Jan 2020 09:33:37 GMT",
            "content-type": "application/csv",
        },
    )
    await db.execute("UPDATE catalog SET priority = TRUE WHERE resource_id = $1", RESOURCE_ID)
    event_loop.run_until_complete(start_checks(iterations=1))

    # HEAD should have been called
    assert len(rmock.requests[("HEAD", URL(rurl))]) == 1

    # GET shouldn't have been called
    assert ("GET", URL(rurl)) not in rmock.requests


@pytest.mark.parametrize(
    "content_type",
    [
        # (content type header, parsed content type)
        ("application/json", "application/json"),
        ("text/html; charset=utf-8", "text/html"),
        ("text/html;h5ai=0.20;charset=UTF-8", "text/html"),
    ],
)
async def test_content_type_from_header(content_type):
    content_type_header, parsed_content_type = content_type
    assert parsed_content_type == await get_content_type_from_header(
        {"content-type": content_type_header}
    )


@pytest.mark.parametrize("resource_status", list(Resource.STATUSES.keys()) + [None])
async def test_dont_check_resources_with_status(
    rmock, event_loop, db, produce_mock, setup_catalog, resource_status
):
    await Resource.update(resource_id=RESOURCE_ID, data={"status": resource_status})
    rurl = RESOURCE_URL
    event_loop.run_until_complete(start_checks(iterations=1))

    if resource_status == "BACKOFF" or resource_status is None:
        # HEAD should have been called
        assert ("HEAD", URL(rurl)) in rmock.requests

        # Status should have been reset to None
        resource: dict = await db.fetchrow(
            "SELECT status FROM catalog WHERE resource_id = $1", RESOURCE_ID
        )
        assert resource["status"] is None

    else:
        # Don't check urls that have a status state pending

        # HEAD shouldn't have been called
        assert ("HEAD", URL(rurl)) not in rmock.requests
        # GET shouldn't have been called
        assert ("GET", URL(rurl)) not in rmock.requests

        # Status should have stayed the same
        resource: dict = await db.fetchrow(
            "SELECT status FROM catalog WHERE resource_id = $1", RESOURCE_ID
        )
        assert resource["status"] == resource_status
