import gzip
import hashlib
import logging
import tempfile
from typing import IO

import aiohttp
import magic

from udata_hydra import config
from udata_hydra.utils import IOException

log = logging.getLogger("udata-hydra")


def compute_checksum_from_file(filename: str) -> str:
    """Compute sha1 in blocks"""
    sha1sum = hashlib.sha1()
    with open(filename, "rb") as f:
        block = f.read(2**16)
        while len(block) != 0:
            sha1sum.update(block)
            block = f.read(2**16)
    return sha1sum.hexdigest()


def read_csv_gz(file_path: str) -> IO[bytes]:
    with gzip.open(file_path, "rb") as gz_file:
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
            temp_file.write(gz_file.read())
    return temp_file


async def download_resource(
    url: str,
    headers: dict,
    max_size_allowed: int | None,
) -> IO[bytes]:
    """
    Attempts downloading a resource from a given url.
    Returns the downloaded file object.
    Raises custom IOException if the resource is too large or if the URL is unreachable.
    """
    tmp_file = tempfile.NamedTemporaryFile(
        dir=config.TEMPORARY_DOWNLOAD_FOLDER or None, delete=False
    )

    if max_size_allowed is not None and float(headers.get("content-length", -1)) > max_size_allowed:
        raise IOException("File too large to download")

    chunk_size = 1024
    i = 0
    try:
        async with aiohttp.ClientSession(
            headers={"user-agent": config.USER_AGENT}, raise_for_status=True
        ) as session:
            async with session.get(url, allow_redirects=True) as response:
                async for chunk in response.content.iter_chunked(chunk_size):
                    if max_size_allowed is None or i * chunk_size < max_size_allowed:
                        tmp_file.write(chunk)
                    else:
                        tmp_file.close()
                        log.warning(f"File {url} is too big, skipping")
                        raise IOException("File too large to download", url=url)
                    i += 1
    except aiohttp.ClientResponseError as e:
        raise IOException("Error downloading CSV", url=url) from e
    finally:
        tmp_file.close()
        if magic.from_file(tmp_file.name, mime=True) in [
            "application/x-gzip",
            "application/gzip",
        ]:
            tmp_file = read_csv_gz(tmp_file.name)
        return tmp_file
