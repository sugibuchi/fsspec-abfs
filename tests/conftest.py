"""Fixtures and options of the tests.

`--api` chooses where the filesystem tests run: on the dummy server, on live accounts, or both. `--hns` chooses the
kinds of account: with a hierarchical namespace, without, or both. Without them, `FSSPEC_ABFS_TEST_API` and
`FSSPEC_ABFS_TEST_HNS` choose, from the environment or `.env`. A test taking `make_fs`, `fs` or `root` runs once per
API and kind chosen.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import threading
import uuid
from typing import TYPE_CHECKING, Any

import pytest
from dummy_api import dummy_storage, serve_app

from fsspec_abfs.abfs import AbfsFileSystem

if TYPE_CHECKING:
    from azure.core.exceptions import HttpResponseError

_APIS = {"dummy": ["dummy"], "live": ["live"], "both": ["dummy", "live"]}
_HNS = {"true": [True], "false": [False], "both": [True, False]}
_LIVE_URLS = {
    True: "FSSPEC_ABFS_TEST_HNS_ACCOUNT_URL",
    False: "FSSPEC_ABFS_TEST_FLAT_ACCOUNT_URL",
}

DUMMY_CONTAINER = "c"
"""The container that the dummy account holds for each test."""

_RUN = uuid.uuid4().hex[:8]


def pytest_addoption(parser: pytest.Parser) -> None:
    # Without the options, the environment chooses, .env included: read when
    # used, as .env is loaded after this.
    parser.addoption(
        "--api",
        choices=sorted(_APIS),
        default=None,
        help="Where the filesystem tests run: the dummy server, live accounts, or both. "
        "Default: $FSSPEC_ABFS_TEST_API, else dummy.",
    )
    parser.addoption(
        "--hns",
        choices=sorted(_HNS),
        default=None,
        help="The kinds of account: with a hierarchical namespace (true), without (false), or both. "
        "Default: $FSSPEC_ABFS_TEST_HNS, else both.",
    )


def pytest_configure(config: pytest.Config) -> None:
    from dotenv import load_dotenv  # noqa: PLC0415

    load_dotenv()
    for marker in (
        "dummy_only: runs on the dummy server only",
        "live_only: runs on live accounts only",
        "hns_only: runs on an account with a hierarchical namespace only",
        "flat_only: runs on an account without a hierarchical namespace only",
    ):
        config.addinivalue_line("markers", marker)


def _choice(
    config: pytest.Config, option: str, variable: str, choices: dict, default: str
) -> str:
    chosen = config.getoption(option) or os.environ.get(variable) or default
    chosen = chosen.lower()
    if chosen not in choices:
        raise pytest.UsageError(
            f"Unknown {variable}: {chosen}, not one of {', '.join(sorted(choices))}"
        )
    return chosen


def _parametrize(
    metafunc: pytest.Metafunc, name: str, chosen: list, only: Any, ids: Any
) -> None:
    if only is not None and only not in chosen:
        reason = f"{ids(only)} only, not chosen by the options"
        values = [pytest.param(only, marks=pytest.mark.skip(reason=reason))]
    else:
        values = [only] if only is not None else chosen
    metafunc.parametrize(name, values, ids=ids, scope="session")


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parametrize each filesystem test by its APIs and kinds of account, both shown in its id."""
    marked = metafunc.definition.get_closest_marker
    if "api" in metafunc.fixturenames:
        only = None
        if marked("live_only"):
            only = "live"
        if marked("dummy_only") or "dummy" in metafunc.fixturenames:
            only = "dummy"
        chosen = _APIS[
            _choice(metafunc.config, "--api", "FSSPEC_ABFS_TEST_API", _APIS, "dummy")
        ]
        _parametrize(metafunc, "api", chosen, only, str)
    if "hns" in metafunc.fixturenames:
        only = True if marked("hns_only") else False if marked("flat_only") else None
        chosen = _HNS[
            _choice(metafunc.config, "--hns", "FSSPEC_ABFS_TEST_HNS", _HNS, "both")
        ]
        _parametrize(metafunc, "hns", chosen, only, lambda v: "hns" if v else "flat")


# The dummy server.


@pytest.fixture(scope="session")
def server_event_loop():
    """An event loop in a thread of its own, which the dummy server runs on."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, name="dummy-server", daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5.0)


@pytest.fixture(scope="session")
def dummy_api(server_event_loop):
    """The base URL of the dummy server."""
    yield from serve_app(server_event_loop)


@pytest.fixture(scope="session")
def dummy_endpoints(dummy_api: str) -> tuple[str, str]:
    """The dummy account's dfs and blob URLs, set on a file system before its first request."""
    return f"{dummy_api}/dfs", f"{dummy_api}/blob"


@pytest.fixture
def dummy(api: str, hns: bool, dummy_api: str):
    """The dummy account, with a container ``c``, its switches, and the requests it served, reset after the test.

    A test taking it runs on the dummy server only.
    """
    storage = dummy_storage()
    storage.reset(hns=hns)
    storage.create_container(DUMMY_CONTAINER)
    try:
        yield storage
    finally:
        storage.reset()


# Live accounts.

_live_checked: set[bool] = set()


def _live_settings(hns: bool) -> tuple[str, str]:
    url = os.environ.get(_LIVE_URLS[hns])
    container = os.environ.get("FSSPEC_ABFS_TEST_CONTAINER")
    if not (url and container):
        pytest.fail(
            f"Live tests asked for, but {_LIVE_URLS[hns]} or FSSPEC_ABFS_TEST_CONTAINER is not set",
            pytrace=False,
        )
    return url, container


# The filesystem.


@pytest.fixture
async def make_fs(api: str, hns: bool, request: pytest.FixtureRequest):
    """Make an `AbfsFileSystem` of the account the test runs on: async, uncached, with ``is_hns_enabled=hns``.

    Live tests are opt in, with `--api` or `FSSPEC_ABFS_TEST_API`. Once asked for, an account that cannot be reached
    is an error, not a skip that would pass unseen.
    """
    made: list[AbfsFileSystem] = []
    credential = None
    if api == "dummy":
        request.getfixturevalue("dummy")
        # An account by its name, as the constructor takes, with the dummy's endpoints.
        endpoints: tuple[str, str] | None = request.getfixturevalue("dummy_endpoints")
        account: dict[str, Any] = {"account_name": "dummy"}
    else:
        from azure.identity.aio import DefaultAzureCredential  # noqa: PLC0415

        url, container = _live_settings(hns)
        account = {"account_url": url}
        endpoints = None
        credential = DefaultAzureCredential()

    def make(**kwargs: Any) -> AbfsFileSystem:
        kwargs.setdefault("is_hns_enabled", hns)
        kwargs.setdefault("asynchronous", True)
        fs = AbfsFileSystem(
            credential=credential, skip_instance_cache=True, **account, **kwargs
        )
        if endpoints is not None:
            # The clients are made on these at first use: the dummy's, then.
            fs._dfs_url, fs._blob_url = endpoints
        made.append(fs)
        return fs

    try:
        if api == "live" and hns not in _live_checked:
            try:
                await make()._info(f"/{container}")
            except Exception as e:
                pytest.fail(
                    f"No live account at {url}, container {container}: {e!r}",
                    pytrace=False,
                )
            _live_checked.add(hns)
        yield make
    finally:
        loop = asyncio.get_running_loop()
        for fs in made:
            # close() only schedules the closing of clients on the running
            # loop, which may end first: they are closed here.
            if fs._client_loop is loop:
                for client in (fs._blob_service_client, fs._dfs_service_client):
                    await client.close()  # type: ignore[union-attr]
                fs._blob_service_client = fs._dfs_service_client = None
                fs._client_loop = None
            fs.close()
        if credential is not None:
            await credential.close()


@pytest.fixture
def fs(make_fs) -> AbfsFileSystem:
    return make_fs()


@pytest.fixture
async def root(api: str, hns: bool, make_fs) -> Any:
    """A folder of the test's own, not created: ``/c/work`` on the dummy server, deleted afterwards when live."""
    if api == "dummy":
        yield f"/{DUMMY_CONTAINER}/work"
        return
    _, container = _live_settings(hns)
    path = f"/{container}/fsspec-abfs-test-{_RUN}/{uuid.uuid4().hex[:8]}"
    try:
        yield path
    finally:
        with contextlib.suppress(FileNotFoundError):
            await make_fs()._rm(path, recursive=True)


# Unit tests.


def azure_error(
    cls: type[HttpResponseError], code: str | None, status: int
) -> Exception:
    e = cls(message=f"{code} at https://x.blob.core.windows.net/c?sig=SECRET")
    e.error_code = code
    e.status_code = status
    return e


@pytest.fixture(name="azure_error")
def azure_error_fixture():
    """`azure_error`, for tests, which never import conftest."""
    return azure_error
