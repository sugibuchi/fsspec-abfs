"""The account URLs and paths of Azure Data Lake Storage Gen2.

A path of the filesystem is ``<container>/<path in container>``, without a
leading "/": the blob API takes names so, and the Data Lake API answers
differently to some requests when a leading "/" is given. The root, "", holds
the containers.

Both endpoints of an account are accepted, as they name the same storage:
``https://<account>.dfs.<domain>`` and ``https://<account>.blob.<domain>``,
where the domain is Azure's, ``core.windows.net``, or Fabric's OneLake,
``fabric.microsoft.com``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

__all__ = [
    "AccountEndpoints",
    "account_url_of",
    "parse_account_url",
    "split_path",
    "strip_protocol",
]

_DOMAINS = r"core\.windows\.net|fabric\.microsoft\.com"

_ACCOUNT_URL = re.compile(
    rf"https://(?P<account>[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)"
    rf"\.(?P<service>dfs|blob)\.(?P<domain>{_DOMAINS})/?",
    re.IGNORECASE,
)

# The Hadoop form, "abfss://<container>@<account>.dfs.<domain>/<path>", and the
# adlfs form, "abfs://<container>/<path>" or "az://<container>/<path>".
_URL = re.compile(
    rf"(?:abfss?|az)://(?P<container>[^/@]*)"
    rf"(?:@(?P<host>[a-z0-9-]+\.(?:dfs|blob)\.(?:{_DOMAINS})))?(?P<path>/.*)?",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class AccountEndpoints:
    """A storage account's name and the URLs of its two endpoints."""

    account_name: str
    dfs_url: str
    blob_url: str


def parse_account_url(account_url: str) -> AccountEndpoints:
    """The endpoints of the account an endpoint's URL names.

    Raises ValueError for any other URL, e.g. one with a path, as a container
    or a path given here would be ignored silently otherwise.
    """
    m = _ACCOUNT_URL.fullmatch(account_url.strip())
    if m is None:
        raise ValueError(
            "account_url must be an account's dfs or blob endpoint, e.g. "
            f"https://<account>.dfs.core.windows.net/: {account_url!r}"
        )
    account = m.group("account").lower()
    domain = m.group("domain").lower()
    return AccountEndpoints(
        account_name=account,
        dfs_url=f"https://{account}.dfs.{domain}",
        blob_url=f"https://{account}.blob.{domain}",
    )


def account_url_of(url: str) -> str | None:
    """The account URL a Hadoop-style URL names, e.g. from ``abfss://c@a.dfs.core.windows.net/p``."""
    m = _URL.fullmatch(url)
    if m is None or m.group("host") is None:
        return None
    return f"https://{m.group('host').lower()}"


def strip_protocol(path: str) -> str:
    """A path as the filesystem takes it: ``<container>/<path>``, without protocol or host.

    The account of a Hadoop-style URL is dropped: the filesystem is of one
    account, and fsspec strips a path before it may tell which.
    """
    path = path.strip()
    m = _URL.fullmatch(path)
    if m is not None:
        path = m.group("container") + (m.group("path") or "")
    return path.strip("/")


def split_path(path: str) -> tuple[str, str]:
    """A stripped path's container and the path within it, each "" if none."""
    container, _, key = path.partition("/")
    return container, key.strip("/")
