"""The accounts, URLs and paths of Azure Data Lake Storage Gen2.

A path of the filesystem is ``/<container>/<path in container>``. The root,
"/", holds the containers. `split_stripped` gives a stripped path's container
and its path within it, without a leading "/": the blob API takes names so, and
the Data Lake API answers differently to some requests when a leading "/" is
given.

An account is its name and its domain: Azure's, ``core.windows.net``, or
Fabric's OneLake, ``fabric.microsoft.com``. Both endpoints of an account name
it, as they serve the same storage: ``https://<account>.dfs.<domain>`` and
``https://<account>.blob.<domain>``.

`strip_protocol` takes the URLs adlfs takes:

- Hadoop's, ``abfs[s]://<container>@<account>.dfs.<domain>/<path>``;
- adlfs' own, ``az://``, ``abfs://`` or ``abfss://<container>/<path>``;
- both without their protocol, ``<container>@<account>.dfs.<domain>/<path>`` and
  ``<container>/<path>``, or ``/<container>/<path>``;
- an endpoint's, ``https://<account>.dfs.<domain>/<container>/<path>``, or of
  the blob endpoint.

`hadoop_url` makes the first: the form Spark and Hadoop's ABFS driver take.
"""

from __future__ import annotations

import re
from typing import Literal

__all__ = [
    "DEFAULT_DOMAIN",
    "AccountDomain",
    "account_of",
    "check_account",
    "hadoop_url",
    "parse_account_url",
    "split_stripped",
    "strip_protocol",
]

AccountDomain = Literal["core.windows.net", "fabric.microsoft.com"]
"""The domains of the accounts the filesystem takes."""

DEFAULT_DOMAIN: AccountDomain = "core.windows.net"
"""The domain of an account given by its name alone: Azure's."""

# Account names are lower case letters and digits, container names lower case
# letters, digits and inner hyphens, as Azure names them.
_ACCOUNT = r"(?P<account>[0-9a-z]+)"
_DOMAIN = r"(?P<domain>core\.windows\.net|fabric\.microsoft\.com)"
_CONTAINER = r"(?P<container>[0-9a-z]+(?:-[0-9a-z]+)*)"
_PATH = r"(?P<path>/.*)?"

_ACCOUNT_URL = re.compile(
    rf"https://{_ACCOUNT}\.(?:dfs|blob)\.{_DOMAIN}/?", re.IGNORECASE
)
"""An account's endpoint, with no container: what ``account_url`` takes."""

_HADOOP_URL = re.compile(
    rf"(?:abfss?://)?{_CONTAINER}@{_ACCOUNT}\.dfs\.{_DOMAIN}{_PATH}", re.IGNORECASE
)
"""Hadoop's URL, with its protocol or without."""

_SERVICE_URL = re.compile(
    rf"https://{_ACCOUNT}\.(?:dfs|blob)\.{_DOMAIN}(?:/(?:{_CONTAINER}{_PATH})?)?",
    re.IGNORECASE,
)
"""A URL of an endpoint, with a container and a path in it, or without."""

_ADLFS_URL = re.compile(rf"(?:az|abfss?)://{_CONTAINER}?{_PATH}", re.IGNORECASE)
"""adlfs' URL, naming no account."""


def parse_account_url(account_url: str) -> tuple[str, AccountDomain]:
    """The name and domain of the account an endpoint's URL names, e.g. ("acc", "core.windows.net").

    Raises ValueError for any other URL, e.g. one with a path, as a container
    or a path given here would be ignored silently otherwise.
    """
    m = _ACCOUNT_URL.fullmatch(account_url.strip())
    if m is None:
        raise ValueError(
            "account_url must be an account's dfs or blob endpoint, e.g. "
            f"https://<account>.dfs.core.windows.net/: {account_url!r}"
        )
    else:
        return m.group("account").lower(), m.group("domain").lower()  # type: ignore[return-value]


def check_account(account_name: str, account_domain: str) -> tuple[str, AccountDomain]:
    """An account's name and domain, checked, the name in lower case as Azure takes it.

    Raises ValueError for a name Azure would refuse, or a domain of no account
    the filesystem takes.
    """
    name = account_name.strip().lower()
    domain = account_domain.strip().lower()
    if not re.fullmatch(_ACCOUNT, name):
        raise ValueError(f"Not an account name: {account_name!r}")
    elif not re.fullmatch(_DOMAIN, domain):
        raise ValueError(
            "account_domain must be core.windows.net or fabric.microsoft.com: "
            f"{account_domain!r}"
        )
    else:
        return name, domain  # type: ignore[return-value]


def _match(path: str) -> re.Match[str] | None:
    """The URL a path is, of any form `strip_protocol` takes, or None for a plain path."""
    for pattern in (_HADOOP_URL, _SERVICE_URL, _ADLFS_URL):
        m = pattern.fullmatch(path)
        if m is not None:
            return m
    return None


def account_of(url: str) -> tuple[str, AccountDomain] | None:
    """The account a URL names, as Hadoop's and the endpoints' do, or None."""
    m = _match(url.strip())
    if m is None or "account" not in m.re.groupindex:
        return None
    else:
        return m.group("account").lower(), m.group("domain").lower()  # type: ignore[return-value]


def strip_protocol(path: str) -> str:
    """A path as the filesystem takes it: ``/<container>/<path>``, without protocol or account.

    The account a URL names is left out here: the filesystem's operations check
    it is its own, see `AbfsFileSystem.split_path`.
    """
    path = path.strip()
    m = _match(path)
    if m is not None:
        path = (m.group("container") or "") + (m.group("path") or "")
    return "/" + path.strip("/")


def split_stripped(path: str) -> tuple[str, str]:
    """A stripped path's container and the path within it, each "" if none."""
    container, _, key = path.strip("/").partition("/")
    return container, key.strip("/")


def hadoop_url(container: str, key: str, account: str, domain: str) -> str:
    """The URL of a path in Hadoop's form, ``abfss://<container>@<account>.dfs.<domain>/<path>``.

    The root, which has no container, is the account's dfs endpoint, as Hadoop's
    form needs a container.
    """
    host = f"{account}.dfs.{domain}"
    if not container:
        return f"https://{host}/"
    else:
        return f"abfss://{container}@{host}/{key}"
