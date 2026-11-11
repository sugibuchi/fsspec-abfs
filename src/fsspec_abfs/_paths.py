"""The accounts and paths of Azure Data Lake Storage Gen2.

A path of the filesystem is ``/<container>/<path in container>``. The root,
"/", holds the containers. `split_path` gives a path's container and its path
within it, without a leading "/": the blob API takes names so, and the Data Lake
API answers differently to some requests when a leading "/" is given.

An account is its name and its domain: Azure's, ``core.windows.net``, or
Fabric's OneLake, ``fabric.microsoft.com``. Both endpoints of an account name
it, as they serve the same storage: ``https://<account>.dfs.<domain>`` and
``https://<account>.blob.<domain>``.
"""

from __future__ import annotations

import re
from typing import Literal

__all__ = [
    "DEFAULT_DOMAIN",
    "AccountDomain",
    "account_of",
    "check_account",
    "parse_account_url",
    "split_path",
    "strip_protocol",
]

AccountDomain = Literal["core.windows.net", "fabric.microsoft.com"]
"""The domains of the accounts the filesystem takes."""

DEFAULT_DOMAIN: AccountDomain = "core.windows.net"
"""The domain of an account given by its name alone: Azure's."""

_DOMAINS = r"core\.windows\.net|fabric\.microsoft\.com"

_ACCOUNT_NAME = re.compile(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?")

_ACCOUNT_URL = re.compile(
    rf"https://(?P<account>{_ACCOUNT_NAME.pattern})"
    rf"\.(?P<service>dfs|blob)\.(?P<domain>{_DOMAINS})/?",
    re.IGNORECASE,
)

# The Hadoop form, "abfss://<container>@<account>.dfs.<domain>/<path>", and the
# adlfs form, "abfs://<container>/<path>" or "az://<container>/<path>".
_URL = re.compile(
    rf"(?:abfss?|az)://(?P<container>[^/@]*)"
    rf"(?:@(?P<account>[a-z0-9-]+)\.(?:dfs|blob)\.(?P<domain>{_DOMAINS}))?"
    rf"(?P<path>/.*)?",
    re.IGNORECASE,
)


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
    return m.group("account").lower(), m.group("domain").lower()  # type: ignore[return-value]


def check_account(account_name: str, account_domain: str) -> tuple[str, AccountDomain]:
    """An account's name and domain, checked, the name in lower case as Azure takes it.

    Raises ValueError for a name Azure would refuse, or a domain of no account
    the filesystem takes.
    """
    name = account_name.strip().lower()
    if not _ACCOUNT_NAME.fullmatch(name):
        raise ValueError(f"Not an account name: {account_name!r}")
    domain = account_domain.strip().lower()
    if not re.fullmatch(_DOMAINS, domain):
        raise ValueError(
            "account_domain must be core.windows.net or fabric.microsoft.com: "
            f"{account_domain!r}"
        )
    return name, domain  # type: ignore[return-value]


def account_of(url: str) -> tuple[str, AccountDomain] | None:
    """The account a Hadoop-style URL names, e.g. from ``abfss://c@a.dfs.core.windows.net/p``, or None."""
    m = _URL.fullmatch(url.strip())
    if m is None or m.group("account") is None:
        return None
    else:
        return m.group("account").lower(), m.group("domain").lower()  # type: ignore[return-value]


def strip_protocol(path: str) -> str:
    """A path as the filesystem takes it: ``/<container>/<path>``, without protocol or account.

    The account of a Hadoop-style URL is left out here: the filesystem checks
    it is its own, see `AbfsFileSystem._strip_protocol`.
    """
    path = path.strip()
    m = _URL.fullmatch(path)
    if m is not None:
        path = m.group("container") + (m.group("path") or "")
    return "/" + path.strip("/")


def split_path(path: str) -> tuple[str, str]:
    """A stripped path's container and the path within it, each "" if none."""
    container, _, key = path.strip("/").partition("/")
    return container, key.strip("/")
