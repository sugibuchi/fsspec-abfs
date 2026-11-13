# Usage

For installation, see [README.md](README.md).

---

## Creating a file system

```python
from fsspec_abfs import AbfsFileSystem

fs = AbfsFileSystem(account_name="myaccount")
```

Or through fsspec, for the `abfs` and `abfss` protocols:

```python
import fsspec

fs = fsspec.filesystem("abfss", account_name="myaccount")
```

A file system works on one account. fsspec caches one per account, credential and options, however the account is
given.

Installing the package registers `AbfsFileSystem` for `abfs` and `abfss`, in place of adlfs. `use()` registers it
again, e.g. after another library registered its own. `use(cross_account=True)` registers
[`AbfsFileSystems`](#many-accounts-abfsfilesystems) instead, for URLs of any account:

```python
import fsspec
import fsspec_abfs

fsspec_abfs.use(cross_account=True)

fs, path = fsspec.url_to_fs("abfss://raw@myaccount.dfs.core.windows.net/data")  # AbfsFileSystems
```

adlfs registers `az` too. `use(adlfs_protocols=True)` registers this package for it as well, so that no URL goes to
adlfs. `AbfsFileSystems` then takes `az://` URLs naming their account only:
`az://<container>@<account>.dfs.<domain>/<path>`.

---

## Options

All are keyword arguments:

| Parameter               | Description                                                                         | Default              |
|-------------------------|-------------------------------------------------------------------------------------|----------------------|
| `account_name`          | The account's name, e.g. `myaccount`.                                               | `None`               |
| `account_domain`        | `"core.windows.net"` for Azure, or `"fabric.microsoft.com"` for Fabric's OneLake.   | `"core.windows.net"` |
| `account_url`           | The account's endpoint, in place of `account_name` and `account_domain`.            | `None`               |
| `credential`            | How to authenticate: see [Authentication](#authentication).                         | `"default"`          |
| `is_hns_enabled`        | Whether the account has a hierarchical namespace. `None` asks the account.          | `None`               |
| `operation_concurrency` | Maximum concurrent requests of a recursive operation, such as `rm` or `copy`.       | `100`                |
| `io_concurrency`        | Maximum concurrent range reads and block uploads, shared by all the files opened.   | `16`                 |

`account_url` is an endpoint such as `https://myaccount.dfs.core.windows.net/`, or the blob endpoint. For OneLake, it
is `https://onelake.dfs.fabric.microsoft.com/`. Give either `account_url`, or `account_name` with `account_domain`:
not both.

The other keyword arguments go to fsspec's `AsyncFileSystem`, e.g. `asynchronous=True`.

### The hierarchical namespace

The file system works the same on accounts with or without a hierarchical namespace, but some operations take fewer
requests with one. Unless `is_hns_enabled` says, the first operation needing to know asks the account information.
That needs a role on the account: a SAS token or a role of one container is refused it.

If the account does not answer, a warning is logged, and the file system takes the way right on both kinds of account.
Pass `is_hns_enabled` to save the request, or where the credential cannot ask.

---

## Logging

The library logs under the `fsspec_abfs` logger, and installs no handlers. A warning says when the hierarchical
namespace cannot be told.

Operations, such as `find`, `rm` or `copy`, are reported by the monitors of `fsspec_alt_strategies`, a dependency.
For example, `LoggingMonitor` logs the operations run in a `monitoring` block:

```python
import logging

from fsspec_alt_strategies import LoggingMonitor, monitoring

logger = logging.getLogger("my_app")

with monitoring(LoggingMonitor(logger, level=logging.INFO)):
    fs.rm("/mycontainer/tmp", recursive=True)
```

---

## Authentication

`credential` takes one of:

- an async token credential of `azure.identity.aio`, used as it is;
- a keyword naming one, made with the file system's options, e.g. `tenant_id`;
- the full name of an async token credential class, e.g. `"azure.identity.aio.AzureCliCredential"`;
- a SAS token, with or without its leading `?`;
- an account's shared key;
- `"anonymous"`, sending no credential, for public containers.

The default, `"default"`, is `DefaultAzureCredential`. `None` is refused.

| Keyword               | Credential                    |
|-----------------------|-------------------------------|
| `"default"`           | `DefaultAzureCredential`      |
| `"environment"`       | `EnvironmentCredential`       |
| `"azure-cli"`         | `AzureCliCredential`          |
| `"client-secret"`     | `ClientSecretCredential`      |
| `"certificate"`       | `CertificateCredential`       |
| `"managed-identity"`  | `ManagedIdentityCredential`   |
| `"workload-identity"` | `WorkloadIdentityCredential`  |

```python
from azure.identity.aio import DefaultAzureCredential

# A credential object: closing it is yours
fs = AbfsFileSystem(account_name="myaccount", credential=DefaultAzureCredential())

# A keyword, with the credential's own options
fs = AbfsFileSystem(
    account_name="myaccount",
    credential="client-secret",
    tenant_id="...",
    client_id="...",
    client_secret="...",
)

# A SAS token
fs = AbfsFileSystem(account_name="myaccount", credential="sv=...&sig=...")
```

A credential made from a keyword or a class name is closed with the file system. Sync credentials, of
`azure.identity` rather than `azure.identity.aio`, are refused: the file system's clients are async.

### Pickling, e.g. with Dask

Dask and other frameworks that run tasks in other processes pickle the file system with each task. A file system is
pickled as the arguments it was made with, `credential` and its options included. Unpickling calls the constructor
again. Nothing the file system made is pickled: not its clients, not the credential object it made from a keyword, and
not its event loop. Each worker makes its own when it first needs them, and fsspec's cache keeps one file system per
set of arguments in each worker.

So, on a cluster, **give the credential as a string**, a keyword or a class name, with its options:

```python
import dask.dataframe as dd

df = dd.read_parquet(
    "abfss://raw@myaccount.dfs.core.windows.net/data/table",
    storage_options={"credential": "managed-identity", "client_id": "..."},
)
```

- **Each worker authenticates on its own.** The identity must be available on every worker, not only on the client
  that builds the graph:
  - `"managed-identity"` or `"workload-identity"`: the identity of the VM, the container or the pod each worker runs in.
  - `"environment"`: `AZURE_CLIENT_ID` and the other variables `EnvironmentCredential` reads, set on each worker.
  - `"default"`: whichever of these the worker has.
  - `"azure-cli"` needs `az login` on each worker, which a cluster rarely has.
- **The options travel with every task.** A `client_secret`, a SAS token or a shared key given as an argument is in
  the pickled task graph, and in anything that logs or stores it. `"managed-identity"`, `"workload-identity"` and
  `"environment"` keep secrets out of the graph.
- **Avoid passing a credential object.** It is pickled as it is, with whatever it holds: its secrets, and once used,
  its tokens and its HTTP session, which may not pickle at all. Each worker gets a copy that it treats as the caller's,
  so the worker never closes it.
- `AbfsFileSystems` is pickled the same way: give its default credential, and those in `accounts`, as strings.

---

## Paths

A path is `/<container>/<path>`. The root, `/`, lists the account's containers.

These URLs name the same path:

| Form                 | Example                                                             |
|----------------------|---------------------------------------------------------------------|
| A path               | `/mycontainer/data/file.csv`                                        |
| Spark's and Hadoop's | `abfss://mycontainer@myaccount.dfs.core.windows.net/data/file.csv`  |
| adlfs'               | `abfs://mycontainer/data/file.csv`, or `az://`                      |
| An endpoint's        | `https://myaccount.dfs.core.windows.net/mycontainer/data/file.csv`  |

A URL naming another account raises `ValueError`. `fs.unstrip_protocol(path)` gives a path's URL in Spark's and
Hadoop's form.

---

## Common operations

All standard fsspec operations are supported.

```python
# List a folder
fs.ls("/mycontainer/data")

# List the containers
fs.ls("/")

# Check existence
fs.exists("/mycontainer/data/file.parquet")

# Get metadata
info = fs.info("/mycontainer/data/file.parquet")
print(info["size"], info["last_modified"], info["etag"])

# Create folders
fs.makedirs("/mycontainer/data/subdir", exist_ok=True)

# Copy a file
fs.copy("/mycontainer/src.csv", "/mycontainer/dst.csv")

# Move or rename a file or a folder
fs.mv("/mycontainer/old", "/mycontainer/new", recursive=True)

# Delete a folder
fs.rm("/mycontainer/data/subdir", recursive=True)

# Glob
files = fs.glob("/mycontainer/data/*.parquet")
```

Errors are standard exceptions: `FileNotFoundError`, `FileExistsError`, `IsADirectoryError`, `NotADirectoryError`,
`PermissionError`, or `OSError`.

### Signed URLs

`sign` gives a file's URL that reads it with no other credential, e.g. for a browser or another service:

```python
url = fs.sign("/mycontainer/data/file.csv", expiration=3600)  # seconds
# 'https://myaccount.blob.core.windows.net/mycontainer/data/file.csv?skoid=...&sp=r&sig=...'
```

The URL carries a SAS reading that file only. How it is signed depends on the credential:

| Credential       | The URL carries                                                          |
|------------------|--------------------------------------------------------------------------|
| Token credential | A user delegation SAS, for `expiration` seconds or until its key expires |
| Shared key       | A SAS signed by the key, for `expiration` seconds                        |
| SAS              | The credential's own SAS, valid as long as it is                         |
| Anonymous        | Nothing: the file must be public                                         |

A user delegation key is asked for once, valid for a day, and kept. A new one is asked for when the kept one has less
than 12 hours left. Asking for one needs a role at the account's level, e.g. Storage Blob Data Reader, Contributor or
Delegator: a role on a container only is refused. See
[Assign permissions with Azure RBAC](https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas#assign-permissions-with-rbac).

### Containers

`rmdir` deletes an empty container. The file system makes none: see [Limitations](#containers-1).

### With or without a hierarchical namespace

| Operation        | With a hierarchical namespace                     | Without one                                      |
|------------------|---------------------------------------------------|--------------------------------------------------|
| Folders          | Paths of their own: an empty folder exists        | Prefixes of the files under them                 |
| `makedirs`       | Creates the folder                                | Sends nothing: a folder exists by its files      |
| `mv`             | One atomic rename, of a file or a whole folder    | A copy on the server, then a delete, per file    |
| `rm` of a folder | One request for the folder with all in it         | One request per file                             |
| `ls`, `find`     | The Data Lake API's listings                      | The blob API's listings                          |

A rename is atomic, within a container or between two containers of the account.

Copies run on the server, with no data through this process. A copy still pending is waited for.

### Reading and writing files

```python
# Read a file
with fs.open("/mycontainer/data/file.bin", "rb") as f:
    data = f.read()

# Write a file
with fs.open("/mycontainer/data/output.bin", "wb") as f:
    f.write(b"hello")

# With pandas
import pandas as pd

df = pd.read_parquet("/mycontainer/data/table.parquet", filesystem=fs)
df.to_parquet("/mycontainer/data/out.parquet", filesystem=fs)
```

| Mode  | Supported | Notes                            |
|-------|-----------|----------------------------------|
| `rb`  | Yes       |                                  |
| `wb`  | Yes       |                                  |
| `xb`  | Yes       | Fails if the file already exists |

Reading:

- `cat_file` reads a file, or a range, in one request, with no metadata read first.
- A file opened reads ranges sized by how it is read, concurrently, limited by `io_concurrency`.
- Every range is of the version opened. A file replaced while it is read fails with an `OSError`.

Writing:

- A file under 8 MiB is uploaded in one request when it is closed.
- A larger one is uploaded in blocks of 4 MiB to 64 MiB, committed when it is closed.
- Nothing is visible before the file is closed. A file left by an error leaves any file at the path as it was.
- `xb` is checked by the request making the file visible, so that no other writer can slip in.

`AbfsFileSystem` is an async fsspec file system. Its `open_async()` returns files whose methods are awaited.

### Transactions

Files written in `with fs.transaction:` become visible when the block ends. If the block raises, they are discarded:

```python
with fs.transaction:
    with fs.open("/mycontainer/data/a.bin", "wb") as f:
        f.write(b"a")
    with fs.open("/mycontainer/data/b.bin", "wb") as f:
        f.write(b"b")
```

A file's blocks are uploaded as it is written, and committed when the block ends. A file under 8 MiB is held in
memory until then. See [Limitations](#transactions-1) for what a transaction does not cover.

---

## Many accounts: AbfsFileSystems

`AbfsFileSystems` works on any account, each URL on the account it names. It makes an `AbfsFileSystem` for each
account when first needed.

```python
from fsspec_abfs import AbfsFileSystems

fs = AbfsFileSystems(
    accounts=[
        {"account_name": "myaccount", "is_hns_enabled": True},
        {"account_name": "opendata", "credential": "anonymous"},
    ],
)

fs.ls("abfss://raw@myaccount.dfs.core.windows.net/data")
# [{'name': 'abfss://raw@myaccount.dfs.core.windows.net/data/2026',
#   'size': 0,
#   'type': 'directory',
#   'last_modified': datetime.datetime(2026, 11, 11, 22, 57, 2, tzinfo=datetime.timezone.utc),
#   'etag': '0x8DF404719C8BD14'},
#  {'name': 'abfss://raw@myaccount.dfs.core.windows.net/data/readme.txt',
#   'size': 1024,
#   'type': 'file',
#   'last_modified': datetime.datetime(2026, 11, 11, 22, 57, 2, tzinfo=datetime.timezone.utc),
#   'etag': '0x8DF404719C7F8C4'}]
```

### Paths

It takes Spark's and Hadoop's URLs only: `abfss://<container>@<account>.dfs.<domain>/<path>`, or `abfs://`. Any
other path raises `ValueError`, as it names no account.

The URLs are its paths, protocol included. `ls`, `info`, `find` and `glob` answer full URLs, always `abfss://`, without
a trailing `/`. Their account and container are in lower case, as Azure names them.

### Options

| Parameter               | Description                                                                         | Default       |
|-------------------------|-------------------------------------------------------------------------------------|---------------|
| `credential`            | The default credential: a Microsoft Entra ID token credential only.                 | `"default"`   |
| `accounts`              | The arguments of `AbfsFileSystem` for some accounts, a dict per account.            | `None`        |
| `operation_concurrency` | Maximum concurrent requests of a recursive operation, for each account.             | `100`         |
| `io_concurrency`        | Maximum concurrent range reads and block uploads, for each account.                 | `16`          |
| `transfer_concurrency`  | Maximum files copied between two accounts at a time.                                | `8`           |
| `cross_account_copy`    | How a file is copied between two accounts: `"streaming"` or `"server-side"`.        | `"streaming"` |

`credential` takes an async token credential, or a keyword or a class name naming one, as in
[Authentication](#authentication). A SAS token, a shared key or `"anonymous"` raises `ValueError`: each fits one
account, so give it to that account in `accounts`.

The router makes the credential once, and every account without a credential of its own shares it, so that tokens
are asked for once. A credential named by a string takes its options from the other keyword arguments, e.g.
`tenant_id`, and is closed by `close()`. A credential object given is yours to close.

Each dict of `accounts` names its account by `account_name`, with `account_domain`, or by `account_url`. The other
keys are those of `AbfsFileSystem`, e.g. `credential` or `is_hns_enabled`. A dict without `credential`,
`operation_concurrency` or `io_concurrency` takes the router's. Any account not listed takes the router's too.

### Copies and moves between accounts

Within an account, a copy or a move is the account's own, e.g. a rename. Between two accounts, files are copied one by
one, `transfer_concurrency` at a time, as `cross_account_copy` says:

| `cross_account_copy` | Each file                                                       | Needs                                                  |
|----------------------|-----------------------------------------------------------------|--------------------------------------------------------|
| `"streaming"`        | Read and written through this process, a block at a time        | Reading the source, and writing the destination        |
| `"server-side"`      | Copied by the destination's account from the source's URL       | The source's URL readable by the destination's account |

With `"server-side"`, the source's URL is its [signed URL](#signed-urls), as `sign` gives it. A SAS signed for the copy
is valid for 12 hours, so that a long copy keeps reading its source.

With `"server-side"`, check two things before a copy:

1. With a Microsoft Entra ID token credential, the identity needs a role giving the action
   `Microsoft.Storage/storageAccounts/blobServices/generateUserDelegationKey` at the source account's level, as
   [Signed URLs](#signed-urls) says.
2. The destination account reads the source over the network itself. The source account must be reachable from it,
   through the network routing settings, the network security groups and the source account's firewall.

A move between two accounts copies each file, then deletes it. A source whose copy failed is kept.

---

## Limitations

### No directory cache

This file system caches no listing: every call reaches the storage, and `use_listings_cache` is always `False`. We have
no plan to implement fsspec's `dircache` listing cache for now.

### File modes

- Files open in `rb`, `wb` and `xb` only. `ab` and `r+b` raise `io.UnsupportedOperation`.
- `open_async` opens binary files only, without compression, as fsspec's does.

### Containers

- The file system makes no container: create them with Azure's own tools, with their settings.
- `makedirs` of a missing container raises `PermissionError`, and of a folder in one `FileNotFoundError`.
- Deleting the root, `/`, raises `PermissionError`.

### Moves

- Without a hierarchical namespace, a move is not atomic: each file is copied on the server, then deleted.
- A rename between two containers needs a token credential or a SAS valid in both. Anonymous, it raises
  `PermissionError`.
- Between two accounts, a move is not atomic either: each file is copied, then deleted.

### Copies between accounts

With `cross_account_copy="server-side"`, no copy falls back on streaming: a copy whose source the destination cannot
read fails.

### Transactions

- The files are committed one after another. A transaction is not atomic across files.
- Only files written with `open` join the transaction, not those of `open_async`.
- Copies, moves and deletes take effect at once.
- If the process dies, uploaded blocks stay uncommitted until Azure drops them, after a week.
