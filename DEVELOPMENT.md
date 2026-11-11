# Development

## Setup

```bash
uv sync
```

All Python commands go through [uv](https://docs.astral.sh/uv/). The tests run on a local dummy server by default,
with no account and no credentials.

## Tests

```bash
uv run pytest                     # all tests, on the dummy server, with and without HNS
uv run pytest --api=live          # the filesystem tests on live accounts
uv run pytest --api=both          # on both
uv run pytest --hns=true          # only accounts with a hierarchical namespace: true, false or both
uvx ruff check .                  # lint
uvx ruff format .                 # format
```

Two options of `tests/conftest.py` choose what runs:

- `--api` chooses where the filesystem tests run: `dummy`, the default, `live` or `both`;
- `--hns` chooses the kinds of account: `true` for a hierarchical namespace, `false` for none, or `both`, the default.

Without the options, `FSSPEC_ABFS_TEST_API` and `FSSPEC_ABFS_TEST_HNS` choose, from the environment or `.env`. An
option overrides its variable. A test id names its API and its kind of account, e.g. `test_ls[dummy-hns]`.

| File                  | Tests of                                                         |
|-----------------------|------------------------------------------------------------------|
| `test_abfs.py`        | `AbfsFileSystem` and its file objects, over HTTP                 |
| `test_hns.py`         | The probe of the hierarchical namespace, and the profiles        |
| `test_listing.py`     | `_listing.py`'s ListBlobs paging, against the SDK's listing      |
| `test_credentials.py` | `credentials.py`: what each value passed as a credential gives   |
| `test_errors.py`      | `_errors.py`, with no server                                     |
| `test_paths.py`       | `_paths.py`, the account's arguments and the cache, no server    |
| `test_package.py`     | The package itself                                               |

The fixtures of `tests/conftest.py`:

- `make_fs`: makes an `AbfsFileSystem` of the account the test runs on, async and uncached;
- `fs`: one made by `make_fs`;
- `root`: a folder of the test's own, not created yet, e.g. `/c/work` on the dummy server;
- `dummy`: the dummy account, its switches, and the requests it served, reset after the test.

A test taking `make_fs`, `fs` or `root` runs once per API and kind of account chosen. Some tests make sense on one
side only:

- A test taking `dummy` runs on the dummy server only. It switches the server's behaviour, or counts the requests
  it served.
- The `dummy_only`, `live_only`, `hns_only` and `flat_only` markers restrict the others.
- A test is skipped when the options exclude its side.

### The dummy server

`tests/dummy_api.py` is a FastAPI app served by Hypercorn on a free local port. The SDK's own async clients talk to it
over HTTP, so their requests, errors and paging run as against Azure. One store serves both APIs, under two path
prefixes: `/blob` and `/dfs`.

`AbfsFileSystem` only accepts Azure's accounts. The `make_fs` fixture gives it the account `dummy`, then sets its
`_dfs_url` and `_blob_url` to the dummy's before the first request, as its clients are made on them at first use. The
dummy needs no credential, and the tests send none: the SDK refuses tokens over plain HTTP.

The dummy's containers are made with `dummy.create_container`: the filesystem makes none.

It answers these operations of the blob API:

- Get Account Information, with `x-ms-is-hns-enabled`;
- List Containers, Create Container, Delete Container and Get Container Properties;
- List Blobs, with a prefix, a delimiter, metadata and pages;
- Get Blob Properties, Get Blob with a range and `If-Match`, and Set Blob Metadata;
- Put Blob, Put Block and Put Block List, with `If-None-Match: *`;
- Delete Blob and Copy Blob.

It answers these operations of the Data Lake API:

- List Paths, one folder or recursive, with pages;
- Create Path, for folders and files;
- Rename Path, of a file or a folder with all in it;
- Delete Path, recursive or not;
- Get Access Control, of a container's root too.

The `hns` switch chooses the kind of account. With a hierarchical namespace, folders are paths of their own:

- the blob API sees them as empty blobs with the metadata `hdi_isfolder=true`;
- writing a blob creates its missing parents;
- the blob API refuses to delete a folder with things in, by `DirectoryIsNotEmpty`.

Without one, folders are prefixes, and every Data Lake request fails with `HierarchicalNamespaceNotEnabled`.

Errors come in Azure's format: an `x-ms-error-code` header, with XML on the blob API and JSON on the Data Lake API.
The SDK raises its own exceptions from them, with their `error_code`.

`dummy.requests` records each request served: its API, its REST operation's name, e.g. `ListBlobs`, its method, path
and query. `dummy.count(operation)` and `dummy.operations()` read them. Tests counting requests assert on these.

Switches of `dummy` make the server answer as a given account:

- `hns` chooses the kind of account, from the test's `hns`;
- `failures` maps an operation to the status and code it fails with, e.g. to refuse the account information;
- `page_size` limits the entries of a listing's page;
- `copy_pending_reads` answers copies as pending, for as many property reads;
- `copy_outcome` is the status of a pending copy at its end, e.g. `failed`.

Where Azure's references leave behaviour open, the dummy makes an assumption, marked in a comment:

- With HNS, a blob written over a folder fails with `OperationNotAllowedOnDirectory`, and under a file with
  `PathConflict`.
- A Data Lake listing of a file fails with `PathNotFound`.
- Creating an existing folder keeps it, with all in it.
- A rename replaces a file only: onto a folder, or of a folder onto anything, it fails with `PathAlreadyExists`.
- A copy's data is in place at once, even while the copy is reported pending.

It checks no authentication, no lease, no SAS and no ACL. It keeps no versions, snapshots or tags.

### Live tests

Live tests are opt in, with `--api` or `FSSPEC_ABFS_TEST_API`. Once asked for, an account that cannot be reached is
an error, not a skip that would pass unseen. They need an account of each kind chosen by `--hns`:

| Environment variable                | Contents                                                       |
|-------------------------------------|----------------------------------------------------------------|
| `FSSPEC_ABFS_TEST_HNS_ACCOUNT_URL`  | The endpoint of an account with a hierarchical namespace       |
| `FSSPEC_ABFS_TEST_FLAT_ACCOUNT_URL` | The endpoint of an account without one                         |
| `FSSPEC_ABFS_TEST_CONTAINER`        | A container of that name in each account                       |

An endpoint is e.g. `https://<account>.dfs.core.windows.net/`. Each test works in a folder of its own,
`fsspec-abfs-test-<run>/<test>` in the container, deleted afterwards. The credential is
`azure.identity.aio.DefaultAzureCredential`, e.g. from `az login`. The tests load a `.env` file in the repository
folder, which can hold these variables.

Only live tests check what the dummy assumes, and what it does not model:

- Azure's real answers where the references are silent, listed above;
- authentication, SAS tokens and roles, and the probe refused by them;
- the latency, throttling and retries of a real service.

## CI

| Workflow       | Runs on                                  | Does                                          |
|----------------|------------------------------------------|-----------------------------------------------|
| `ci.yaml`      | Pull requests, pushes to `main`, by hand | Lint, offline tests, then live tests on Azure |
| `release.yaml` | Pushed `v*` tags                         | Build, GitHub release, and publishing to PyPI |

`ci.yaml` has three jobs:

- `lint`: ruff check, ruff format check, zizmor and actionlint;
- `test`: pytest `--api=dummy` on Python 3.11 to 3.14, with no credentials;
- `live`: after `test`, pytest `--api=live` on Python 3.14, in the GitHub environment `azure`.

`live` runs on pushes, by hand, and on pull requests from branches of this repository. Pull requests from forks and
Dependabot get no environment, so they run `test` only. An environment without `AZURE_CLIENT_ID` is skipped with a
notice.

Until the library's next release, `test` and `live` also check out fsspec-alt-strategies' `wip` branch beside this
repository, where `[tool.uv.sources]` and `uv.lock` expect it.

### Supply chain

The live job holds a credential of the accounts, so what it runs is fixed:

- `uv.lock` is committed, with every package's hash. CI installs with `uv sync --locked`, which fails on a stale lock.
- Actions are pinned to commit SHAs, and setup-uv to a uv version.
- Dependabot updates `uv.lock` and the Actions' SHAs weekly.
- Each workflow's token can only read the repository. Only the jobs that need them add `id-token: write` or
  `contents: write`.
- The live job and the release build use no cache.

ruff, zizmor and actionlint, run with `uvx`, are the exception: CI runs their latest versions, in a job without secrets.

### The `azure` environment

The live tests authenticate as a Microsoft Entra ID app, by workload identity federation: no secret is stored. The
app needs Storage Blob Data Contributor on the test container of each account.

| Variable                            | Value                                                         |
|-------------------------------------|---------------------------------------------------------------|
| `AZURE_CLIENT_ID`                   | The app's client ID                                           |
| `AZURE_TENANT_ID`                   | Its tenant's ID                                               |
| `FSSPEC_ABFS_TEST_HNS_ACCOUNT_URL`  | The endpoint of an account with a hierarchical namespace      |
| `FSSPEC_ABFS_TEST_FLAT_ACCOUNT_URL` | The endpoint of an account without one                        |
| `FSSPEC_ABFS_TEST_CONTAINER`        | A container of that name in each account                      |

The kinds of account tested are those whose endpoint is set. The app's federated credential names the issuer
`https://token.actions.githubusercontent.com`, the subject `repo:sugibuchi/fsspec-abfs:environment:azure` and the
audience `api://AzureADTokenExchange`.

`.github/scripts/with-id-token-file.sh` keeps GitHub's ID token in a file, refreshed every minute, and sets
`AZURE_FEDERATED_TOKEN_FILE`. `DefaultAzureCredential` then exchanges it through its `WorkloadIdentityCredential`.

## Releasing

The version comes from the git tag, through hatch-vcs.

1. Push a new tag `vX.Y.Z` on `main`, e.g. `git push origin v0.1.0`.
2. `release.yaml` builds the package and checks that its version is the tag's.
3. It creates the GitHub release with the built files, and publishes them to PyPI in the GitHub environment `pypi`.

PyPI's trusted publisher for `fsspec-abfs` names the workflow `release.yaml` and the environment `pypi`. It
authenticates with the workflow's OIDC token, with no stored token.

Release only once fsspec-alt-strategies has released the interface this package needs. The published package depends
on `fsspec-alt-strategies` from PyPI: `[tool.uv.sources]` is for development only.

## Design

### One file system per account

`AbfsFileSystem` works on one storage account. Its paths are `/<container>/<path>`, and its root lists the
containers.

- The account is given by `account_name` and `account_domain`, or by `account_url`. Both make the same cache key, so
  fsspec caches one file system per account however it is named.
- A URL naming another account is refused, as the file system would otherwise act on its own path of that name.
- No container is made. Containers are made by Azure's own means, with their settings, e.g. their access level.

### Strategies from fsspec-alt-strategies

The recursive operations and the file objects come from
[fsspec-alt-strategies](https://github.com/sugibuchi/fsspec-alt-strategies), a library of filesystem-independent
strategies for fsspec. `AbfsFileSystem` implements only the primitives of Azure's APIs, and the library does the rest:

- **Tree strategies:** `AbfsFileSystem` subclasses `TreeFileSystem`, as both APIs list one folder per request, or a
  whole subtree by one recursive listing. `find`, `du`, `glob`, `rm`, `copy` and `mv` act on files while the listing
  goes on.
- **Storage profiles:** what the account does is declared in a `StorageProfile`, which follows the hierarchical
  namespace. The strategies then skip the requests it makes needless.
- **File objects:** files are adaptive files, whose blocks grow with sequential access. Transactions hold a file's
  last request back until fsspec's transaction commits.
- **Monitors:** the operations behind fsspec's methods report their progress to the monitors of the interface
  package, and can be cancelled.

The strategies are copied into the package rather than installed, so that a release of the library never changes this
package's behaviour. Their shared interface, `fsspec-alt-strategies`, is installed.
[The copy of fsspec-alt-strategies](#the-copy-of-fsspec-alt-strategies) says how the copy is kept.

### Two APIs of one account

Every account serves two APIs over the same storage, on two endpoints:

- the blob API, for what both kinds of account answer alike: a path's properties, deletions, server-side copies, and
  the files' data;
- the Data Lake API, for what only an account with a hierarchical namespace has: real folders, listings of one
  folder, atomic renames and recursive deletes.

Whether the account has a hierarchical namespace is declared by `is_hns_enabled`, or asked once. While it is unknown,
the file system takes the way right on both kinds: the blob API, and moves by copy and delete.

### Fewer requests

Each primitive sends as few requests as the account allows:

- `cat_file` downloads a file in one request, with no properties read first.
- `info` reads a blob's properties, and lists a prefix only on an account without a hierarchical namespace.
- With a hierarchical namespace, a move is one rename, and a recursive delete one request.
- A recursive listing of the blob API asks for each next page while the last one is parsed.

### Standard exceptions

Errors are `OSError` subclasses, as from local files. Azure's errors are mapped by their error code first, then by
their HTTP status, as the two APIs name the same failure differently. A message keeps the error's type, status and
code only: Azure's own messages may quote request URLs, with a SAS token in them.

### Picklable

File systems are pickled to run in other processes, e.g. by Dask. fsspec pickles a file system as its constructor
arguments, and unpickling calls the constructor again:

- the clients are not pickled, and are made again on first use;
- a credential given as a string is made again;
- a credential object must be picklable itself.

### Known limits

- **Renames stay within a container.** A file moved between two containers is copied, then deleted. A folder moved
  between two raises `OSError` with `EXDEV`, before anything is copied.
- **Recursive methods do not check a URL's account.** They strip it before any primitive sees it.
  `test_recursive_methods_refuse_another_account` marks the gap as an expected failure.
- **No append or update modes.** `ab` and `r+b` are refused.
- **No listing cache.** Each call reaches the account.
- **`az://` is not registered.** The URLs are understood, but fsspec sends them to adlfs.

## Architecture

| Module               | Holds                                                                             |
|----------------------|-----------------------------------------------------------------------------------|
| `abfs.py`            | `AbfsFileSystem`, its three storage profiles, and its file objects                |
| `_paths.py`          | The accounts, URLs and paths, as regular expressions                              |
| `credentials.py`     | `StorageCredential`: what each value passed as a credential gives                 |
| `_errors.py`         | Azure's errors as `OSError`s                                                      |
| `_listing.py`        | The blob API's recursive listing, a page asked for while the last one is parsed   |
| `_contrib/strategy/` | The copy of fsspec-alt-strategies                                                 |

`__init__.py` imports `AbfsFileSystem` on first use, as the Azure SDK is slow to import. The entry points register it
for `abfs` and `abfss`, replacing adlfs' registration.

### The account and the cache

`_AccountCached`, the metaclass of `AbfsFileSystem`, runs before fsspec computes the cache key:

- it refuses positional arguments;
- it makes `account_url` into `account_name` and `account_domain`, which are in the key;
- `_strip_tokenize_options` leaves `account_url` out of the key.

`split_path` gives a path's container and its path within the container. It strips the protocol, and refuses a URL of
another account. Every primitive starts with it. `unstrip_protocol` gives Hadoop's URL, which names the account.

### The clients

A `BlobServiceClient` and a `DataLakeServiceClient` are made together on first use, on the running loop, with one
credential. The SDK's async clients hold an aiohttp session bound to that loop. fsspec caches an async file system per
loop, so the clients are made once, and the loop is kept for closing them.

`close()` closes both clients on their loop, and the credential when the file system made it from a string. A token
credential object given is the caller's to close.

### The storage profile

`storage_profile` is `HNS_PROFILE`, `NO_HNS_PROFILE` or `UNKNOWN_HNS_PROFILE`:

- `is_hns_enabled` given to the constructor sets it, and nothing is asked.
- Otherwise the first operation needing it calls `_load_storage_profile` with its paths.
- `_check_hns_enabled` asks the account information, then the root ACL of the operation's container, as a SAS or a
  role of one container is refused the account information.
- If neither tells, a warning is logged, and the profile stays unknown for the file system's life.

### Primitives

| Primitive              | With a hierarchical namespace                  | Without one                                    |
|------------------------|------------------------------------------------|------------------------------------------------|
| `_info`                | Get Blob Properties                            | Get Blob Properties, then List Blobs           |
| `_ils`, one folder     | List Paths                                     | List Blobs with a delimiter                    |
| `_ils`, recursive      | List Paths, recursive                          | `_listing.list_blobs_flat`                     |
| `_makedirs`            | Create Path                                    | Nothing, the container checked                 |
| `_rm_file`, `_rmdir`   | Delete Blob                                    | Delete Blob                                    |
| `_rm_tree`             | Delete Path, recursive                         | Not called                                     |
| `_cp_file`             | Copy Blob, waited for while pending            | Copy Blob, waited for while pending            |
| `_mv_file`, `_mv_tree` | Rename Path within a container                 | Copy Blob, then Delete Blob                    |
| `_cat_file`            | Get Blob                                       | Get Blob                                       |

While the namespace is unknown, `_makedirs` writes a blob with the metadata `hdi_isfolder=true`: a real folder with a
hierarchical namespace, a marker blob without one. The listings take both as folders.

`_ils` lists the containers at the root. A Data Lake listing of a file fails as not found: the file then lists as
itself, as fsspec's `_ls` lists it. Any other error is the listing's. An empty listing asks `_info`, to tell an empty
folder from a file or nothing.

### File objects

`AbfsReadableFile` reads ranges of one version of a blob, by `If-Match` with its ETag. A blob replaced while it is read
raises `OSError` with `ESTALE`.

`AbfsWritableFile` writes a block blob:

- under 8 MiB, by one Put Blob when it is closed;
- otherwise by Put Block, in blocks of 4 MiB to 64 MiB, then Put Block List when it is closed;
- mode `xb` by `If-None-Match: *` on the request making the blob visible;
- an abort sends nothing, as the service drops uncommitted blocks after a week.

In `with fs.transaction:`, `_open` wraps the file in the library's `AdaptiveWritableTransaction`. It holds back the
Put Blob or Put Block List until commit.

### Errors

`azure_errors(path)` is a context manager around SDK calls. `os_error` maps an `AzureError`:

| Azure error                                                          | Raised                         |
|----------------------------------------------------------------------|--------------------------------|
| `DirectoryNotEmpty`, `DirectoryIsNotEmpty`                           | `OSError` with `ENOTEMPTY`     |
| `OperationNotAllowedOnDirectory`                                     | `IsADirectoryError`            |
| `PathIsNotADirectory`, `OperationNotAllowedOnFile`                   | `NotADirectoryError`           |
| `BlobAlreadyExists`, `PathAlreadyExists`, `ContainerAlreadyExists`   | `FileExistsError`              |
| `ConditionNotMet`, `SourceConditionNotMet`, `ResourceModifiedError`  | `OSError` with `ESTALE`        |
| `ResourceNotFoundError`, status 404                                  | `FileNotFoundError`            |
| Authorization and authentication codes, status 401 or 403            | `PermissionError`              |
| Any other conflict, with no error code                               | `FileExistsError`              |
| Status 400                                                           | `OSError` with `EINVAL`        |
| Any other                                                            | `OSError` with `EIO`           |

`CancelledError` and errors other than `AzureError` pass as they are.

## The copy of fsspec-alt-strategies

`src/fsspec_abfs/_contrib/strategy/` is a copy of the package `fsspec-alt-strategies-impl`, from the library's `wip`
branch after 0.3.0: its `_version.py` names the commit. It keeps
only what `AbfsFileSystem` uses: `tree/`, `common/` and `files/`. `AbfsFileSystem` lists a whole tree by Azure's own
recursive listings, so the tree family's fallback, `tree/listing.py`, is left out too. `_version.py` stays, as the
copy's command line recognises a previous copy by it. Ruff skips the copy.

Its interface is not copied: it is the dependency `fsspec-alt-strategies>=0.3,<0.4`, bounded at the copy's minor
version, as a minor release may break the copy before 1.0. Until the library's next release, `[tool.uv.sources]` takes
it from a checkout of the library's `wip` branch beside this repository, at `../fsspec-alt-strategies`.

To refresh it:

1. Copy the package over the old copy, which replaces it whole:

   ```bash
   uv run --with "fsspec-alt-strategies-impl[cli]==<version>" fsspec-alt-strategies-impl copy src/fsspec_abfs/_contrib/strategy
   ```

2. Delete again what `AbfsFileSystem` does not use:

   ```bash
   cd src/fsspec_abfs/_contrib/strategy
   rm -r flat wrapper monitor.py router.py __init__.py common/sync_client.py tree/listing.py
   ```

3. Update the version above, and the interface's bounds in `pyproject.toml`, then run `uv lock` and the tests.
