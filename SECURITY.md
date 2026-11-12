# Security policy

## Supported versions

Security fixes are made in the latest release only.

## Reporting a vulnerability

Report vulnerabilities privately, through
[GitHub's private vulnerability reporting](https://github.com/sugibuchi/fsspec-abfs/security/advisories/new).
Do not open a public issue.

Include:

- the version of `fsspec-abfs`, and of `azure-storage-blob` and `azure-storage-file-datalake`;
- the steps to reproduce, or a proof of concept;
- the impact, e.g. leaked credentials or access to files beyond the user's permissions.

Never include real credentials, SAS tokens, account keys or account names in a report.

## Scope

`fsspec-abfs` accesses Azure Storage with the credentials of its user, through the Azure SDK. In scope are, for
example:

- credentials, SAS tokens or account keys written to logs, exceptions or `repr`s;
- access to an account, a container or a path beyond what the given path names;
- requests sent to hosts other than the account's blob and Data Lake endpoints, or the token endpoints of the
  credential.

Vulnerabilities of Azure Storage itself or of the Azure SDK go to Microsoft, through the
[Microsoft Security Response Center](https://msrc.microsoft.com/).
