#!/usr/bin/env bash
# Runs a command with GitHub's ID token kept in a file, for Microsoft Entra ID's workload identity federation.
# DefaultAzureCredential's WorkloadIdentityCredential reads AZURE_FEDERATED_TOKEN_FILE whenever it exchanges it.
set -euo pipefail

: "${AZURE_CLIENT_ID:?AZURE_CLIENT_ID must be set}"
: "${AZURE_TENANT_ID:?AZURE_TENANT_ID must be set}"
token_file="$RUNNER_TEMP/github-id-token"

fetch_token() {
  local token
  token=$(curl -sSf -G \
    -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
    --data-urlencode "audience=api://AzureADTokenExchange" \
    "$ACTIONS_ID_TOKEN_REQUEST_URL" | jq -r .value) || return 1
  [[ -n "$token" && "$token" != "null" ]] || return 1
  echo "::add-mask::$token"
  printf '%s' "$token" > "$token_file.tmp"
  mv "$token_file.tmp" "$token_file"
}

fetch_token
# GitHub's ID tokens are short-lived, and each new exchange reads the one in the file.
(while sleep 60; do fetch_token || echo "Failed to refresh GitHub's ID token" >&2; done) &
refresher=$!
trap 'kill "$refresher"' EXIT

export AZURE_FEDERATED_TOKEN_FILE="$token_file"
"$@"
