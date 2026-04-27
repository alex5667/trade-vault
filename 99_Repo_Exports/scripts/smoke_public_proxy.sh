#!/usr/bin/env bash
set -euo pipefail

PUBLIC_BASE_URL="${PUBLIC_BASE_URL:-https://localhost}"
VERIFY_TLS="${SMOKE_VERIFY_TLS:-0}"

curl_args=(-sS --max-time 6)
if [[ "${VERIFY_TLS}" != "1" ]]; then
  curl_args+=(-k)
fi

echo "[smoke] base=${PUBLIC_BASE_URL} verify_tls=${VERIFY_TLS}"

endpoints=(
  "/grafana/api/health"
  "/runbooks/healthz"
  "/alertmanager/-/ready"
  "/prometheus/-/ready"
)

for ep in "${endpoints[@]}"; do
  url="${PUBLIC_BASE_URL}${ep}"
  echo "[smoke] GET ${url}"
  code="$(curl "${curl_args[@]}" -o /dev/null -w '%{http_code}' "${url}")"
  if [[ "${code}" != "200" ]]; then
    echo "[smoke] FAIL ${url} status=${code}" >&2
    exit 2
  fi
done

echo "[smoke] OK"
