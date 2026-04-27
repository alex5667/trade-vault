#!/usr/bin/env sh
set -eu

TEMPLATE="${PROM_TEMPLATE_PATH:-/etc/prometheus/prometheus.yml.tmpl}"
OUT="${PROM_CONFIG_PATH:-/etc/prometheus/prometheus.yml}"

if [ ! -f "$TEMPLATE" ]; then
  echo "[entrypoint] missing template: $TEMPLATE" >&2
  exit 1
fi
echo "[entrypoint] rendering config: $TEMPLATE -> $OUT"
sed "s|\${PUBLIC_BASE_URL}|$PUBLIC_BASE_URL|g" "$TEMPLATE" > "$OUT"

echo "[entrypoint] starting prometheus"
exec /bin/prometheus \
  --config.file="$OUT" \
  --storage.tsdb.path=/prometheus \
  --web.enable-lifecycle \
  ${PROMETHEUS_EXTRA_ARGS:-}
