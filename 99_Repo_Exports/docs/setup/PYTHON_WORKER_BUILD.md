# python-worker Docker Build Guide

## Multi-stage image

- `python-worker/Dockerfile` now uses a builder stage that collects wheels and a runtime stage with a compact Python environment.
- The builder stage installs compilation toolchains (`build-essential`, `gfortran`, `libopenblas-dev`, `liblapack-dev`) and writes wheels into `/tmp/wheels`.
- The runtime stage reuses the same base image but installs only runtime libraries and leverages the prebuilt wheels for fast installs.
- `docker compose build` automatically reuses the pip cache mount to avoid re-downloading artifacts between builds.

## Build arguments

| Argument              | Default                   | Purpose                                             |
| --------------------- | ------------------------- | --------------------------------------------------- |
| `PYTHON_BASE_IMAGE`   | `python:3.12-slim`        | Override base runtime (e.g. `python:3.12-bullseye`) |
| `PIP_INDEX_URL`       | `https://pypi.org/simple` | Point pip to a primary mirror                       |
| `PIP_EXTRA_INDEX_URL` | _(empty)_                 | Append additional mirrors/private indexes           |

### Compose build example

```bash
docker compose build --build-arg PIP_INDEX_URL=https://pypi.org/simple \
  python-worker ohlc-aggregator multi-symbol-orderflow tick-ingest-server \
  signal-performance-tracker periodic-reporter aggregated-hub dom-ingester \
  atr-worker signal-hub paper-executor
```

### Direct docker build

```bash
docker build \
  --build-arg PYTHON_BASE_IMAGE=python:3.12-slim-bookworm \
  --build-arg PIP_INDEX_URL=https://mirror.example.com/pypi/simple \
  --build-arg PIP_EXTRA_INDEX_URL=https://internal.example.com/simple \
  -f python-worker/Dockerfile .
```

## Verification

- After rebuilding, confirm the new image is used: `docker images | grep scanner_infra-python-worker`.
- Run a quick import check: `docker run --rm scanner_infra-python-worker python -c "import numpy, pandas, plotly"`.
