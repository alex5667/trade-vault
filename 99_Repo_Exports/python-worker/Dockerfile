ARG PYTHON_BASE_IMAGE=python:3.12-slim
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_EXTRA_INDEX_URL=

###############################################################################
# Builder stage: fetch wheels using configurable PyPI mirror
###############################################################################
FROM ${PYTHON_BASE_IMAGE} AS builder

ARG PIP_INDEX_URL
ARG PIP_EXTRA_INDEX_URL

ENV PIP_INDEX_URL=${PIP_INDEX_URL}
ENV PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL}

ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv "${VIRTUAL_ENV}"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      gfortran \
      libopenblas-dev \
      liblapack-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --default-timeout=100 --upgrade pip wheel hatchling

WORKDIR /app

COPY python-worker/requirements.txt ./

RUN --mount=type=cache,target=/root/.cache/pip \
    pip wheel --wheel-dir /tmp/wheels -r requirements.txt

###############################################################################
# Runtime stage: install wheels only, minimal runtime deps
###############################################################################
FROM ${PYTHON_BASE_IMAGE} AS runtime

ARG PIP_INDEX_URL
ARG PIP_EXTRA_INDEX_URL

ENV PYTHONUNBUFFERED=1
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV PIP_INDEX_URL=${PIP_INDEX_URL}
ENV PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL}
ENV PYTHONPATH=/app

RUN python -m venv "${VIRTUAL_ENV}"

ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      procps \
      libopenblas-dev \
      liblapack-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /tmp/wheels /tmp/wheels
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir --no-deps /tmp/wheels/* && \
    rm -rf /tmp/wheels

# Copy application code (order matters for layer caching)
COPY common/          ./common/
COPY hub/             ./hub/
COPY infra/           ./infra/
COPY core/            ./core/
COPY risk/            ./risk/
COPY dispatch/        ./dispatch/
COPY persistence/     ./persistence/
COPY regime-worker/   ./regime-worker/
COPY specs/           ./specs/
COPY orderflow_services/ ./orderflow_services/
COPY python-worker/   .
COPY scripts/send_real_report.py /app/send_real_report.py

# Install the project itself as an editable package so all sub-packages
# (core, services, handlers, ...) are registered in the venv.
# This makes the container fully self-contained and portable across machines:
# no PYTHONPATH manipulation needed, pkg_resources/importlib.metadata work correctly.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -e /app


CMD ["python", "main.py"]
