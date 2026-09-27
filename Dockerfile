ARG QDL_DEPENDENCY_IMAGE=builder

FROM python:3.12-slim@sha256:2c941e860699f878900b0edc2403613c234d4b32eda3cc9fa7036991a2a63c4a AS builder

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV POETRY_VERSION=2.3.4
ENV POETRY_NO_INTERACTION=1

WORKDIR /app

RUN pip install --no-cache-dir poetry==$POETRY_VERSION

COPY pyproject.toml poetry.lock ./

RUN python -m venv /opt/venv && \
    poetry config installer.max-workers 10 && \
    VIRTUAL_ENV=/opt/venv PATH="/opt/venv/bin:$PATH" \
      poetry install --no-root --only main --no-ansi && \
    /opt/venv/bin/python -m pip install --no-cache-dir --upgrade "setuptools>=78.1.1"

# An explicit pinned image preserves locked dependencies withdrawn upstream.
FROM ${QDL_DEPENDENCY_IMAGE} AS verified-dependencies
ARG QDL_DEPENDENCY_IMAGE
USER root
COPY poetry.lock scripts/verify_runtime_dependencies.py /tmp/qdl-build/
RUN /opt/venv/bin/python -B /tmp/qdl-build/verify_runtime_dependencies.py \
    --lock /tmp/qdl-build/poetry.lock --image "${QDL_DEPENDENCY_IMAGE}" \
    --output /tmp/qdl-build/dependency-receipt.json

FROM python:3.12-slim@sha256:2c941e860699f878900b0edc2403613c234d4b32eda3cc9fa7036991a2a63c4a AS runtime

ARG QDL_DEPENDENCY_IMAGE
ARG QDL_UID=10001
ARG QDL_GID=10001
ARG QDL_GIT_SHA=unknown
ARG QDL_RELEASE=development

LABEL io.qdl.dependency-image="${QDL_DEPENDENCY_IMAGE}" \
      org.opencontainers.image.title="Quant Data Layer" \
      org.opencontainers.image.revision="${QDL_GIT_SHA}" \
      org.opencontainers.image.version="${QDL_RELEASE}" \
      org.opencontainers.image.source="https://github.com/BobbyAxerol/quant-data-layer"

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV VIRTUAL_ENV=/opt/venv
ENV HOME=/home/qdl
ENV XDG_CACHE_HOME=/home/qdl/.cache
ENV MPLCONFIGDIR=/home/qdl/.cache/matplotlib
ENV PATH=/opt/venv/bin:$PATH

WORKDIR /app

RUN apt-get update && \
    apt-get upgrade -y --no-install-recommends && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid ${QDL_GID} qdl && \
    useradd --uid ${QDL_UID} --gid ${QDL_GID} --create-home \
      --home-dir /home/qdl --shell /usr/sbin/nologin qdl && \
    install -d -o qdl -g qdl -m 0750 /home/qdl/.cache/matplotlib

COPY --from=verified-dependencies --chown=qdl:qdl /opt/venv /opt/venv
COPY --from=verified-dependencies /tmp/qdl-build/dependency-receipt.json /opt/qdl/dependency-receipt.json

COPY --chown=qdl:qdl . /app

RUN mkdir -p /app/data/preload/1m /app/logs && \
    chown -R qdl:qdl /app/data /app/logs

USER qdl:qdl

EXPOSE 8100

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8100"]
