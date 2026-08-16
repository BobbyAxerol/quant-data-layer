FROM python:3.12-slim AS builder

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

FROM python:3.12-slim AS runtime

ARG QDL_UID=10001
ARG QDL_GID=10001

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV VIRTUAL_ENV=/opt/venv
ENV PATH=/opt/venv/bin:$PATH

WORKDIR /app

RUN groupadd --gid ${QDL_GID} qdl && \
    useradd --uid ${QDL_UID} --gid ${QDL_GID} --no-create-home --shell /usr/sbin/nologin qdl

COPY --from=builder --chown=qdl:qdl /opt/venv /opt/venv

COPY --chown=qdl:qdl . /app

RUN mkdir -p /app/data/preload/1m /app/logs && \
    chown -R qdl:qdl /app/data /app/logs

USER qdl:qdl

EXPOSE 8100

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8100"]
