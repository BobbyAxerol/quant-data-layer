FROM python:3.12-slim AS builder

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV POETRY_VERSION=2.3.4
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV POETRY_NO_INTERACTION=1

WORKDIR /app

RUN pip install --no-cache-dir poetry==$POETRY_VERSION

COPY pyproject.toml poetry.lock ./

RUN poetry config installer.max-workers 10 && \
    poetry install --no-root --only main --no-ansi

FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV PATH=/opt/venv/bin:$PATH

WORKDIR /app

COPY --from=builder /app/.venv /opt/venv

COPY . /app

RUN mkdir -p /app/data/preload/1m

EXPOSE 8100

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8100"]
