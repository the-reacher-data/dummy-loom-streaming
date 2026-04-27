FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY src ./src
COPY tests ./tests
COPY config ./config

RUN uv sync --extra dev

ENV PYTHONPATH=/app/src
