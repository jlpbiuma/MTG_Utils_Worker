FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS base

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    openssl \
    curl \
    ca-certificates \
    libatomic1 \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

COPY prisma ./prisma
RUN uv run prisma generate

COPY src ./src

EXPOSE 8001

CMD ["sh", "-c", "uv run prisma generate && exec uv run python -m src.main"]
