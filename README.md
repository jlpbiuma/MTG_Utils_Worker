# MTG Utils - Set Sync Background Worker

Background daemon that periodically synchronizes Magic: The Gathering card sets and their printings/reprints from the Scryfall API into the local PostgreSQL database.

## Features
- **Set Discovery**: Periodically discovers all MTG sets from Scryfall (`GET https://api.scryfall.com/sets`) and records new sets.
- **Gradual Download**: Downloads sets incrementally in controlled batches (`SETS_PER_CYCLE`, default 2 sets per cycle) to avoid saturating Scryfall or system resources.
- **Paging & Rate Limiting**: Tor and direct requests share a per-host, process-local limiter: 650ms between collection/search/named/random calls, 6.1s for manifest, and at least 150ms for other calls. HTTP 429 pauses all requests to the host for at least 30 seconds, honors longer `Retry-After` values, and increases the wait on repeated failures. Backend processes calling Scryfall directly are not covered by this limiter.
- **Card Catalog Enrichment**: In addition to saving printings to `CardPrinting`, sets populate `CardCatalog` so the rest of the application has immediate access to new cards.
- **Private image storage**: Card originals are downloaded from Scryfall into MinIO; the database stores signed imgproxy URLs for WebP derivatives instead of Scryfall image URLs.
- **HTTP Health & Manual Control**:
  - `GET /health` - Health status and run info.
  - `GET /status` - Complete worker status and configuration.
  - `POST /trigger` - Trigger an immediate background synchronization cycle.

## Setup & Running Locally
```bash
uv sync
uv run prisma generate
uv run python -m src.main
```

## Running Tests
```bash
uv run pytest
```

Read-only before/after transport measurements and their limitations are in
[`benchmarks/RESULTS.md`](benchmarks/RESULTS.md). The live diagnostic stops at
the first HTTP 429 and does not import cards or download images.

## Image storage configuration

When using the root `docker-compose.yml`, MinIO, imgproxy and Nginx are started together. Copy the root `.env.example` to `.env` and replace the development credentials and imgproxy key/salt before production use. Nginx exposes processed images at `PUBLIC_IMAGE_BASE_URL` and caches them in the `nginx_image_cache` volume; original images stay private in the `minio_data` volume.
