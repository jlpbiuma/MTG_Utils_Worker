# MTG Utils - Set Sync Background Worker

Background daemon that periodically synchronizes Magic: The Gathering card sets and their printings/reprints from the Scryfall API into the local PostgreSQL database.

## Features
- **Set Discovery**: Periodically discovers all MTG sets from Scryfall (`GET https://api.scryfall.com/sets`) and records new sets.
- **Gradual Download**: Downloads sets incrementally in controlled batches (`SETS_PER_CYCLE`, default 2 sets per cycle) to avoid saturating Scryfall or system resources.
- **Paging & Rate Limiting**: Traverses cards with pagination while enforcing polite request delays (default 100ms) adhering to Scryfall guidelines.
- **Card Catalog Enrichment**: In addition to saving printings to `CardPrinting`, sets populate `CardCatalog` so the rest of the application has immediate access to new cards.
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
