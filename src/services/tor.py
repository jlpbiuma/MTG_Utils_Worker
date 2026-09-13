"""Tor transport support used by the unified worker.

Tor can request a fresh circuit, but it deliberately does not guarantee an exit-IP
change and never replaces Scryfall's published rate limits.
"""
import asyncio
import logging

import httpx

from src.config import settings

logger = logging.getLogger("mtg_worker.tor")


class TorClient:
    def __init__(self, host: str = settings.TOR_CONTROL_HOST, port: int = settings.TOR_CONTROL_PORT, password: str = settings.TOR_CONTROL_PASSWORD):
        self.host, self.port, self.password = host, port, password

    async def request_new_identity(self) -> bool:
        """Ask Tor for a new circuit. False means the worker continues normally."""
        if not settings.TOR_ENABLED or not settings.TOR_ROTATE_BEFORE_CYCLE:
            return False
        writer = None
        try:
            reader, writer = await asyncio.open_connection(self.host, self.port)
            command = f'AUTHENTICATE "{self.password}"\r\n' if self.password else "AUTHENTICATE\r\n"
            writer.write(command.encode())
            await writer.drain()
            if not (await reader.readline()).startswith(b"250"):
                return False
            writer.write(b"SIGNAL NEWNYM\r\n")
            await writer.drain()
            return (await reader.readline()).startswith(b"250")
        except OSError as error:
            logger.warning("Tor control endpoint is unavailable: %s", error)
            return False
        finally:
            if writer:
                writer.close()
                await writer.wait_closed()


def async_http_client(**kwargs) -> httpx.AsyncClient:
    """Create an HTTP client routed through Tor when it is enabled."""
    if settings.TOR_ENABLED:
        kwargs.setdefault("proxy", settings.TOR_SOCKS_PROXY)
    return httpx.AsyncClient(**kwargs)
