"""Private MinIO storage and signed imgproxy URLs for card artwork."""

import asyncio
import base64
import hashlib
import hmac
import logging
from io import BytesIO
from typing import Dict, Optional

import httpx
from miniopy_async import Minio

from src.config import settings
from src.services.scryfall_transport import request as scryfall_request

logger = logging.getLogger("mtg_worker.image_storage")


class CardImageStorage:
    """Stores Scryfall originals privately and exposes only imgproxy derivatives."""

    _CONTENT_TYPE_EXTENSIONS = {
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
    }

    def __init__(
        self,
        client: Optional[Minio] = None,
        *,
        bucket: str = settings.MINIO_BUCKET,
        public_base_url: str = settings.PUBLIC_IMAGE_BASE_URL,
        imgproxy_key: str = settings.IMGPROXY_KEY,
        imgproxy_salt: str = settings.IMGPROXY_SALT,
    ) -> None:
        self.client = client or Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        self.bucket = bucket
        self.public_base_url = public_base_url.rstrip("/")
        self.key = self._decode_hex(imgproxy_key, "IMGPROXY_KEY")
        self.salt = self._decode_hex(imgproxy_salt, "IMGPROXY_SALT")
        self._bucket_ready = False
        self._bucket_lock = asyncio.Lock()

    @staticmethod
    def _decode_hex(value: str, setting_name: str) -> bytes:
        if not value:
            return b""
        try:
            return bytes.fromhex(value)
        except ValueError as error:
            raise ValueError(f"{setting_name} must be hex encoded") from error

    async def ensure_bucket(self) -> None:
        if self._bucket_ready:
            return
        async with self._bucket_lock:
            if self._bucket_ready:
                return
            if not await self.client.bucket_exists(self.bucket):
                await self.client.make_bucket(self.bucket)
                logger.info("Created MinIO bucket %s", self.bucket)
            self._bucket_ready = True

    async def close(self) -> None:
        """Release the aiohttp session owned by the async MinIO client."""
        await self.client.close_session()

    async def store_card_images(self, card_id: str, image_urls: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
        """Download the best original once; imgproxy creates small, normal, and large variants."""
        existing_object = await self._find_existing_card_object(card_id)
        if existing_object:
            return self.derivatives_for_object(existing_object)

        source_url = (
            image_urls.get("normal")
            or image_urls.get("large")
            or image_urls.get("png")
            or image_urls.get("small")
        )
        if not source_url:
            return {
                "image_uri": None,
                "image_uri_small": None,
                "image_uri_large": None,
            }

        object_key = await self._download_and_store(card_id, source_url)
        return self.derivatives_for_object(object_key)

    def derivatives_for_object(self, object_key: str) -> Dict[str, str]:
        """Return every supported derivative for one original in MinIO."""
        return {
            "image_uri": self.imgproxy_url(object_key, width=672, height=936),
            "image_uri_small": self.imgproxy_url(object_key, width=146, height=204),
            "image_uri_large": self.imgproxy_url(object_key, width=1024, height=1404),
        }

    async def _find_existing_card_object(self, card_id: str) -> Optional[str]:
        """Avoid downloading Scryfall again when this original is already in MinIO."""
        await self.ensure_bucket()
        for extension in self._CONTENT_TYPE_EXTENSIONS.values():
            object_key = f"originals/{card_id}/card.{extension}"
            try:
                await self.client.stat_object(self.bucket, object_key)
                return object_key
            except Exception:
                continue
        return None

    def minio_uri(self, object_key: str) -> str:
        """Stable S3 URI identifying an object stored exclusively in MinIO."""
        return f"s3://{self.bucket}/{object_key}"

    async def store_set_icon(self, set_code: str, source_url: str) -> str:
        """Store a set SVG in MinIO; never write icons to the container filesystem."""
        object_key = f"set-icons/{set_code.lower()}.svg"
        await self.ensure_bucket()
        try:
            await self.client.stat_object(self.bucket, object_key)
            return self.minio_uri(object_key)
        except Exception:
            pass
        response = await scryfall_request(
            "GET", source_url,
            headers={"User-Agent": settings.USER_AGENT, "Accept": "image/svg+xml,image/*;q=0.8,*/*;q=0.5"},
        )
        response.raise_for_status()
        if not response.content:
            raise ValueError(f"Scryfall returned an empty set icon for {set_code}")
        await self.client.put_object(
            self.bucket, object_key, BytesIO(response.content), len(response.content), content_type="image/svg+xml"
        )
        return self.minio_uri(object_key)

    async def _download_and_store(self, card_id: str, source_url: str) -> str:
        headers = {
            "User-Agent": settings.USER_AGENT,
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }
        response = await scryfall_request("GET", source_url, headers=headers, timeout=60.0)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "image/jpeg").split(";", 1)[0].lower()
        extension = self._CONTENT_TYPE_EXTENSIONS.get(content_type, "jpg")
        object_key = f"originals/{card_id}/card.{extension}"
        content = response.content
        if not content:
            raise ValueError(f"Scryfall returned an empty image for card {card_id}")

        await self.client.put_object(
            self.bucket,
            object_key,
            BytesIO(content),
            len(content),
            content_type=content_type,
        )
        return object_key

    def imgproxy_url(self, object_key: str, *, width: int, height: int) -> str:
        """Build a signed URL for a WebP derivative without exposing MinIO."""
        source = f"s3://{self.bucket}/{object_key}".encode()
        encoded_source = base64.urlsafe_b64encode(source).rstrip(b"=").decode()
        path = f"/rs:fill:{width}:{height}:0/{encoded_source}.webp"
        signature = "insecure"
        if self.key and self.salt:
            digest = hmac.new(self.key, self.salt + path.encode(), hashlib.sha256).digest()
            signature = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
        return f"{self.public_base_url}/{signature}{path}"
