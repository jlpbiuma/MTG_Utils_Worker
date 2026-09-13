import base64
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.image_storage import CardImageStorage


def test_imgproxy_url_is_signed_and_does_not_expose_minio() -> None:
    storage = CardImageStorage(
        client=MagicMock(),
        bucket="mtg-images",
        public_base_url="https://images.example/images",
        imgproxy_key="00" * 32,
        imgproxy_salt="11" * 32,
    )

    url = storage.imgproxy_url("originals/card-123/card.jpg", width=146, height=204)

    assert url.startswith("https://images.example/images/")
    assert "/rs:fill:146:204:0/" in url
    assert "minio" not in url
    assert "s3://" not in url
    encoded_source = url.rsplit("/", 1)[-1].removesuffix(".webp")
    assert base64.urlsafe_b64decode(encoded_source + "===") == b"s3://mtg-images/originals/card-123/card.jpg"


@pytest.mark.asyncio
async def test_store_card_images_uploads_original_and_returns_derivatives() -> None:
    client = MagicMock()
    client.bucket_exists = AsyncMock(return_value=True)
    client.stat_object = AsyncMock(side_effect=Exception("not found"))
    client.put_object = AsyncMock()
    storage = CardImageStorage(
        client=client,
        bucket="mtg-images",
        public_base_url="https://images.example/images",
        imgproxy_key="00" * 32,
        imgproxy_salt="11" * 32,
    )

    import respx

    with respx.mock as mock:
        route = mock.get("https://cards.example/card.jpg").respond(
            status_code=200,
            content=b"image-bytes",
            headers={"content-type": "image/jpeg"},
        )
        images = await storage.store_card_images(
            "card-123", {"normal": "https://cards.example/card.jpg"}
        )

    client.put_object.assert_awaited_once()
    assert route.calls[0].request.headers["user-agent"] == "MTGUtilsWorker/1.0"
    assert images["image_uri"].endswith(".webp")
    assert images["image_uri_small"].endswith(".webp")
    assert images["image_uri_large"].endswith(".webp")
    assert "/rs:fill:672:936:0/" in images["image_uri"]
    assert "/rs:fill:146:204:0/" in images["image_uri_small"]
    assert "/rs:fill:1024:1404:0/" in images["image_uri_large"]


@pytest.mark.asyncio
async def test_store_card_images_reuses_minio_original_without_downloading_again() -> None:
    client = MagicMock()
    client.bucket_exists = AsyncMock(return_value=True)
    client.stat_object = AsyncMock(return_value=MagicMock())
    client.put_object = AsyncMock()
    storage = CardImageStorage(
        client=client,
        bucket="mtg-images",
        public_base_url="https://images.example/images",
        imgproxy_key="00" * 32,
        imgproxy_salt="11" * 32,
    )

    import respx
    with respx.mock(assert_all_called=False) as mock:
        upstream = mock.get("https://cards.example/card.jpg").respond(200, content=b"must-not-be-read")
        images = await storage.store_card_images(
            "card-123", {"normal": "https://cards.example/card.jpg"}
        )

    assert not upstream.called
    client.put_object.assert_not_awaited()
    assert "/rs:fill:146:204:0/" in images["image_uri_small"]
    assert "/rs:fill:672:936:0/" in images["image_uri"]
    assert "/rs:fill:1024:1404:0/" in images["image_uri_large"]


@pytest.mark.asyncio
async def test_store_set_icon_writes_svg_to_minio_without_a_local_file() -> None:
    client = MagicMock()
    client.bucket_exists = AsyncMock(return_value=True)
    client.stat_object = AsyncMock(side_effect=Exception("not found"))
    client.put_object = AsyncMock()
    storage = CardImageStorage(client=client, bucket="mtg-images")
    import respx
    with respx.mock as mock:
        mock.get("https://svgs.example/mh3.svg").respond(200, content=b"<svg></svg>")
        uri = await storage.store_set_icon("mh3", "https://svgs.example/mh3.svg")
    assert uri == "s3://mtg-images/set-icons/mh3.svg"
    assert client.put_object.await_args.args[1] == "set-icons/mh3.svg"


@pytest.mark.asyncio
async def test_close_releases_minio_session() -> None:
    client = MagicMock()
    client.close_session = AsyncMock()
    storage = CardImageStorage(client=client)

    await storage.close()

    client.close_session.assert_awaited_once()
