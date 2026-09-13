from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.tor import TorClient


@pytest.mark.asyncio
async def test_tor_requests_a_new_circuit_over_control_port():
    reader = MagicMock(readline=AsyncMock(side_effect=[b"250 OK\r\n", b"250 OK\r\n"]))
    writer = MagicMock(drain=AsyncMock(), wait_closed=AsyncMock())
    with patch("src.services.tor.asyncio.open_connection", new=AsyncMock(return_value=(reader, writer))):
        assert await TorClient(host="tor", port=9051).request_new_identity()
    sent = b"".join(call.args[0] for call in writer.write.call_args_list)
    assert b"AUTHENTICATE" in sent and b"SIGNAL NEWNYM" in sent
