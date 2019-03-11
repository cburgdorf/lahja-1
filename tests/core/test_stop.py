import asyncio

import pytest

from conftest import (
    generate_unique_name,
)
from helpers import (
    DummyResponse,
)
from lahja import (
    ConnectionConfig,
    Endpoint,
    ListenerConfig,
)


@pytest.mark.asyncio
async def test_can_stop(
        event_loop: asyncio.AbstractEventLoop) -> None:

    first = ConnectionConfig.from_name(generate_unique_name())
    first_endpoint = Endpoint()
    await first_endpoint.start_serving(first, event_loop)

    second = ConnectionConfig.from_name(generate_unique_name())
    second_endpoint = Endpoint()
    await second_endpoint.start_serving(second, event_loop)

    await first_endpoint.add_listener_endpoints(ListenerConfig.from_connection_config(second))
    await second_endpoint.add_listener_endpoints(ListenerConfig.from_connection_config(first))

    first_endpoint.stop()

    second_endpoint.broadcast(DummyResponse(None))
