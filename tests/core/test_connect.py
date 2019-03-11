import asyncio

import pytest

from conftest import (
    generate_unique_name,
)
from lahja import (
    ConnectionAttemptRejected,
    ConnectionConfig,
    Endpoint,
    ListenerConfig,
)


@pytest.mark.asyncio
async def test_can_not_connect_conflicting_names_blocking(
        event_loop: asyncio.AbstractEventLoop) -> None:

    own = ConnectionConfig.from_name(generate_unique_name())
    endpoint = Endpoint()
    await endpoint.start_serving(own, event_loop)

    # We connect to our own Endpoint because for this test, it doesn't matter
    # if we use a foreign one or our own
    endpoint.add_listener_endpoints_blocking(ListenerConfig.from_connection_config(own))

    # Can't connect a second time
    with pytest.raises(ConnectionAttemptRejected):
        endpoint.add_listener_endpoints_blocking(ListenerConfig.from_connection_config(own))


@pytest.mark.asyncio
async def test_can_not_connect_conflicting_names(
        event_loop: asyncio.AbstractEventLoop) -> None:

    own = ConnectionConfig.from_name(generate_unique_name())
    endpoint = Endpoint()
    await endpoint.start_serving(own, event_loop)

    # We connect to our own Endpoint because for this test, it doesn't matter
    # if we use a foreign one or our own
    await endpoint.add_listener_endpoints(ListenerConfig.from_connection_config(own))

    # Can't connect a second time
    with pytest.raises(ConnectionAttemptRejected):
        await endpoint.add_listener_endpoints(ListenerConfig.from_connection_config(own))


@pytest.mark.asyncio
async def test_rejects_duplicates_when_connecting_blocking(
        event_loop: asyncio.AbstractEventLoop) -> None:

    own = ConnectionConfig.from_name(generate_unique_name())
    endpoint = Endpoint()
    await endpoint.start_serving(own, event_loop)

    with pytest.raises(ConnectionAttemptRejected):
        endpoint.add_listener_endpoints_blocking(
            ListenerConfig.from_connection_config(own),
            ListenerConfig.from_connection_config(own),
        )


@pytest.mark.asyncio
async def test_rejects_duplicates_when_connecting(
        event_loop: asyncio.AbstractEventLoop) -> None:

    own = ConnectionConfig.from_name(generate_unique_name())
    endpoint = Endpoint()
    await endpoint.start_serving(own, event_loop)

    with pytest.raises(ConnectionAttemptRejected):
        await endpoint.add_listener_endpoints(
            ListenerConfig.from_connection_config(own),
            ListenerConfig.from_connection_config(own),
        )


@pytest.mark.asyncio
async def test_rejects_duplicates_when_connecting_nowait(
        event_loop: asyncio.AbstractEventLoop) -> None:

    own = ConnectionConfig.from_name(generate_unique_name())
    endpoint = Endpoint()
    await endpoint.start_serving(own, event_loop)

    with pytest.raises(ConnectionAttemptRejected):
        endpoint.add_listener_endpoints_nowait(
            ListenerConfig.from_connection_config(own),
            ListenerConfig.from_connection_config(own),
        )
