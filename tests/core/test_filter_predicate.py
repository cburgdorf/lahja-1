import asyncio
from typing import (
    AsyncGenerator,
    Tuple,
    cast,
)

import pytest

from conftest import (
    EndpointTriplet,
    generate_unique_name,
)
from helpers import (
    DummyEvent,
)
from lahja import (
    BaseEvent,
    ConnectionConfig,
    Endpoint,
    ListenerConfig,
)


@pytest.fixture(scope="function")
async def endpoints_with_filter_predicate(
        event_loop: asyncio.AbstractEventLoop) -> AsyncGenerator[EndpointTriplet, None]:

    endpoint1 = Endpoint()
    endpoint2 = Endpoint()
    endpoint3 = Endpoint()
    await endpoint1.start_serving(ConnectionConfig.from_name(generate_unique_name()), event_loop)
    await endpoint2.start_serving(ConnectionConfig.from_name(generate_unique_name()), event_loop)
    await endpoint3.start_serving(ConnectionConfig.from_name(generate_unique_name()), event_loop)

    def is_even(ev: BaseEvent) -> bool:
        return cast(DummyEvent, ev).payload % 2 == 0

    def is_odd(ev: BaseEvent) -> bool:
        return not is_even(ev)

    await endpoint3.add_listener_endpoints(
        ListenerConfig.from_name(endpoint1.name, filter_predicate=is_even),
        ListenerConfig.from_name(endpoint2.name, filter_predicate=is_odd),
    )

    try:
        yield endpoint1, endpoint2, endpoint3
    finally:
        endpoint1.stop()
        endpoint2.stop()
        endpoint3.stop()


@pytest.mark.asyncio
async def test_broadcasts_to_all_endpoints(
        endpoints_with_filter_predicate: Tuple[Endpoint, Endpoint, Endpoint]) -> None:

    endpoint1, endpoint2, endpoint3 = endpoints_with_filter_predicate
    endpoint1_received = []
    endpoint2_received = []

    endpoint1.subscribe(
        DummyEvent,
        lambda ev: endpoint1_received.append(ev.payload)
    )

    endpoint2.subscribe(
        DummyEvent,
        lambda ev: endpoint2_received.append(ev.payload)
    )

    for i in range(10):
        endpoint3.broadcast(DummyEvent(i))

    await asyncio.sleep(0.01)

    assert endpoint1_received == [0, 2, 4, 6, 8]
    assert endpoint2_received == [1, 3, 5, 7, 9]
