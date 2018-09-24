import asyncio
from typing import (
    Any,
    Type,
)

from cancel_token import (
    CancelToken,
)
import pytest

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
    EventBus,
    UnexpectedResponse,
)


class DummyRequest(BaseEvent):
    property_of_dummy_request = None


class DummyResponse(BaseEvent):
    property_of_dummy_response = None

    def __init__(self, something: Any) -> None:
        pass


class DummyRequestPair(BaseRequestResponseEvent[DummyResponse]):
    property_of_dummy_request_pair = None

    @staticmethod
    def expected_response_type() -> Type[DummyResponse]:
        return DummyResponse


@pytest.mark.asyncio
async def test_can_unsubscribe() -> None:
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect()

    cancel_token = endpoint.subscribe(
        DummyRequestPair,
        lambda ev: endpoint.broadcast(
            # Accessing `ev.property_of_dummy_request_pair` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            DummyResponse(ev.property_of_dummy_request_pair), ev.broadcast_config()
        )
    )

    # This is just to prove that we are getting answers when subscribed
    response1 = await endpoint.request(DummyRequestPair())
    assert isinstance(response1, DummyResponse)

    cancel_token.trigger()

    # This would hang forever because we unsubscribed
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(endpoint.request(DummyRequestPair()), timeout=0.1)

    endpoint.stop()
    bus.stop()


@pytest.mark.asyncio
async def test_can_unsubscribe_with_passed_token() -> None:
    event_loop = asyncio.get_event_loop()
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect(event_loop)

    parent_cancel_token = CancelToken('parent', event_loop)

    endpoint.subscribe(
        DummyRequestPair,
        lambda ev: endpoint.broadcast(
            # Accessing `ev.property_of_dummy_request_pair` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            DummyResponse(ev.property_of_dummy_request_pair), ev.broadcast_config()
        ),
        parent_cancel_token
    )

    # This is just to prove that we are getting answers when subscribed
    response1 = await endpoint.request(DummyRequestPair())
    assert isinstance(response1, DummyResponse)

    parent_cancel_token.trigger()

    # This would hang forever because we unsubscribed
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(endpoint.request(DummyRequestPair()), timeout=0.1)

    endpoint.stop()
    bus.stop()


@pytest.mark.asyncio
async def test_request() -> None:
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect()

    endpoint.subscribe(
        DummyRequestPair,
        lambda ev: endpoint.broadcast(
            # Accessing `ev.property_of_dummy_request_pair` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            DummyResponse(ev.property_of_dummy_request_pair), ev.broadcast_config()
        )
    )

    response = await endpoint.request(DummyRequestPair())
    # Accessing `ev.property_of_dummy_response` here allows us to validate
    # mypy has the type information we think it has. We run mypy on the tests.
    print(response.property_of_dummy_response)
    assert isinstance(response, DummyResponse)
    endpoint.stop()
    bus.stop()


@pytest.mark.asyncio
async def test_response_must_match() -> None:
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect()

    endpoint.subscribe(
        DummyRequestPair,
        lambda ev: endpoint.broadcast(
            # We intentionally broadcast an unexpected response. Mypy can't catch
            # this but we ensure it is caught and raised during the processing.
            DummyRequest(), ev.broadcast_config()
        )
    )

    with pytest.raises(UnexpectedResponse):
        await endpoint.request(DummyRequestPair())
    endpoint.stop()
    bus.stop()


@pytest.mark.asyncio
async def test_stream_with_max() -> None:
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect()
    stream_counter = 0

    async def stream_response() -> None:
        async for event in endpoint.stream(DummyRequest, max=2):
            # Accessing `ev.property_of_dummy_request` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            print(event.property_of_dummy_request)
            nonlocal stream_counter
            stream_counter += 1

    asyncio.ensure_future(stream_response())

    # we broadcast one more item than what we consume and test for that
    for i in range(3):
        endpoint.broadcast(DummyRequest())

    await asyncio.sleep(0.1)
    assert len(endpoint._queues[DummyRequest]) == 0
    endpoint.stop()
    bus.stop()
    assert stream_counter == 2


@pytest.mark.asyncio
async def test_stream_with_cancellation() -> None:
    event_loop = asyncio.get_event_loop()
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect(event_loop)
    stream_counter = 0

    cancel_token = CancelToken('test', event_loop)

    async def stream_response() -> None:
        async for event in endpoint.stream(DummyRequest, cancel_token=cancel_token):
            # Accessing `ev.property_of_dummy_request` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            print(event.property_of_dummy_request)
            nonlocal stream_counter
            stream_counter += 1

    async def request() -> None:
        # we broadcast one more item than what we consume and test for that
        for i in range(10):
            endpoint.broadcast(DummyRequest())
            # We need to yield back to the event loop. Otherwise responses will not
            # even have a chance to kick in
            await asyncio.sleep(0.01)

            if i == 2:
                cancel_token.trigger()

    asyncio.ensure_future(stream_response(), loop=event_loop)
    await request()
    # Ensure the registration was cleaned up
    assert len(endpoint._queues[DummyRequest]) == 0
    endpoint.stop()
    bus.stop()
    assert stream_counter == 3


@pytest.mark.asyncio
async def test_wait_for() -> None:
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect()
    received = None

    async def stream_response() -> None:
        request = await endpoint.wait_for(DummyRequest)
        # Accessing `ev.property_of_dummy_request` here allows us to validate
        # mypy has the type information we think it has. We run mypy on the tests.
        print(request.property_of_dummy_request)
        nonlocal received
        received = request

    asyncio.ensure_future(stream_response())
    endpoint.broadcast(DummyRequest())

    await asyncio.sleep(0.01)
    endpoint.stop()
    bus.stop()
    assert isinstance(received, DummyRequest)
