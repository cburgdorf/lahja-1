import asyncio
from concurrent.futures.thread import (
    ThreadPoolExecutor,
)
import multiprocessing
from typing import (  # noqa: F401
    Any,
    AsyncIterable,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    cast,
)
import uuid

from cancel_token import (
    CancelToken,
    OperationCancelled,
)

from .async_util import (
    async_get,
)
from .exceptions import (
    NoConnection,
    UnexpectedResponse,
)
from .misc import (
    TRANSPARENT_EVENT,
    BaseEvent,
    BaseRequestResponseEvent,
    BroadcastConfig,
)


class Endpoint:

    def __init__(self,
                 name: str,
                 sending_queue: multiprocessing.Queue,
                 receiving_queue: multiprocessing.Queue) -> None:

        self.name = name
        self._sending_queue = sending_queue
        self._receiving_queue = receiving_queue
        self._futures: Dict[str, asyncio.Future] = {}
        self._handler: Dict[Type[BaseEvent], List[Callable[[BaseEvent], Any]]] = {}
        self._queues: Dict[Type[BaseEvent], List[asyncio.Queue]] = {}
        self._running = False
        self._executor: Optional[ThreadPoolExecutor] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            raise NoConnection("Need to call `connect()` first")

        return self._loop

    def connect(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """
        Connect the :class:`~lahja.endpoint.Endpoint` to the :class:`~lahja.eventbus.EventBus`
        instance that created this endpoint.
        """
        if (loop is None):
            loop = asyncio.get_event_loop()

        self._loop = loop
        asyncio.ensure_future(self._try_connect(loop), loop=loop)

    async def _try_connect(self, loop: asyncio.AbstractEventLoop) -> None:
        # We need to handle exceptions here to not get `Task exception was never retrieved`
        # errors in case the `connect()` fails (e.g. because the remote process is shutting down)
        try:
            await asyncio.ensure_future(self._connect(), loop=loop)
        except Exception as exc:
            raise Exception("Exception from Endpoint.connect()") from exc

    async def _connect(self) -> None:
        self._running = True
        self._executor = ThreadPoolExecutor()
        while self._running:
            (item, config) = await async_get(self._receiving_queue, executor=self._executor)

            if item is TRANSPARENT_EVENT:
                continue

            has_config = config is not None

            event_type = type(item)
            in_futures = has_config and config.filter_event_id in self._futures
            in_queue = event_type in self._queues
            in_handler = event_type in self._handler

            if not in_queue and not in_handler and not in_futures:
                continue

            if in_futures:
                future = self._futures[config.filter_event_id]
                future.set_result(item)
                self._futures.pop(config.filter_event_id)

            if in_queue:
                for queue in self._queues[event_type]:
                    queue.put_nowait(item)

            if in_handler:
                for handler in self._handler[event_type]:
                    handler(item)

    def stop(self) -> None:
        """
        Stop the :class:`~lahja.endpoint.Endpoint` from receiving further events, effectively
        disconnecting it from the to the :class:`~lahja.eventbus.EventBus` that created it.
        """
        if not self._running:
            return

        self._running = False
        self._receiving_queue.put_nowait((TRANSPARENT_EVENT, None))
        if self._executor is not None:
            self._executor.shutdown()
        self._receiving_queue.close()

    def broadcast(self, item: BaseEvent, config: Optional[BroadcastConfig] = None) -> None:
        """
        Broadcast an instance of :class:`~lahja.misc.BaseEvent` on the event bus. Takes
        an optional second parameter of :class:`~lahja.misc.BroadcastConfig` to decide
        where this event should be broadcasted to. By default, events are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        item._origin = self.name
        self._sending_queue.put_nowait((item, config))

    TResponse = TypeVar('TResponse', bound=BaseEvent)

    async def request(self,
                      item: BaseRequestResponseEvent[TResponse],
                      cancel_token: Optional[CancelToken] = None) -> TResponse:
        """
        Broadcast an instance of :class:`~lahja.misc.BaseEvent` on the event bus and immediately
        wait on an expected answer of type :class:`~lahja.misc.BaseEvent`.
        """
        item._origin = self.name
        item._id = str(uuid.uuid4())

        future: asyncio.Future = asyncio.Future()
        self._futures[item._id] = future

        self._sending_queue.put_nowait((item, None))

        token = self._chain_or_create_cancel_token(f'request#{item}', cancel_token)

        try:
            result = await token.cancellable_wait(future)
        except OperationCancelled as e:
            del self._futures[item._id]
            raise e

        expected_response_type = item.expected_response_type()
        if not isinstance(result, expected_response_type):
            raise UnexpectedResponse(
                f"The type of the response is {type(result)}, expected: {expected_response_type}"
            )

        return result

    TSubscribeEvent = TypeVar('TSubscribeEvent', bound=BaseEvent)

    def subscribe(self,
                  event_type: Type[TSubscribeEvent],
                  handler: Callable[[TSubscribeEvent], None],
                  cancel_token: Optional[CancelToken] = None) -> CancelToken:
        """
        Subscribe to receive updates for any event that matches the specified event type.
        A handler is passed as a second argument an :class:`~lahja.misc.Subscription` is returned
        to unsubscribe from the event if needed.
        """
        if event_type not in self._handler:
            self._handler[event_type] = []

        casted_handler = cast(Callable[[BaseEvent], Any], handler)

        self._handler[event_type].append(casted_handler)

        token = self._chain_or_create_cancel_token(f'subscribe#{event_type}', cancel_token)

        asyncio.ensure_future(self._run_when_triggered(
            token,
            lambda: self._handler[event_type].remove(casted_handler)
        ), loop=self.loop)

        return token

    TStreamEvent = TypeVar('TStreamEvent', bound=BaseEvent)

    async def stream(self,
                     event_type: Type[TStreamEvent],
                     cancel_token: Optional[CancelToken] = None,
                     max: Optional[int] = None) -> AsyncIterable[TStreamEvent]:
        """
        Stream all events that match the specified event type. This returns an
        ``AsyncIterable[BaseEvent]`` which can be consumed through an ``async for`` loop.
        An optional ``max`` parameter can be passed to stop streaming after a maximum amount
        of events was received.
        """
        queue: asyncio.Queue = asyncio.Queue()

        if event_type not in self._queues:
            self._queues[event_type] = []

        self._queues[event_type].append(queue)

        token = self._chain_or_create_cancel_token(f'stream#{event_type}', cancel_token)

        i = None if max is None else 0
        while True:
            try:
                event = await token.cancellable_wait(queue.get())
            except OperationCancelled:
                self._queues[event_type].remove(queue)
                break

            if i is not None:
                i += 1
            try:
                yield event
            except GeneratorExit:
                token.trigger()
            else:
                if i is not None and i >= cast(int, max):
                    token.trigger()

    TWaitForEvent = TypeVar('TWaitForEvent', bound=BaseEvent)

    async def wait_for(self,
                       event_type: Type[TWaitForEvent],
                       cancel_token: Optional[CancelToken] = None) -> TWaitForEvent:
        """
        Wait for a single instance of an event that matches the specified event type.
        """

        token = self._chain_or_create_cancel_token(f'wait_for#{event_type}', cancel_token)

        async for event in self.stream(event_type, max=1, cancel_token=token):
            return event

        # We can only get here if the token was triggered
        raise OperationCancelled("Operation was cancelled")

    def _raise_if_loop_missing(self) -> None:
        if self._loop is None:
            raise NoConnection("Need to call `connect()` first")

    async def _run_when_triggered(self, token: CancelToken, handler: Callable[..., Any]) -> None:
        await token.wait()
        handler()

    def _chain_or_create_cancel_token(self,
                                      token_name: str,
                                      token: Optional[CancelToken] = None) -> CancelToken:

        return (CancelToken(token_name, self.loop).chain(token)
                if token is not None else
                CancelToken(token_name, self.loop))
