import asyncio
import multiprocessing  # noqa: F401
from multiprocessing.queues import (
    Queue,
)
from types import (
    ModuleType,
)
from typing import (  # noqa: F401
    Dict,
    List,
)

from .async_util import (
    async_get,
)
from .endpoint import (
    Endpoint,
)
from .misc import (
    BroadcastConfig,
)


class EventBus:

    def __init__(self, ctx: ModuleType) -> None:
        self.ctx = ctx
        self._queues: List[multiprocessing.Queue] = []
        self._endpoints: Dict[str, Endpoint] = {}
        self._incoming_queue: Queue = Queue(0, ctx=self.ctx)
        self._running = False

    def create_endpoint(self, name: str) -> Endpoint:
        if name in self._endpoints:
            raise ValueError("An endpoint with that name does already exist")

        receiving_queue: Queue = Queue(0, ctx=self.ctx)

        endpoint = Endpoint(name, self._incoming_queue, receiving_queue)
        self._endpoints[name] = endpoint
        return endpoint

    def start(self) -> None:
        asyncio.ensure_future(self._start())

    async def _start(self) -> None:
        self._running = True
        while self._running:
            (item, config) = await async_get(self._incoming_queue)
            for endpoint in self._endpoints.values():

                if not self._is_allowed_to_receive(config, endpoint.name):
                    continue

                endpoint._receiving_queue.put_nowait((item, config))

    def _is_allowed_to_receive(self, config: BroadcastConfig, endpoint: str) -> bool:
        return config is None or config.allowed_to_receive(endpoint)

    def stop(self) -> None:
        self._running = False
        self._incoming_queue.close()
