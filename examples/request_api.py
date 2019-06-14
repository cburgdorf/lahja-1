import asyncio
import multiprocessing
from dataclasses import dataclass

from lahja import (
    AsyncioEndpoint,
    BaseEvent,
    BaseRequestResponseEvent,
    ConnectionConfig,
)


@dataclass
class DeliverSomethingResponse(BaseEvent):
    payload: str

# Define request / response pair
class GetSomethingRequest(BaseRequestResponseEvent[DeliverSomethingResponse]):
    @staticmethod
    def expected_response_type():
        return DeliverSomethingResponse


# Base functions for first process
def spawn_proc1():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_proc1())


async def run_proc1():
    config = ConnectionConfig.from_name("e1")
    async with AsyncioEndpoint.serve(config) as server:
        async for event in server.stream(GetSomethingRequest, num_events=3):
            await server.broadcast(
                DeliverSomethingResponse("Yay"), event.broadcast_config()
            )


# Base functions for second process
def run_proc2():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(proc2_worker())


async def proc2_worker():
    config = ConnectionConfig.from_name("e1")
    async with AsyncioEndpoint("e2").run() as client:
        await client.connect_to_endpoints(config)
        await client.wait_until_any_endpoint_subscribed_to(GetSomethingRequest)

        for i in range(3):
            print("Requesting")
            result = await client.request(GetSomethingRequest())
            print(f"Got answer: {result.payload}")


if __name__ == "__main__":

    # WARNING: The `fork` method does not work well with asyncio yet.
    # This might change with Python 3.8 (See https://bugs.python.org/issue22087#msg318140)
    multiprocessing.set_start_method("spawn")

    p1 = multiprocessing.Process(target=spawn_proc1)
    p1.start()

    p2 = multiprocessing.Process(target=run_proc2)
    p2.start()
    p1.join()
