import asyncio
import multiprocessing
import time
import sys

from lahja import BaseEvent, AsyncioEndpoint, ConnectionConfig


# The example in this file runs infinitely until the user stops the program.
# To be able to test the example and run assertions on the live output, we
# need to ensure the output won't get buffered.
def print_line(line, *args):
    print(line, *args, flush=True)


class BaseExampleEvent(BaseEvent):
    def __init__(self, payload):
        super().__init__()
        self.payload = payload


# Define two different events
class FirstThingHappened(BaseExampleEvent):
    pass


class SecondThingHappened(BaseExampleEvent):
    pass


# Base functions for first process
def run_proc1():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(proc1_worker())


async def proc1_worker():
    async with AsyncioEndpoint.serve(ConnectionConfig.from_name("e1")) as endpoint:
        await endpoint.connect_to_endpoints(ConnectionConfig.from_name("e2"))
        await endpoint.subscribe(
            SecondThingHappened,
            lambda event: print_line(
                "Received via SUBSCRIBE API in proc1: ", event.payload
            ),
        )
        await endpoint.subscribe(
            FirstThingHappened,
            lambda event: print_line("Receiving own event: ", event.payload),
        )

        while True:
            print_line("Hello from proc1")
            if is_nth_second(5):
                await endpoint.broadcast(
                    FirstThingHappened("Hit from proc1 ({})".format(time.time()))
                )
            await asyncio.sleep(1)


# Base functions for second process
def run_proc2():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(proc2_worker())


async def proc2_worker():
    async with AsyncioEndpoint.serve(ConnectionConfig.from_name("e2")) as endpoint:
        await endpoint.connect_to_endpoints(ConnectionConfig.from_name("e1"))
        asyncio.ensure_future(display_proc1_events(endpoint))
        await endpoint.subscribe(
            FirstThingHappened,
            lambda event: print_line(
                "Received via SUBSCRIBE API in proc2:", event.payload
            ),
        )
        while True:
            print_line("Hello from proc2")
            if is_nth_second(2):
                await endpoint.broadcast(
                    SecondThingHappened("Hit from proc2 ({})".format(time.time()))
                )
            await asyncio.sleep(1)


async def display_proc1_events(endpoint):
    async for event in endpoint.stream(FirstThingHappened):
        print_line("Received via STREAM API in proc2: ", event.payload)


# Helper function to send events every n seconds
def is_nth_second(interval):
    return int(time.time()) % interval is 0


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")

    # Start two processes
    p1 = multiprocessing.Process(target=run_proc1)
    p1.start()

    p2 = multiprocessing.Process(target=run_proc2)
    p2.start()
    p1.join()
    p2.join()
