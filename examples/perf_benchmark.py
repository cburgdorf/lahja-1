import argparse
import asyncio
import multiprocessing
import os
import signal

from lahja import (
    EventBus,
)

from lahja.tools.benchmark.constants import (
    ROOT_ENDPOINT,
)
from lahja.tools.benchmark.process import (
    ConsumerProcess,
    DriverProcess,
    ReportingProcess,
)
from lahja.tools.benchmark.typing import (
    ShutdownEvent,
)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--num-processes', type=int, default=10,
                        help='The number of processes listening for events')
    parser.add_argument('--num-events', type=int, default=100,
                        help='The number of processes listening for events')
    args = parser.parse_args()

    # WARNING: The `fork` method does not work well with asyncio yet.
    # This might change with Python 3.8 (See https://bugs.python.org/issue22087#msg318140)
    multiprocessing.set_start_method('spawn')

    # Configure and start event bus
    event_bus = EventBus()
    root = event_bus.create_endpoint(ROOT_ENDPOINT)
    root.connect_no_wait()

    event_bus.start()

    # In this benchmark, this is the only process that is flooding events
    driver = DriverProcess(args.num_events, event_bus.create_endpoint('e1'))

    # The reporter process is collecting statistical events from all consumer processes
    # For some reason, doing this work in the main process didn't end so well which is
    # why it was moved into a dedicated process. Notice that this will slightly skew results
    # as the reporter process will also receive events which we don't account for
    reporter = ReportingProcess(args.num_processes, event_bus.create_endpoint('reporter'))

    for n in range(args.num_processes):
        consumer_process = ConsumerProcess(
            args.num_events,
            event_bus.create_endpoint(f'consumer_{n}')
        )
        consumer_process.start()

    async def shutdown():
        await root.wait_for(ShutdownEvent)
        root.stop()
        event_bus.stop()
        asyncio.get_event_loop().stop()
        # Not entirely sure why we need to send a SIGTERM because all processes have shutdown
        # at this point and the main process should only block on this method which we have
        # obviously reached at this point.
        os.kill(os.getpid(), signal.SIGTERM)

    reporter.start()
    driver.start()
    asyncio.get_event_loop().run_until_complete(shutdown())
