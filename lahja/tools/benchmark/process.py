import asyncio
import logging
import multiprocessing
import time
from typing import (  # noqa: F401
    Optional,
)

from lahja import (
    BroadcastConfig,
    Endpoint,
)
from lahja.tools.benchmark.constants import (
    ROOT_ENDPOINT,
)
from lahja.tools.benchmark.stats import (
    GlobalStatistic,
    LocalStatistic,
)
from lahja.tools.benchmark.typing import (
    PerfMeasureEvent,
    RawMeasureEntry,
    ShutdownEvent,
    TotalRecordedEvent,
)
from lahja.tools.benchmark.utils.reporting import (
    print_full_report,
)


class DriverProcess:

    def __init__(self, num_events: int, event_bus: Endpoint) -> None:
        self._name = event_bus.name
        self._event_bus = event_bus
        self._num_events = num_events
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch,
            args=(self._event_bus, self._num_events)
        )
        self._process.start()

    @staticmethod
    def launch(event_bus: Endpoint, num_events: int) -> None:
        # UNCOMMENT FOR DEBUGGING
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)
        loop = asyncio.get_event_loop()
        event_bus.connect_no_wait()
        loop.run_until_complete(DriverProcess.worker(event_bus, num_events))

    @staticmethod
    async def worker(event_bus: Endpoint, num_events: int) -> None:
        for n in range(num_events):
            event_bus.broadcast(
                PerfMeasureEvent(n, time.time())
            )

        event_bus.stop()


class ConsumerProcess:

    def __init__(self, num_events: int, event_bus: Endpoint) -> None:
        self._name = event_bus.name
        self._event_bus = event_bus
        self._num_events = num_events
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch,
            args=(self._event_bus, self._num_events)
        )
        self._process.start()

    @staticmethod
    async def worker(event_bus: Endpoint, num_events: int) -> None:
        seen = 0
        stats = LocalStatistic()
        async for event in event_bus.stream(PerfMeasureEvent):
            stats.add(RawMeasureEntry(
                sent_at=event.sent_at,
                received_at=time.time()
            ))

            seen += 1

            if seen == num_events:
                event_bus.broadcast(
                    TotalRecordedEvent(stats.crunch(event_bus.name)),
                    BroadcastConfig(filter_endpoint="reporter")
                )
                event_bus.stop()
                break

    @staticmethod
    def launch(event_bus: Endpoint, num_events: int) -> None:
        # UNCOMMENT FOR DEBUGGING
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)

        loop = asyncio.get_event_loop()
        event_bus.connect_no_wait()

        loop.run_until_complete(ConsumerProcess.worker(event_bus, num_events))


class ReportingProcess:

    def __init__(self, num_reporters: int, event_bus: Endpoint) -> None:
        self._name = event_bus.name
        self._event_bus = event_bus
        self._num_reporters = num_reporters
        self._process: Optional[multiprocessing.Process] = None

    def start(self) -> None:
        self._process = multiprocessing.Process(
            target=self.launch,
            args=(self._event_bus, self._num_reporters)
        )
        self._process.start()

    @staticmethod
    async def worker(logger: logging.Logger, event_bus: Endpoint, num_reporters: int) -> None:
        global_statistic = GlobalStatistic()
        async for event in event_bus.stream(TotalRecordedEvent):

            global_statistic.add(event.total)
            if len(global_statistic) == num_reporters:
                print_full_report(logger, num_reporters, global_statistic)
                event_bus.broadcast(ShutdownEvent(), BroadcastConfig(filter_endpoint=ROOT_ENDPOINT))
                event_bus.stop()

    @staticmethod
    def launch(event_bus: Endpoint, num_reporters: int) -> None:
        logging.basicConfig(level=logging.INFO, format='%(message)s')
        logger = logging.getLogger('reporting')
        logger.setLevel(logging.INFO)

        loop = asyncio.get_event_loop()
        event_bus.connect_no_wait()

        loop.run_until_complete(ReportingProcess.worker(logger, event_bus, num_reporters))
