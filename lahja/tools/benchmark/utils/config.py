from typing import (
    Tuple,
)

from lahja import (
    ListenerConfig,
)


def create_consumer_endpoint_configs(num_processes: int) -> Tuple[ListenerConfig, ...]:
    return tuple(
        ListenerConfig.from_name(create_consumer_endpoint_name(i)) for i in range(num_processes)
    )


def create_consumer_endpoint_name(id: int) -> str:
    return f"consumer_{id}"
