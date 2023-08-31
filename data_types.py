from typing import Dict, Self, Any
from typing_extensions import Protocol
from enum import Enum


class OutputStore(Protocol):
    def save(self: Self, features: Any) -> None:
        ...


class InputSource(Protocol):
    def fetch_and_store(self: Self, output: OutputStore) -> None:
        ...


class PgType(Enum):
    VARCHAR = 0
    INT = 1
    FLOAT = 2
    POINT = 3


Properties = Dict[str, PgType]
