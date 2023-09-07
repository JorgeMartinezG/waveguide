from typing import Dict, Self, List
from typing_extensions import Protocol
from pygeojson import Feature
from enum import Enum


class OutputStore(Protocol):
    def init(self: Self) -> None:
        ...

    def save(self: Self, features: List[Feature]) -> None:
        ...


class InputSource(Protocol):
    def fetch_and_store(self: Self, output: OutputStore) -> None:
        ...


class ValueType(Enum):
    VARCHAR = 0
    INT = 1
    FLOAT = 2
    POINT = 3


Properties = Dict[str, ValueType]
