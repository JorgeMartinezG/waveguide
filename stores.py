import psycopg
from dataclasses import dataclass
from psycopg import sql
from typing import Self, Any


@dataclass
class PgStore:
    def save(self: Self, features: Any) -> None:
        print(f"Total features: {len(features)}")
