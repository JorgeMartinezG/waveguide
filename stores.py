import psycopg
from dataclasses import dataclass
from psycopg import sql
from typing import Self, Any, List
from pygeojson import Feature
from data_types import Properties, ValueType


@dataclass
class PgStore:
    table_name: str
    schema: str
    columns: Properties

    def init(self: Self) -> None:
        columns_list: List[str] = []
        for col_name, col_type in self.columns.items():
            match col_type:
                case ValueType.INT:
                    value = "BIGINT"
                case ValueType.FLOAT:
                    value = "REAL"
                case ValueType.POINT:
                    value = "GEOMETRY(POINT, 4326)"
                case _:
                    value = "VARCHAR"

            columns_list.append(f"{col_name} {value} NOT NULL")

        columns_str = ",\n".join(columns_list)

        query = f"CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name} ({columns_str})"

        print(query)

    def save(self: Self, features: List[Feature]) -> None:
        sql_query = sql.SQL("INSERT INTO {schema}.{table_name}({fields}) VALUES {values}").format(
            schema=sql.Identifier(self.schema),
            table_name=sql.Identifier(self.table_name),
            fields=sql.SQL(",").join([sql.Identifier(k) for k, _ in self.columns.items()]),
            values=["%s" for _,_ in self.columns.items()]
        )
        print(sql_query)

        print(f"Total features: {len(features)}")
