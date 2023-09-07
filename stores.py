import psycopg
from dataclasses import dataclass, asdict
from psycopg import sql
from typing import Self, List, Any, Dict, cast
from pygeojson import Feature, GeometryObject
from data_types import Properties, ValueType, GEOMETRY_FIELD
from shapely.geometry import shape  # type: ignore


@dataclass
class PgConnectionParams:
    host: str
    user: str
    password: str
    port: str
    dbname: str


@dataclass
class PgStore:
    table_name: str
    schema: str
    columns: Properties
    connection: PgConnectionParams

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

        conn_dict = asdict(self.connection)
        with psycopg.connect(**conn_dict) as conn:
            with conn.cursor() as cur:
                query = f"CREATE SCHEMA IF NOT EXISTS {self.schema}"
                cur.execute(query)

                query = f"CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name} ({columns_str})"
                cur.execute(query)

    @staticmethod
    def get_values(feature: Feature, columns: Properties) -> Dict[str, str]:
        properties = feature.properties
        values = {}
        for key, value_type in columns.items():
            if value_type not in (ValueType.INT, ValueType.FLOAT, ValueType.VARCHAR):
                geom_dict: Dict[str, Any] = asdict(
                    cast(GeometryObject, feature.geometry)
                )
                value = shape(geom_dict).wkb
            else:
                value = properties[key]
            values[key] = value

        return values

    def save(self: Self, features: List[Feature]) -> None:
        expressions = [
            f"%({key})s"
            if key != GEOMETRY_FIELD
            else f"ST_SETSRID(%({key})s::geometry, 4326)"
            for key in self.columns.keys()
        ]
        sql_expressions = [sql.SQL(e) for e in expressions]

        rows_values = [PgStore.get_values(f, self.columns) for f in features]

        sql_query = sql.SQL(
            "INSERT INTO {schema}.{table_name}({fields}) VALUES ({values})"
        ).format(
            schema=sql.Identifier(self.schema),
            table_name=sql.Identifier(self.table_name),
            fields=sql.SQL(",").join(
                [sql.Identifier(k) for k, _ in self.columns.items()]
            ),
            values=sql.SQL(",").join(sql_expressions)
        )

        with psycopg.connect(**asdict(self.connection)) as conn:
            with psycopg.ClientCursor(conn) as cur:
                [cur.execute(sql_query, r) for r in rows_values]
