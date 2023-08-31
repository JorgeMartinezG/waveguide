import requests
from dataclasses import dataclass, asdict
from typing import List, Self, Any, Dict, TypedDict
from typing_extensions import Protocol
from datetime import date, timedelta
from enum import Enum
from protocols import CHUNK_SIZE

GEOMETRY_FIELD = "geom"


class OutputStore(Protocol):
    def save(self: Self, features: Any) -> None:
        ...


class InputSource(Protocol):
    def fetch_and_store(self: Self, output: OutputStore) -> None:
        ...


class DummyStore:
    def save(self: Self, features: Any) -> None:
        print(f"Total features: {len(features)}")


@dataclass
class AcledRequest:
    key: str
    email: str
    page: int
    iso: int
    event_date: str
    event_date_where: str = "BETWEEN"


@dataclass
class WaveGuide:
    input_source: InputSource
    output_store: OutputStore

    def process(self: Self) -> None:
        self.input_source.fetch_and_store(self.output_store)


class PgType(Enum):
    VARCHAR = 0
    INT = 1
    FLOAT = 2
    POINT = 3


@dataclass
class AcledSource:
    url: str
    email: str
    key: str
    countries_iso: List[int]
    start_date: date
    end_date: date
    fields: Dict[str, PgType] = {
        "actor1": PgType.VARCHAR,
        "actor2": PgType.VARCHAR,
        "admin1": PgType.VARCHAR,
        "admin2": PgType.VARCHAR,
        "admin3": PgType.VARCHAR,
        "assoc_actor_1": PgType.VARCHAR,
        "assoc_actor_2": PgType.VARCHAR,
        "civilian_targeting": PgType.VARCHAR,
        "country": PgType.VARCHAR,
        "disorder_type": PgType.VARCHAR,
        "event_date": PgType.VARCHAR,
        "event_id_cnty": PgType.VARCHAR,
        "event_type": PgType.VARCHAR,
        "fatalities": PgType.INT,
        "geo_precision": PgType.INT,
        "inter1": PgType.VARCHAR,
        "inter2": PgType.VARCHAR,
        "interaction": PgType.VARCHAR,
        "iso": PgType.VARCHAR,
        "latitude": PgType.FLOAT,
        "location": PgType.VARCHAR,
        "longitude": PgType.FLOAT,
        "notes": PgType.VARCHAR,
        "region": PgType.VARCHAR,
        "source": PgType.VARCHAR,
        "source_scale": PgType.VARCHAR,
        "sub_event_type": PgType.VARCHAR,
        "tags": PgType.VARCHAR,
        "time_precision": PgType.VARCHAR,
        "timestamp": PgType.VARCHAR,
        "year": PgType.VARCHAR,
        GEOMETRY_FIELD: PgType.POINT,
    }

    def build_request_params(self: Self, iso: int, page: int) -> AcledRequest:
        start_date_str = self.start_date.isoformat()
        end_date_str = self.end_date.isoformat()

        return AcledRequest(
            key=self.key,
            email=self.email,
            page=page,
            iso=iso,
            event_date=f"{start_date_str}|{end_date_str}",
            event_date_where="BETWEEN",
        )

    def fetch_and_store(self: Self, store: OutputStore) -> None:
        for iso in self.countries_iso:
            self.fetch_and_store_from(iso, store)

    def fetch_and_store_from(self: Self, iso: int, store: OutputStore) -> None:
        page: int = 1
        while True:
            params = self.build_request_params(iso, page)
            resp = requests.get(self.url, params=asdict(params))

            resp.raise_for_status()

            json_response = resp.json()
            count = json_response["count"]
            if count == 0:
                break

            store.save(json_response["data"])
            page += 1


def main() -> None:
    today = date.today()
    acled_source = AcledSource(
        "https://api.acleddata.com/acled/read",
        "",
        "",
        [430],
        today - timedelta(days=365 * 2),
        today,
    )

    wave_guide = WaveGuide(acled_source, DummyStore())

    wave_guide.process()


if __name__ == "__main__":
    main()
