import requests
from datetime import date
from dataclasses import dataclass, asdict, field
from typing import List, Self, Dict, Any, Tuple
from pygeojson import Point, Feature
from data_types import ValueType, Properties, OutputStore

GEOMETRY_FIELD = "geom"


@dataclass
class AcledRequest:
    key: str
    email: str
    page: int
    iso: int
    event_date: str
    event_date_where: str = "BETWEEN"


@dataclass
class IsoCode:
    iso3: str
    code: int


def get_iso_codes(items: List[Tuple[str, str]]) -> List[IsoCode]:
    return [IsoCode(iso3, int(code)) for (iso3, code) in items]


@dataclass
class AcledSource:
    url: str
    email: str
    key: str
    countries_iso: List[IsoCode]
    start_date: date
    end_date: date
    properties: Properties = field(
        default_factory=lambda: {
            "actor1": ValueType.VARCHAR,
            "actor2": ValueType.VARCHAR,
            "admin1": ValueType.VARCHAR,
            "admin2": ValueType.VARCHAR,
            "admin3": ValueType.VARCHAR,
            "assoc_actor_1": ValueType.VARCHAR,
            "assoc_actor_2": ValueType.VARCHAR,
            "civilian_targeting": ValueType.VARCHAR,
            "country": ValueType.VARCHAR,
            "disorder_type": ValueType.VARCHAR,
            "event_date": ValueType.VARCHAR,
            "event_id_cnty": ValueType.VARCHAR,
            "event_type": ValueType.VARCHAR,
            "fatalities": ValueType.INT,
            "geo_precision": ValueType.INT,
            "inter1": ValueType.VARCHAR,
            "inter2": ValueType.VARCHAR,
            "interaction": ValueType.VARCHAR,
            "iso": ValueType.VARCHAR,
            "latitude": ValueType.FLOAT,
            "location": ValueType.VARCHAR,
            "longitude": ValueType.FLOAT,
            "notes": ValueType.VARCHAR,
            "region": ValueType.VARCHAR,
            "source": ValueType.VARCHAR,
            "source_scale": ValueType.VARCHAR,
            "sub_event_type": ValueType.VARCHAR,
            "tags": ValueType.VARCHAR,
            "time_precision": ValueType.VARCHAR,
            "timestamp": ValueType.VARCHAR,
            "year": ValueType.VARCHAR,
            GEOMETRY_FIELD: ValueType.POINT,
        }
    )

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
            self.fetch_and_store_from(iso.code, store)

    @staticmethod
    def objects_to_features(objects: List[Dict[str, Any]]) -> List[Feature]:
        return [
            Feature(geometry=Point(o["longitude"], o["latitude"]), properties=o)
            for o in objects
        ]

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

            features = AcledSource.objects_to_features(json_response["data"])
            store.save(json_response["data"])
            page += 1
