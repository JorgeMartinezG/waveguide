from dataclasses import dataclass
from typing import Self
from configparser import ConfigParser
from datetime import datetime, date
from sources import AcledSource, get_iso_codes
from stores import PgStore, PgConnectionParams
from data_types import InputSource, OutputStore

DATE_FORMAT = "%Y-%m-%d"


@dataclass
class WaveGuide:
    input_source: InputSource
    output_store: OutputStore

    def process(self: Self) -> None:
        self.input_source.fetch_and_store(self.output_store)


def main() -> None:
    today = date.today()
    acled_config = ConfigParser()
    acled_config.read("./configs/acled.toml")

    acled_codes = get_iso_codes(acled_config.items("codes"))

    start_date_str = acled_config.get("api", "start_date")
    start_date = datetime.strptime(start_date_str, DATE_FORMAT).date()

    end_date_str = acled_config.get("api", "end_date", fallback=None)
    end_date = (
        today
        if end_date_str is None
        else datetime.strptime(end_date_str, DATE_FORMAT).date()
    )

    acled_source = AcledSource(
        acled_config.get("api", "url"),
        acled_config.get("api", "email"),
        acled_config.get("api", "key"),
        acled_codes,
        start_date,
        end_date,
    )

    pg_config = ConfigParser()
    pg_config.read("./configs/postgres.toml")

    connection = PgConnectionParams(**dict(pg_config["database"]))
    db_schema = pg_config.get("schema", "name")
    table_name = acled_config.get("layer", "name")

    store = PgStore(table_name, db_schema, acled_source.properties, connection)
    store.init()
    breakpoint()

    store.save([])
    # wave_guide = WaveGuide(acled_source, store)

    # wave_guide.process()


if __name__ == "__main__":
    main()
