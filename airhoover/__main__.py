import clickhouse_connect

import time


class AirHoover:
    DATA_URL = "https://api.airplanes.live/v2/point/{lat}/{lon}/{radius}"

    def __init__(self, lat: float, lon: float, radius: float):
        self.ch = clickhouse_connect.get_client(host="localhost", username="default", password="")
        self.lat = lat
        self.lon = lon
        self.radius = radius

    def create_table_maybe(self) -> None:
        create_table_sql = """CREATE TABLE IF NOT EXISTS aircraft_tracking
(
    `time_ingested` DateTime,
    `hex_id` String,
    `hex` Nullable(String),
    `type` Nullable(String),
    `flight` Nullable(String),
    `r` Nullable(String),
    `t` Nullable(String),
    `desc` Nullable(String),
    `ownOp` Nullable(String),
    `alt_baro` Nullable(String),
    `alt_geom` Nullable(Int64),
    `gs` Nullable(Float64),
    `ias` Nullable(Int64),
    `tas` Nullable(Int64),
    `mach` Nullable(Float64),
    `wd` Nullable(Int64),
    `ws` Nullable(Int64),
    `oat` Nullable(Int64),
    `tat` Nullable(Int64),
    `track` Nullable(Float64),
    `track_rate` Nullable(Float64),
    `roll` Nullable(Float64),
    `mag_heading` Nullable(Float64),
    `true_heading` Nullable(Float64),
    `baro_rate` Nullable(Int64),
    `geom_rate` Nullable(Int64),
    `squawk` Nullable(String),
    `emergency` Nullable(String),
    `category` Nullable(String),
    `nav_qnh` Nullable(Float64),
    `nav_altitude_mcp` Nullable(Int64),
    `nav_altitude_fms` Nullable(Int64),
    `nav_heading` Nullable(Float64),
    `lat` Nullable(Float64),
    `lon` Nullable(Float64),
    `nic` Nullable(Int64),
    `rc` Nullable(Int64),
    `seen_pos` Nullable(Float64),
    `version` Nullable(Int64),
    `nic_baro` Nullable(Int64),
    `nac_p` Nullable(Int64),
    `nac_v` Nullable(Int64),
    `sil` Nullable(Int64),
    `sil_type` Nullable(String),
    `gva` Nullable(Int64),
    `sda` Nullable(Int64),
    `alert` Nullable(Int64),
    `spi` Nullable(Int64),
    `mlat` Array(Nullable(String)),
    `tisb` Array(Nullable(String)),
    `messages` Nullable(Int64),
    `seen` Nullable(Float64),
    `rssi` Nullable(Float64),
    `dst` Nullable(Float64),
    `dir` Nullable(Float64),
    `year` Nullable(String),
    `nav_modes` Array(Nullable(String))
)
ENGINE = MergeTree
ORDER BY (hex_id, time_ingested)
TTL time_ingested + toIntervalDay(7)
SETTINGS index_granularity = 8192
"""

        self.ch.raw_query(create_table_sql)

    def update_loop(self, interval: int = 2):
        min_next_update = 0

        while True:
            try:
                if min_next_update < (now := time.monotonic()):
                    print("updating")
                    min_next_update = now + interval
                    self.update()

                time.sleep(0.01)
            except Exception:
                print("Error, will try again...")

    def update(self):
        url = self.DATA_URL.format(lat=self.lat, lon=self.lon, radius=self.radius)
        query = f"""INSERT INTO aircraft_tracking SELECT
            now() AS time_ingested,
            ifNull(hex, '') AS hex_id,
            hex,
            type,
            flight,
            r,
            t,
            desc,
            ownOp,
            alt_baro,
            alt_geom,
            gs,
            ias,
            tas,
            mach,
            wd,
            ws,
            oat,
            tat,
            track,
            track_rate,
            roll,
            mag_heading,
            true_heading,
            baro_rate,
            geom_rate,
            squawk,
            emergency,
            category,
            nav_qnh,
            nav_altitude_mcp,
            nav_altitude_fms,
            nav_heading,
            lat,
            lon,
            nic,
            rc,
            seen_pos,
            version,
            nic_baro,
            nac_p,
            nac_v,
            sil,
            sil_type,
            gva,
            sda,
            alert,
            spi,
            mlat,
            tisb,
            messages,
            seen,
            rssi,
            dst,
            dir,
            year,
            nav_modes
        FROM format((
            SELECT JSONExtractString(json, 'ac')
            FROM url('https://api.airplanes.live/v2/point/{self.lat}/{self.lon}/{self.radius}', JSONAsString)
        ))"""

        self.ch.raw_query(query)


if __name__ == "__main__":
    hoover = AirHoover(52.392363, -1.610521, 250)
    hoover.update_loop(10)  # Trigger every 10 seconds
