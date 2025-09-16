from dataclasses import dataclass


@dataclass(frozen=True)
class Point:
    lon: float
    lat: float

    def to_tuple(self) -> tuple[float, float]:
        return (self.lon, self.lat)

    def to_geojson(self) -> dict:
        return {"type": "Point", "coordinates": [self.lon, self.lat]}
