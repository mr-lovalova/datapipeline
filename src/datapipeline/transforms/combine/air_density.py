import math
from typing import Any

from datapipeline.transforms.combine.base import (
    AlignedFeatureCombineTransform,
    AlignedFeatureRow,
)


class AirDensityCombineTransform(AlignedFeatureCombineTransform):
    """Compute air density from pressure, temperature, and optional humidity streams."""

    def __init__(
        self,
        *,
        temperature_id: str,
        humidity_id: str | None = None,
        pressure_units: str = "hpa",
        target_units: str = "kg/m3",
    ) -> None:
        super().__init__(required={temperature_id})
        self.temperature_id = temperature_id
        self.humidity_id = humidity_id
        self.pressure_units = pressure_units
        self.target_units = target_units

    def compute(self, row: AlignedFeatureRow) -> Any:
        pressure = row.record.record.value
        if pressure is None:
            return None
        temp_value = row.get(self.temperature_id)
        if temp_value is None:
            return None
        humidity_value = (
            row.get(self.humidity_id) if self.humidity_id else None
        )
        return self._density(pressure, temp_value, humidity_value)

    def _density(
        self,
        pressure: float | int,
        temperature_c: float | int,
        humidity_percent: float | int | None,
    ) -> float:
        pressure_pa = float(pressure)
        if self.pressure_units.lower() in {"hpa", "millibar", "mb"}:
            pressure_pa *= 100.0

        temp_k = float(temperature_c) + 273.15
        density = pressure_pa / (287.05 * temp_k)

        if humidity_percent is not None:
            rh = float(humidity_percent)
            if rh > 1.0:
                rh /= 100.0
            saturation = 6.112 * math.exp((17.67 * float(temperature_c)) / (float(temperature_c) + 243.5))
            vapor_pressure = rh * saturation * 100.0
            density = (pressure_pa - 0.378 * vapor_pressure) / (287.05 * temp_k)
        return density
