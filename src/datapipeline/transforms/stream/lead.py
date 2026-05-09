from datapipeline.transforms.stream.period_shift import PeriodShiftTransformer


class LeadTransformer(PeriodShiftTransformer):
    def __init__(
        self,
        *,
        field: str,
        periods: int,
        to: str | None = None,
        partition_by: str | list[str] | None = None,
    ) -> None:
        super().__init__(
            field=field,
            periods=periods,
            direction="lead",
            to=to,
            partition_by=partition_by,
        )
