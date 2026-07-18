from datapipeline.domain.record import TemporalRecord


def combine_valuation_inputs(price, earnings) -> TemporalRecord:
    record = TemporalRecord(time=price.time)
    record.ticker = price.ticker
    record.price = price.value
    record.earnings = earnings.value
    return record
