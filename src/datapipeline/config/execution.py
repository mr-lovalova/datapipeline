from pydantic import BaseModel, ConfigDict, Field, StrictInt


class ExecutionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    sort_batch_records: StrictInt = Field(default=100_000, ge=1)
