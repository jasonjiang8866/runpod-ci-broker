"""Small validated projections of documented REST v2 response fields.

Unknown fields are discarded. Validation errors must never leave the provider
boundary because raw Pod data can contain credentials.
"""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from .models import Nonnegative

Stock = Literal["LOW", "MEDIUM", "HIGH", "NONE"]
WireID = Annotated[str, Field(min_length=1, max_length=200)]


class Wire(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)


class DataCenterStock(Wire):
    id: WireID
    availability: Stock | None = None


class CudaStock(Wire):
    version: Annotated[str, Field(pattern=r"^\d+\.\d+$")]
    available: bool


class GPU(Wire):
    id: WireID
    memory: Nonnegative | None = None
    price: dict[str, Nonnegative] = {}
    maxCount: dict[str, Annotated[int, Field(ge=0)]] = {}
    availability: Stock | None = None
    dataCenters: list[DataCenterStock] = []
    cudaVersions: list[CudaStock] = []


class Pod(Wire):
    id: WireID
    status: Literal["PROVISIONING", "STARTING", "RUNNING", "EXITED", "ERROR", "TERMINATED"]
    env: dict[str, str]


class DataCenter(Wire):
    id: WireID
    region: str | None = None


class NetworkVolume(Wire):
    id: WireID
    dataCenter: WireID


class GlobalVolume(Wire):
    id: WireID
