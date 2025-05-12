from dataclasses import dataclass, asdict
from typing import Optional, Union

@dataclass
class OperationDTO:
    date: Union[str, None]
    operation_type: str
    payment_sum: Union[str, float]
    currency: str
    ticker: Optional[str]
    isin: Optional[str]
    price: Optional[float]
    quantity: Optional[float]
    aci: Optional[Union[str, float]]
    comment: Optional[str]
    operation_id: Optional[str]
    _sort_key: Optional[str] = None

    def to_dict(self):
        return asdict(self)
