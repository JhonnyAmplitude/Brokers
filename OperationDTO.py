from dataclasses import dataclass, asdict, field
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
    quantity: Optional[int]
    aci: Optional[Union[str, float]]
    comment: Optional[str]
    operation_id: Optional[str]
    _sort_key: Optional[str] = field(init=False, default=None)

    def __post_init__(self):
        if self.date:
            if isinstance(self.date, str) and len(self.date) == 10:
                self.date += " 00:00:00"
            self._sort_key = self.date
        else:
            self._sort_key = ""

    def to_dict(self):
        return asdict(self)
