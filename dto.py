from dataclasses import dataclass
from typing import Optional

@dataclass
class OperationDTO:
    asset_type: str
    ticker: str
    date: str
    operation_number: Optional[str]
    quantity: float
    price: float
    amount: float
    accrued_coupon: Optional[float]  # только для облигаций
