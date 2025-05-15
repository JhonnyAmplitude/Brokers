from datetime import datetime
from dataclasses import dataclass, asdict, field
from typing import Optional, Union

@dataclass
class OperationDTO:
    date: Optional[Union[str, datetime]]
    operation_type: str
    payment_sum: Union[str, float]
    currency: str
    ticker: Optional[str] = ""
    isin: Optional[str] = ""
    price: Optional[float] = 0.0
    quantity: Optional[int] = 0
    aci: Optional[Union[str, float]] = 0.0
    comment: Optional[str] = ""
    operation_id: Optional[str] = ""
    _sort_key: Optional[str] = field(init=False, default=None)

    def __post_init__(self):
        if self.date:
            if isinstance(self.date, str):
                if len(self.date) == 10:
                    self.date += " 00:00:00"
                # Можно было бы даже преобразовать строку в datetime
                # self.date = datetime.strptime(self.date, "%Y-%m-%d %H:%M:%S")
            self._sort_key = str(self.date)
        else:
            self._sort_key = ""

        if isinstance(self.aci, str):
            try:
                self.aci = float(self.aci.replace(',', '.'))
            except ValueError:
                self.aci = 0.0


    def to_dict(self):
        result = asdict(self)
        # Если date - это datetime, преобразуем его в строку для сериализации
        if isinstance(self.date, datetime):
            result['date'] = self.date.isoformat()
        # Удаляем служебное поле
        if '_sort_key' in result:
            del result['_sort_key']
        return result