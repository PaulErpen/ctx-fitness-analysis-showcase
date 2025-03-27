from enum import Enum
from typing import Any, List

class ListableStringEnum(str, Enum):
    @classmethod
    def values(cls) -> List[str]:
        return [e.value for e in cls]