from __future__ import annotations

from typing import Iterable, List

SHARED_PLAN_CODE_OPTIONS: list[str] = [
    "115CA006/致癌計畫",
    "115CA008/SDG計畫",
    "115CA011/化學品計畫",
    "115CA012/國際交流計畫",
    "115GO003/傳統茶品計畫",
    "115GO004/防檢署計畫",
    "115GO005/指定茶品計畫",
    "115ID003/產發署計畫",
    "115IS002/民營工服",
    "115GA002/管理處",
    "115GA003/化安處共同支出",
]


def get_shared_plan_code_options(extra_values: Iterable[str] | None = None, include_other: bool = True) -> List[str]:
    values: List[str] = []
    for v in SHARED_PLAN_CODE_OPTIONS:
        if v not in values:
            values.append(v)
    for v in extra_values or []:
        s = str(v).strip()
        if s and s not in values:
            values.append(s)
    if include_other and "其他" not in values:
        values.append("其他")
    return values
