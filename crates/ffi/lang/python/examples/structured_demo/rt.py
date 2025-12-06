from __future__ import annotations

from typing import Dict, List


def translate_point(pt: Dict[str, int], dx: int = 1, dy: int = -1) -> Dict[str, int]:
    return {"x": int(pt["x"]) + int(dx), "y": int(pt["y"]) + int(dy)}


def flip_mode(mode: Dict[str, object]) -> Dict[str, object]:
    name = mode.get("name")
    if name == "A":
        return {"name": "B", "value": {"x": 7, "y": 9}}
    return {"name": "A", "value": 1}


def map_len(m: Dict[str, int]) -> int:
    return len(m)


def list_sum(items: List[int]) -> int:
    return int(sum(items))
