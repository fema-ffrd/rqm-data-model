"""The demo notebooks share one Iceberg warehouse (notebooks/README.md)."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

NOTEBOOKS = Path(__file__).resolve().parent.parent / "notebooks"
DEMOS = [
    "01_setup_demo.ipynb",
    "02_versioning_demo.ipynb",
    "03_multi_source_demo.ipynb",
]
SHARED_WAREHOUSE = 'WAREHOUSE = DATA_DIR / "warehouse-notebook"'


def code_lines(name: str) -> list[str]:
    cells = json.loads((NOTEBOOKS / name).read_text())["cells"]
    return [
        line.strip()
        for cell in cells
        if cell["cell_type"] == "code"
        for line in "".join(cell["source"]).splitlines()
    ]


@pytest.mark.parametrize("name", DEMOS)
def test_demo_uses_shared_warehouse(name):
    assignments = [l for l in code_lines(name) if re.match(r"WAREHOUSE\s*=", l)]
    assert assignments == [SHARED_WAREHOUSE]


@pytest.mark.parametrize("name", DEMOS)
def test_only_setup_demo_resets_warehouse(name):
    resets = [l for l in code_lines(name) if re.search(r"rmtree\(\s*WAREHOUSE\b", l)]
    assert bool(resets) == (name == DEMOS[0])
