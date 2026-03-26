"""@bruin
name: thalassa.auto_panel_snapshot_writer
type: python
image: python:3.11
connection: gcp-default

depends:
  - thalassa.intelligence_snapshots
@bruin"""

from __future__ import annotations

import runpy
from pathlib import Path


def main() -> None:
    project_root = Path(__file__).resolve().parents[3]
    script_path = project_root / "scripts" / "generate_auto_panel_snapshot.py"
    runpy.run_path(str(script_path), run_name="__main__")


if __name__ == "__main__":
    main()
