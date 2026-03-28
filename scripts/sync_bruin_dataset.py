"""Sync Bruin asset dataset prefixes to THALASSA_BQ_DATASET."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from runtime_config import get_bq_dataset

PIPELINE_ASSETS_DIR = PROJECT_ROOT / "pipeline" / "assets"
ASSET_NAME_PATTERN = re.compile(
    r'^\s*name:\s*"?([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)"?\s*$',
    re.MULTILINE,
)
DATASET_ID_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def iter_asset_files() -> list[Path]:
    """Return all Bruin asset files that may contain dataset prefixes."""
    return sorted(
        path
        for path in PIPELINE_ASSETS_DIR.rglob("*")
        if path.is_file() and path.suffix in {".sql", ".py"}
    )


def extract_asset_tables(asset_files: list[Path]) -> set[str]:
    """Collect all logical asset table names from the pipeline."""
    table_names: set[str] = set()
    for path in asset_files:
        match = ASSET_NAME_PATTERN.search(path.read_text(encoding="utf-8"))
        if match:
            table_names.add(match.group(2))
    return table_names


def build_reference_pattern(table_names: set[str]) -> re.Pattern[str]:
    """Build a regex that matches known dataset.table references."""
    table_pattern = "|".join(sorted(re.escape(name) for name in table_names))
    return re.compile(rf"\b[A-Za-z_][A-Za-z0-9_]*\.({table_pattern})\b")


def rewrite_asset_namespace(content: str, target_dataset: str, reference_pattern: re.Pattern[str]) -> str:
    """Rewrite all known asset references to the target dataset."""
    return reference_pattern.sub(lambda match: f"{target_dataset}.{match.group(1)}", content)


def validate_dataset_id(dataset_id: str) -> None:
    """Validate the target dataset name for Bruin asset usage."""
    if not DATASET_ID_PATTERN.fullmatch(dataset_id):
        raise ValueError("THALASSA_BQ_DATASET must match [A-Za-z_][A-Za-z0-9_]*.")


def sync_assets(check_only: bool = False) -> list[Path]:
    """Sync all pipeline asset references to THALASSA_BQ_DATASET."""
    target_dataset = get_bq_dataset()
    validate_dataset_id(target_dataset)

    asset_files = iter_asset_files()
    table_names = extract_asset_tables(asset_files)
    reference_pattern = build_reference_pattern(table_names)

    changed_files: list[Path] = []
    for path in asset_files:
        original = path.read_text(encoding="utf-8")
        rewritten = rewrite_asset_namespace(original, target_dataset, reference_pattern)
        if rewritten == original:
            continue
        changed_files.append(path)
        if not check_only:
            path.write_text(rewritten, encoding="utf-8")

    return changed_files


def main() -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if pipeline asset prefixes do not match THALASSA_BQ_DATASET.",
    )
    args = parser.parse_args()

    changed_files = sync_assets(check_only=args.check)
    if args.check and changed_files:
        for path in changed_files:
            print(path.relative_to(PROJECT_ROOT))
        return 1

    action = "Need sync" if args.check else "Synced"
    print(f"{action} {len(changed_files)} asset files to dataset '{get_bq_dataset()}'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
