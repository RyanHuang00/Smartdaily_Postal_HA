"""Release metadata contract tests."""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _load_json(relative_path):
    with (ROOT / relative_path).open(encoding="utf-8") as file_handle:
        return json.load(file_handle)


def test_hacs_metadata_matches_integration_manifest():
    hacs = _load_json("hacs.json")
    manifest = _load_json(
        "custom_components/smartdaily_postal_ha/manifest.json"
    )

    assert hacs["domain"] == manifest["domain"]
    assert hacs["documentation"] == manifest["documentation"]
    assert hacs["issue_tracker"] == manifest["issue_tracker"]
