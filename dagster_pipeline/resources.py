"""
Shared Dagster resources.

DbtCliResource is the only resource needed.  It is registered under the key
"dbt" so that @dbt_assets can receive it as a function argument.

profiles.yml resolution order:
  1. repo root (adventure_works_dbt/../profiles.yml)  — convenient for local dev
  2. ~/.dbt/profiles.yml                              — standard dbt default
Copy profiles.yml.example → profiles.yml and fill in your credentials.
"""

from pathlib import Path

from dagster_dbt import DbtCliResource

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DBT_PROJECT_DIR = _REPO_ROOT / "adventure_works_dbt"

# Auto-detect profiles.yml location: repo root first, then ~/.dbt
_profiles_at_root = _REPO_ROOT / "profiles.yml"
_DBT_PROFILES_DIR = str(_REPO_ROOT) if _profiles_at_root.exists() else str(Path.home() / ".dbt")

dbt_resource = DbtCliResource(
    project_dir=str(_DBT_PROJECT_DIR),
    profiles_dir=_DBT_PROFILES_DIR,
)
