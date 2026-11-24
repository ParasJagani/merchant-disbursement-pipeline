"""Gold, built by dbt on DuckDB over the same silver tables, checked against the oracle.

These tests run the real dbt project (models and tests) on a small simulated world. They are
skipped when dbt is not installed.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("dbt.cli.main")

from generator.chaos import deliver  # noqa: E402
from generator.config import GeneratorConfig  # noqa: E402
from generator.oracle import write_oracle  # noqa: E402
from generator.reference import replay_to_oracle  # noqa: E402
from generator.schema import validate_message  # noqa: E402
from generator.simulate import simulate  # noqa: E402
from silver.local import LocalSilver  # noqa: E402
from silver.spec import EVENT_TOPICS, REFERENCE_TABLES  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
CFG = GeneratorConfig(days=20, tx_per_day=200)
BRONZE = {t.topic: t.bronze for t in (*REFERENCE_TABLES, *EVENT_TOPICS)}
RUN = (
    "import sys; from dbt.cli.main import dbtRunner; "
    "sys.exit(0 if dbtRunner().invoke(sys.argv[1:]).success else 1)"
)


@pytest.fixture(scope="module")
def project(tmp_path_factory):
    """A copy of the dbt project, silver loaded into a DuckDB file, and the matching oracle."""
    work = tmp_path_factory.mktemp("dbt_run")
    shutil.copytree(ROOT / "dbt", work / "dbt", ignore=shutil.ignore_patterns("target", "logs"))
    messages, _ = deliver(CFG, simulate(CFG).events)
    messages = sorted(messages, key=lambda m: m.order)

    silver = LocalSilver(str(work / "local.duckdb"))
    silver.deploy()
    rows = {}
    for m in messages:
        if validate_message(m.topic, m.value)[0]:
            batch = rows.setdefault(BRONZE[m.topic], [])
            batch.append((m.value, "part.ndjson.gz", len(batch) + 1))
    for table, batch in rows.items():
        silver.load_bronze(table, batch)
    silver.run_reference()
    silver.close()

    oracle = replay_to_oracle(messages)
    write_oracle(work / "dbt" / "seeds" / "oracle_daily_disbursement.csv", oracle)
    return work, oracle


def _dbt(work: Path, *args: str) -> subprocess.CompletedProcess:
    env = {**os.environ, "DBT_DUCKDB_PATH": str(work / "local.duckdb")}
    return subprocess.run(
        [sys.executable, "-c", RUN, *args, "--project-dir", str(work / "dbt"),
         "--profiles-dir", str(work / "dbt"), "--target", "duckdb"],
        capture_output=True, text=True, env=env,
    )  # fmt: skip


def test_dbt_build_passes_every_model_and_test(project):
    work, oracle = project
    result = _dbt(work, "build")
    assert result.returncode == 0, result.stdout[-3000:]
    assert f"INSERT {len(oracle)}" in result.stdout  # the oracle seed loaded in full


def test_gold_fails_loudly_when_it_is_off_by_one_cent(project):
    work, _ = project
    seed = work / "dbt" / "seeds" / "oracle_daily_disbursement.csv"
    original = seed.read_text()
    lines = original.splitlines()
    head, first = lines[0], lines[1].split(",")
    first[-1] = str(int(first[-1]) + 1)  # one cent off in the answer key
    seed.write_text("\n".join([head, ",".join(first), *lines[2:]]) + "\n")
    try:
        result = _dbt(work, "build")
    finally:
        seed.write_text(original)
    assert result.returncode != 0
    assert "gold_matches_oracle" in result.stdout and "FAIL" in result.stdout


def test_project_files_hold_no_secrets_and_default_to_duckdb():
    profiles = (ROOT / "dbt" / "profiles.yml").read_text()
    assert "target: duckdb" in profiles
    assert "env_var('SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH')" in profiles
    assert "password" not in profiles.lower()


def test_gold_models_do_not_read_bronze_or_silver_tables_directly():
    for path in (ROOT / "dbt" / "models" / "gold").glob("*.sql"):
        text = path.read_text()
        assert "BRONZE" not in text.upper(), path.name
        assert "from silver" not in text.lower(), path.name  # only through source()
