import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_expected_folders_exist():
    for name in ["generator", "file_writer", "terraform", "dbt", "dashboard", "statements"]:
        assert (ROOT / name).is_dir(), name


def test_core_docs_exist():
    for name in ["README.md", "DECISIONS.md", "docs/roadmap.md"]:
        assert (ROOT / name).is_file(), name


def test_decisions_lists_all_seven():
    text = (ROOT / "DECISIONS.md").read_text()
    for number in range(1, 8):
        assert f"| {number} |" in text, number


def test_branding_check_passes():
    result = subprocess.run(
        [str(ROOT / "scripts" / "check_branding.sh")],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_branding_check_catches_a_denied_term(tmp_path):
    work = tmp_path / "repo"
    (work / "scripts").mkdir(parents=True)
    script = (ROOT / "scripts" / "check_branding.sh").read_text()
    (work / "scripts" / "check_branding.sh").write_text(script)
    (work / "scripts" / "check_branding.sh").chmod(0o755)
    (work / ".branding-denylist").write_text("forbidden-word\n")
    (work / "notes.txt").write_text("this mentions Forbidden-Word here\n")
    run = lambda *a: subprocess.run(a, cwd=work, capture_output=True, text=True)  # noqa: E731
    run("git", "init", "-q")
    run("git", "add", "notes.txt")
    result = run("./scripts/check_branding.sh")
    assert result.returncode == 1
