import importlib
import os
import stat
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_module():
    sys.modules.pop("spark.delta_maintenance", None)
    return importlib.import_module("spark.delta_maintenance")


def make_fake_java_home(tmp_path: Path, name: str, *, supports_vector: bool) -> Path:
    java_home = tmp_path / name
    bin_dir = java_home / "bin"
    bin_dir.mkdir(parents=True)
    java_bin = bin_dir / "java"
    modules_output = "jdk.incubator.vector@17.0.18\n" if supports_vector else ""
    java_bin.write_text(
        "#!/usr/bin/env bash\n"
        "if [[ \"$1\" == \"--list-modules\" ]]; then\n"
        f"  printf '%s' {modules_output!r}\n"
        "  exit 0\n"
        "fi\n"
        "echo 'fake java'\n",
        encoding="utf-8",
    )
    java_bin.chmod(java_bin.stat().st_mode | stat.S_IXUSR)
    return java_home


def make_java_home_discovery_script(tmp_path: Path, resolved_java_home: Path | None) -> Path:
    script = tmp_path / "java_home_discovery.sh"
    output = "" if resolved_java_home is None else str(resolved_java_home)
    script.write_text(
        "#!/usr/bin/env bash\n"
        "if [[ \"$1\" == \"-v\" && \"$2\" == \"17\" ]]; then\n"
        f"  printf '%s\\n' {output!r}\n"
        "  exit 0\n"
        "fi\n"
        "exit 1\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return script


def test_configure_spark_java_prefers_discovered_jdk17_when_current_java_home_is_incompatible(
    monkeypatch, tmp_path
):
    module = load_module()
    incompatible_java_home = make_fake_java_home(tmp_path, "android-studio-jbr", supports_vector=False)
    compatible_java_home = make_fake_java_home(tmp_path, "openjdk-17", supports_vector=True)
    discovery_script = make_java_home_discovery_script(tmp_path, compatible_java_home)

    monkeypatch.setenv("JAVA_HOME", str(incompatible_java_home))
    monkeypatch.delenv("SPARK_JAVA_HOME", raising=False)
    monkeypatch.setenv("JAVA_HOME_DISCOVERY_BIN", str(discovery_script))

    module.configure_spark_java()

    assert os.environ["JAVA_HOME"] == str(compatible_java_home)
    assert os.environ["PATH"].split(os.pathsep)[0] == str(compatible_java_home / "bin")


def test_build_cron_entry_uses_workspace_python_and_logfile():
    module = load_module()

    entry = module.build_cron_entry(PROJECT_ROOT, hour=3, minute=0)

    assert entry.startswith("0 3 * * * ")
    assert str(PROJECT_ROOT / ".venv" / "bin" / "python") in entry
    assert str(PROJECT_ROOT / "spark" / "delta_maintenance.py") in entry
    assert str(PROJECT_ROOT / "data" / "logs" / "delta_maintenance.log") in entry
    assert module.CRON_MARKER in entry


def test_upsert_cron_entry_adds_or_replaces_single_managed_entry():
    module = load_module()
    existing = "MAILTO=\"\"\n15 1 * * * echo already-here\n"
    first = "0 3 * * * /tmp/first # marker"
    second = "30 4 * * * /tmp/second # marker"

    updated = module.upsert_cron_entry(existing, first, marker="# marker")
    replaced = module.upsert_cron_entry(updated, second, marker="# marker")

    assert "15 1 * * * echo already-here" in replaced
    assert first not in replaced
    assert replaced.count("# marker") == 1
    assert second in replaced


def test_apply_table_maintenance_sets_retention_and_vacuums_each_table():
    module = load_module()

    class FakeSpark:
        def __init__(self):
            self.queries = []

        def sql(self, query):
            self.queries.append(query)
            return self

    spark = FakeSpark()
    main_table = Path("/tmp/main-table").resolve()
    dead_letter = Path("/tmp/dead-letter").resolve()

    module.apply_table_maintenance(
        spark,
        [
            main_table,
            dead_letter,
        ],
        deleted_file_retention="interval 7 days",
        log_retention="interval 30 days",
    )

    assert spark.queries == [
        f"ALTER TABLE delta.`{main_table}` SET TBLPROPERTIES ("
        "'delta.deletedFileRetentionDuration' = 'interval 7 days', "
        "'delta.logRetentionDuration' = 'interval 30 days')",
        f"VACUUM delta.`{main_table}`",
        f"ALTER TABLE delta.`{dead_letter}` SET TBLPROPERTIES ("
        "'delta.deletedFileRetentionDuration' = 'interval 7 days', "
        "'delta.logRetentionDuration' = 'interval 30 days')",
        f"VACUUM delta.`{dead_letter}`",
    ]
