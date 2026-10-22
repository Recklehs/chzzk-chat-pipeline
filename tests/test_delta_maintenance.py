import importlib
import os
import plistlib
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


def test_build_launchd_plist_runs_workspace_python_daily_into_logfile():
    module = load_module()

    job = plistlib.loads(module.build_launchd_plist(PROJECT_ROOT, hour=3, minute=0))

    assert job["Label"] == module.LAUNCHD_LABEL
    assert job["ProgramArguments"] == [
        str(PROJECT_ROOT / ".venv" / "bin" / "python"), str(PROJECT_ROOT / "spark" / "delta_maintenance.py"), "run"
    ]
    assert job["WorkingDirectory"] == str(PROJECT_ROOT)
    assert job["StartCalendarInterval"] == {"Hour": 3, "Minute": 0}
    log_path = str(PROJECT_ROOT / "data" / "logs" / "delta_maintenance.log")
    assert job["StandardOutPath"] == job["StandardErrorPath"] == log_path
    assert "EnvironmentVariables" not in job

    pinned = plistlib.loads(module.build_launchd_plist(
        PROJECT_ROOT, hour=3, minute=0, java_home="/opt/jdk17", properties_file=Path("custom.properties")
    ))
    assert pinned["EnvironmentVariables"] == {"SPARK_JAVA_HOME": "/opt/jdk17"}
    assert pinned["ProgramArguments"][2:] == ["--properties-file", str(Path("custom.properties").resolve()), "run"]


def test_apply_table_maintenance_sets_changed_retention_and_vacuums_each_table():
    module = load_module()

    class FakeSpark:
        def __init__(self, properties):
            self.properties = properties
            self.queries = []

        def sql(self, query):
            self.queries.append(query)
            return self

        def first(self):
            return {"properties": self.properties[self.queries[-1].split("`")[1]]}

    silver = Path("/tmp/silver-table").resolve()
    gold = Path("/tmp/gold-table").resolve()
    spark = FakeSpark({
        str(silver): {
            "delta.deletedFileRetentionDuration": "interval 7 days",
            "delta.logRetentionDuration": "interval 30 days",
        },
        str(gold): {},
    })

    module.apply_table_maintenance(
        spark,
        [
            (silver, "interval 7 days", "interval 30 days"),
            (gold, "interval 1 days", "interval 1 days"),
        ],
    )

    assert spark.queries == [
        f"DESCRIBE DETAIL delta.`{silver}`",
        f"VACUUM delta.`{silver}`",
        f"DESCRIBE DETAIL delta.`{gold}`",
        f"ALTER TABLE delta.`{gold}` SET TBLPROPERTIES ("
        "'delta.deletedFileRetentionDuration' = 'interval 1 days', "
        "'delta.logRetentionDuration' = 'interval 1 days')",
        f"VACUUM delta.`{gold}`",
    ]


def test_default_tables_follow_frame_layout_with_short_gold_retention(tmp_path):
    module = load_module()
    frames = tmp_path / "frames"
    for table in ("bronze", "silver/chat_messages", "silver/donations", "gold/broadcast_sessions"):
        (frames / table / "_delta_log").mkdir(parents=True)
    (frames / "silver_v3_1" / "chat_messages" / "_delta_log").mkdir(parents=True)
    properties = tmp_path / "local.properties"
    properties.write_text(
        "app.kafka.bootstrap.servers=localhost:9092\n"
        "app.kafka.topic=chzzk.events.raw\n"
        "app.kafka.startingOffsets=earliest\n"
        f"app.bucket.uri={frames}\n",
        encoding="utf-8",
    )

    tables = module.default_tables(properties)

    keep = ("interval 7 days", "interval 30 days")
    assert tables == [
        (frames / "bronze", *keep),
        (frames / "silver" / "chat_messages", *keep),
        (frames / "silver" / "donations", *keep),
        (frames / "gold" / "broadcast_sessions", "interval 1 days", "interval 1 days"),
    ]
