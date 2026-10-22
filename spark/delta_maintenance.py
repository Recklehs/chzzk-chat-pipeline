import argparse
import os
import plistlib
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_DELETED_FILE_RETENTION = "interval 7 days"
DEFAULT_LOG_RETENTION = "interval 30 days"
# Gold rewrites its rows every trigger and can be rebuilt with `silver_to_gold.py --full`, so a week of replaced
# files buys nothing. Bronze and Silver keep the defaults: Silver and Gold stream checkpoints point at their versions.
GOLD_DELETED_FILE_RETENTION = "interval 1 days"
GOLD_LOG_RETENTION = "interval 1 days"
DEFAULT_PROPERTIES_FILE = Path("spark") / "conf" / "local.properties"
DEFAULT_SCHEDULE_HOUR = 3
DEFAULT_SCHEDULE_MINUTE = 0
LAUNCHD_LABEL = "com.chzzk.delta-vacuum"
LAUNCHD_PLIST = Path.home() / "Library" / "LaunchAgents" / f"{LAUNCHD_LABEL}.plist"
BOOTSTRAP_ATTEMPTS = 10


def project_root_from_path(path: str | Path | None = None) -> Path:
    if path is None:
        return PROJECT_ROOT
    return Path(path).resolve()


def delta_tables(*roots: str | Path) -> list[Path]:
    """Delta tables among `roots` themselves and their direct children."""
    tables = []
    for root in map(Path, roots):
        candidates = [root, *sorted(child for child in root.iterdir() if child.is_dir())] if root.is_dir() else []
        tables += [path for path in candidates if (path / "_delta_log").is_dir()]
    return tables


def default_tables(properties_file: str | Path) -> list[tuple[Path, str, str]]:
    """Frame Bronze, Silver and Gold tables from the job properties, with each layer's retention."""
    from spark.silver_to_gold import load_settings

    settings = load_settings(["--properties-file", str(properties_file)])
    keep = (DEFAULT_DELETED_FILE_RETENTION, DEFAULT_LOG_RETENTION)
    return [
        *((path, *keep) for path in delta_tables(settings.silver.bronze_path, settings.silver.output_path)),
        *((path, GOLD_DELETED_FILE_RETENTION, GOLD_LOG_RETENTION) for path in delta_tables(settings.gold_path)),
    ]


def java_supports_vector_module(java_bin: str | Path) -> bool:
    java_bin_path = Path(java_bin)
    if not java_bin_path.exists():
        return False
    result = subprocess.run(
        [str(java_bin_path), "--list-modules"],
        text=True,
        capture_output=True,
        check=False,
    )
    return result.returncode == 0 and "jdk.incubator.vector@" in result.stdout


def discover_spark_java_home(discovery_bin: str | None = None) -> str | None:
    discovery = discovery_bin or os.environ.get("JAVA_HOME_DISCOVERY_BIN", "/usr/libexec/java_home")
    discovery_path = Path(discovery)
    if not discovery_path.exists():
        return None

    result = subprocess.run(
        [str(discovery_path), "-v", "17"],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return None

    value = result.stdout.strip()
    return value or None


def choose_java_home() -> str:
    preferred_java_home = os.environ.get("SPARK_JAVA_HOME")
    if preferred_java_home:
        if java_supports_vector_module(Path(preferred_java_home) / "bin" / "java"):
            return preferred_java_home
        raise RuntimeError(f"SPARK_JAVA_HOME={preferred_java_home} is not Spark-compatible.")

    current_java_home = os.environ.get("JAVA_HOME")
    if current_java_home and java_supports_vector_module(Path(current_java_home) / "bin" / "java"):
        return current_java_home

    discovered_java_home = discover_spark_java_home()
    if discovered_java_home and java_supports_vector_module(Path(discovered_java_home) / "bin" / "java"):
        return discovered_java_home

    raise RuntimeError(
        "Unable to find a Spark-compatible JDK 17. "
        "Set JAVA_HOME or SPARK_JAVA_HOME to a JDK with jdk.incubator.vector."
    )


def configure_spark_java() -> str:
    selected_java_home = choose_java_home()
    os.environ["JAVA_HOME"] = selected_java_home
    java_bin_dir = str(Path(selected_java_home) / "bin")
    path_parts = os.environ.get("PATH", "").split(os.pathsep) if os.environ.get("PATH") else []
    if not path_parts or path_parts[0] != java_bin_dir:
        remaining = [part for part in path_parts if part != java_bin_dir]
        os.environ["PATH"] = os.pathsep.join([java_bin_dir, *remaining]) if remaining else java_bin_dir
    return selected_java_home


def build_retention_sql(table_path: str | Path, *, deleted_file_retention: str, log_retention: str) -> str:
    resolved_path = Path(table_path).resolve()
    return (
        f"ALTER TABLE delta.`{resolved_path}` SET TBLPROPERTIES ("
        f"'delta.deletedFileRetentionDuration' = '{deleted_file_retention}', "
        f"'delta.logRetentionDuration' = '{log_retention}')"
    )


def build_vacuum_sql(
    table_path: str | Path,
    *,
    vacuum_retain_hours: int | None = None,
    dry_run: bool = False,
) -> str:
    resolved_path = Path(table_path).resolve()
    parts = [f"VACUUM delta.`{resolved_path}`"]
    if vacuum_retain_hours is not None:
        parts.append(f"RETAIN {vacuum_retain_hours} HOURS")
    if dry_run:
        parts.append("DRY RUN")
    return " ".join(parts)


def apply_table_maintenance(
    spark,
    tables: list[tuple[Path, str, str]],
    *,
    vacuum_retain_hours: int | None = None,
    dry_run: bool = False,
) -> None:
    for table_path, deleted_file_retention, log_retention in tables:
        properties = spark.sql(f"DESCRIBE DETAIL delta.`{Path(table_path).resolve()}`").first()["properties"]
        # A property change is a metadata commit that fails the running streams' concurrent writes, so only
        # set the properties when they differ.
        if (
            properties.get("delta.deletedFileRetentionDuration") != deleted_file_retention
            or properties.get("delta.logRetentionDuration") != log_retention
        ):
            spark.sql(
                build_retention_sql(
                    table_path,
                    deleted_file_retention=deleted_file_retention,
                    log_retention=log_retention,
                )
            )
        result = spark.sql(
            build_vacuum_sql(
                table_path,
                vacuum_retain_hours=vacuum_retain_hours,
                dry_run=dry_run,
            )
        )
        print(f"{table_path}: {result.count()} files to delete" if dry_run else f"{table_path}: vacuumed", flush=True)


def run_maintenance(
    *,
    properties_file: Path,
    table_paths: list[Path] | None = None,
    deleted_file_retention: str = DEFAULT_DELETED_FILE_RETENTION,
    log_retention: str = DEFAULT_LOG_RETENTION,
    vacuum_retain_hours: int | None = None,
    dry_run: bool = False,
    spark_factory=None,
) -> None:
    configure_spark_java()

    if spark_factory is None:
        from spark.kafka_raw_to_bronze import create_spark_session, load_runtime_settings

        def spark_factory():
            settings = load_runtime_settings(["--properties-file", str(properties_file)])
            return create_spark_session(settings, kafka=False)

    if table_paths:
        tables = [(path, deleted_file_retention, log_retention) for path in table_paths]
    else:
        tables = default_tables(properties_file)
    if not tables:
        raise RuntimeError(f"No Delta tables found for {properties_file}.")

    spark = spark_factory()
    try:
        apply_table_maintenance(
            spark,
            tables,
            vacuum_retain_hours=vacuum_retain_hours,
            dry_run=dry_run,
        )
    finally:
        spark.stop()


def build_launchd_plist(
    project_root: Path,
    *,
    hour: int,
    minute: int,
    java_home: str | None = None,
    properties_file: Path | None = None,
) -> bytes:
    root = project_root.resolve()
    log_path = str(root / "data" / "logs" / "delta_maintenance.log")
    properties = ["--properties-file", str(properties_file.resolve())] if properties_file else []
    job = {
        "Label": LAUNCHD_LABEL,
        "ProgramArguments": [
            str(root / ".venv" / "bin" / "python"), str(root / "spark" / "delta_maintenance.py"), *properties, "run"
        ],
        "WorkingDirectory": str(root),
        # Unlike cron, launchd runs a calendar job missed during sleep once the Mac wakes.
        "StartCalendarInterval": {"Hour": hour, "Minute": minute},
        "StandardOutPath": log_path,
        "StandardErrorPath": log_path,
    }
    # launchd starts without the shell's JAVA_HOME, so pin the JDK found when the agent is installed.
    if java_home:
        job["EnvironmentVariables"] = {"SPARK_JAVA_HOME": java_home}
    return plistlib.dumps(job)


def install_launchd_agent(project_root: Path, *, hour: int, minute: int, properties_file: Path | None = None) -> Path:
    (project_root / "data" / "logs").mkdir(parents=True, exist_ok=True)
    LAUNCHD_PLIST.parent.mkdir(parents=True, exist_ok=True)
    LAUNCHD_PLIST.write_bytes(
        build_launchd_plist(
            project_root, hour=hour, minute=minute, java_home=choose_java_home(), properties_file=properties_file
        )
    )
    domain = f"gui/{os.getuid()}"
    # bootout fails when the agent is not loaded yet; bootstrap then loads the new plist.
    subprocess.run(["launchctl", "bootout", f"{domain}/{LAUNCHD_LABEL}"], capture_output=True, check=False)
    # bootout can return before the old agent is gone, and bootstrap then fails with "5: Input/output error".
    for attempt in range(BOOTSTRAP_ATTEMPTS):
        result = subprocess.run(
            ["launchctl", "bootstrap", domain, str(LAUNCHD_PLIST)], text=True, capture_output=True, check=False
        )
        if result.returncode == 0:
            return LAUNCHD_PLIST
        if attempt < BOOTSTRAP_ATTEMPTS - 1:
            time.sleep(0.5)
    raise RuntimeError(
        f"launchctl bootstrap failed after {BOOTSTRAP_ATTEMPTS} attempts, so the agent is not loaded: "
        f"{result.stderr.strip()}"
    )


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Run Delta retention and VACUUM maintenance.")
    parser.add_argument(
        "--project-root",
        default=str(project_root_from_path()),
        help="Absolute project root for the launchd agent and the default properties file.",
    )
    parser.add_argument(
        "--properties-file",
        help=f"Job properties that locate the frame tables. Defaults to <project-root>/{DEFAULT_PROPERTIES_FILE}.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Apply retention properties and VACUUM.")
    run_parser.add_argument(
        "--table-path",
        action="append",
        dest="table_paths",
        help="Delta table path to maintain instead of the frame tables. Repeat to add multiple tables.",
    )
    run_parser.add_argument(
        "--deleted-file-retention",
        default=DEFAULT_DELETED_FILE_RETENTION,
        help="delta.deletedFileRetentionDuration for --table-path tables.",
    )
    run_parser.add_argument(
        "--log-retention",
        default=DEFAULT_LOG_RETENTION,
        help="delta.logRetentionDuration for --table-path tables.",
    )
    run_parser.add_argument(
        "--vacuum-retain-hours",
        type=int,
        help="Override VACUUM retention for this run.",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Still apply retention properties, then run VACUUM in DRY RUN mode and print each table's file count.",
    )

    print_parser = subparsers.add_parser("print-launchd", help="Print the managed launchd agent plist.")
    print_parser.add_argument("--hour", type=int, default=DEFAULT_SCHEDULE_HOUR)
    print_parser.add_argument("--minute", type=int, default=DEFAULT_SCHEDULE_MINUTE)

    install_parser = subparsers.add_parser("install-launchd", help="Install or replace the managed launchd agent.")
    install_parser.add_argument("--hour", type=int, default=DEFAULT_SCHEDULE_HOUR)
    install_parser.add_argument("--minute", type=int, default=DEFAULT_SCHEDULE_MINUTE)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = project_root_from_path(args.project_root)

    if args.command == "run":
        table_paths = [Path(path).resolve() for path in args.table_paths] if args.table_paths else None
        run_maintenance(
            properties_file=Path(args.properties_file or project_root / DEFAULT_PROPERTIES_FILE).resolve(),
            table_paths=table_paths,
            deleted_file_retention=args.deleted_file_retention,
            log_retention=args.log_retention,
            vacuum_retain_hours=args.vacuum_retain_hours,
            dry_run=args.dry_run,
        )
        return 0

    properties_file = Path(args.properties_file) if args.properties_file else None
    if args.command == "print-launchd":
        plist = build_launchd_plist(
            project_root,
            hour=args.hour,
            minute=args.minute,
            java_home=choose_java_home(),
            properties_file=properties_file,
        )
        print(plist.decode(), end="")
        return 0

    if args.command == "install-launchd":
        print(install_launchd_agent(project_root, hour=args.hour, minute=args.minute, properties_file=properties_file))
        return 0

    raise AssertionError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
