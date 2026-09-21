import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_DELETED_FILE_RETENTION = "interval 7 days"
DEFAULT_LOG_RETENTION = "interval 30 days"
DEFAULT_CRON_HOUR = 3
DEFAULT_CRON_MINUTE = 0
CRON_MARKER = "# chzzk-delta-vacuum"


def project_root_from_path(path: str | Path | None = None) -> Path:
    if path is None:
        return PROJECT_ROOT
    return Path(path).resolve()


def default_table_paths(project_root: Path) -> list[Path]:
    return [
        project_root / "data" / "output" / "chat_bdy_stream",
        project_root / "data" / "output" / "dead_letter" / "chat_bdy_stream",
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
    table_paths: list[Path],
    *,
    deleted_file_retention: str = DEFAULT_DELETED_FILE_RETENTION,
    log_retention: str = DEFAULT_LOG_RETENTION,
    vacuum_retain_hours: int | None = None,
    dry_run: bool = False,
) -> None:
    for table_path in table_paths:
        spark.sql(
            build_retention_sql(
                table_path,
                deleted_file_retention=deleted_file_retention,
                log_retention=log_retention,
            )
        )
        spark.sql(
            build_vacuum_sql(
                table_path,
                vacuum_retain_hours=vacuum_retain_hours,
                dry_run=dry_run,
            )
        )


def run_maintenance(
    *,
    project_root: Path,
    table_paths: list[Path] | None = None,
    deleted_file_retention: str = DEFAULT_DELETED_FILE_RETENTION,
    log_retention: str = DEFAULT_LOG_RETENTION,
    vacuum_retain_hours: int | None = None,
    dry_run: bool = False,
    spark_factory=None,
) -> None:
    configure_spark_java()

    if spark_factory is None:
        from spark.kafka_raw_to_bronze import create_spark_session

        spark_factory = create_spark_session

    spark = spark_factory()
    try:
        apply_table_maintenance(
            spark,
            table_paths or default_table_paths(project_root),
            deleted_file_retention=deleted_file_retention,
            log_retention=log_retention,
            vacuum_retain_hours=vacuum_retain_hours,
            dry_run=dry_run,
        )
    finally:
        spark.stop()


def build_cron_entry(project_root: Path, *, hour: int, minute: int) -> str:
    root = project_root.resolve()
    python_bin = root / ".venv" / "bin" / "python"
    script_path = root / "spark" / "delta_maintenance.py"
    log_path = root / "data" / "logs" / "delta_maintenance.log"
    command = (
        f"cd {shlex.quote(str(root))} && "
        f"{shlex.quote(str(python_bin))} {shlex.quote(str(script_path))} run "
        f">> {shlex.quote(str(log_path))} 2>&1"
    )
    return f"{minute} {hour} * * * {command} {CRON_MARKER}"


def upsert_cron_entry(existing: str, entry: str, *, marker: str = CRON_MARKER) -> str:
    lines = [line for line in existing.splitlines() if marker not in line]
    lines.append(entry)
    return "\n".join(lines).strip() + "\n"


def read_existing_crontab() -> str:
    result = subprocess.run(
        ["crontab", "-l"],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        return result.stdout
    stderr = result.stderr.lower()
    if result.returncode == 1 and ("no crontab" in stderr or "crontab file not found" in stderr):
        return ""
    raise RuntimeError(f"Unable to read current crontab: {result.stderr.strip()}")


def install_cron_entry(project_root: Path, *, hour: int, minute: int) -> str:
    log_dir = project_root / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    entry = build_cron_entry(project_root, hour=hour, minute=minute)
    updated = upsert_cron_entry(read_existing_crontab(), entry)
    subprocess.run(
        ["crontab", "-"],
        input=updated,
        text=True,
        check=True,
    )
    return entry


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Run Delta retention and VACUUM maintenance.")
    parser.add_argument(
        "--project-root",
        default=str(project_root_from_path()),
        help="Absolute project root for default Delta table paths.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Apply retention properties and VACUUM.")
    run_parser.add_argument(
        "--table-path",
        action="append",
        dest="table_paths",
        help="Delta table path to maintain. Repeat to add multiple tables.",
    )
    run_parser.add_argument(
        "--deleted-file-retention",
        default=DEFAULT_DELETED_FILE_RETENTION,
        help="delta.deletedFileRetentionDuration value.",
    )
    run_parser.add_argument(
        "--log-retention",
        default=DEFAULT_LOG_RETENTION,
        help="delta.logRetentionDuration value.",
    )
    run_parser.add_argument(
        "--vacuum-retain-hours",
        type=int,
        help="Override VACUUM retention for this run.",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run VACUUM in DRY RUN mode.",
    )

    print_parser = subparsers.add_parser("print-cron", help="Print the managed cron entry.")
    print_parser.add_argument("--hour", type=int, default=DEFAULT_CRON_HOUR)
    print_parser.add_argument("--minute", type=int, default=DEFAULT_CRON_MINUTE)

    install_parser = subparsers.add_parser("install-cron", help="Install or replace the managed cron entry.")
    install_parser.add_argument("--hour", type=int, default=DEFAULT_CRON_HOUR)
    install_parser.add_argument("--minute", type=int, default=DEFAULT_CRON_MINUTE)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = project_root_from_path(args.project_root)

    if args.command == "run":
        table_paths = [Path(path).resolve() for path in args.table_paths] if args.table_paths else None
        run_maintenance(
            project_root=project_root,
            table_paths=table_paths,
            deleted_file_retention=args.deleted_file_retention,
            log_retention=args.log_retention,
            vacuum_retain_hours=args.vacuum_retain_hours,
            dry_run=args.dry_run,
        )
        return 0

    if args.command == "print-cron":
        print(build_cron_entry(project_root, hour=args.hour, minute=args.minute))
        return 0

    if args.command == "install-cron":
        print(install_cron_entry(project_root, hour=args.hour, minute=args.minute))
        return 0

    raise AssertionError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
