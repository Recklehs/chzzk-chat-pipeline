"""Local Bronze Delta -> six Silver Delta tables, with one checkpoint and writer."""
import argparse
from contextlib import contextmanager
from dataclasses import dataclass, replace
import json
from pathlib import Path
import signal
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from spark.config import parse_properties_file
from spark.kafka_raw_to_bronze import RuntimeSettings, create_spark_session, load_runtime_settings
from spark.silver_parser import PARSER_VERSION
from spark.silver_store import write_batch


@dataclass(frozen=True)
class SilverSettings:
    bronze_path: str
    output_path: str
    checkpoint_path: str
    runtime: RuntimeSettings
    available_now: bool = False


def load_settings(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--properties-file', required=True)
    parser.add_argument('--available-now', action='store_true', help='Process the current Bronze snapshot and exit')
    args = parser.parse_args(argv)
    runtime = load_runtime_settings(['--properties-file', args.properties_file])
    props = parse_properties_file(args.properties_file)
    if '://' in runtime.output_path:
        raise ValueError('Silver writer currently supports local filesystem paths')
    bronze = Path(runtime.output_path).expanduser().resolve()
    output = props.get('app.silver.path', str(bronze.parent / 'silver'))
    checkpoint = props.get('app.silver.checkpoint.path', str(bronze.parent / 'silver_checkpoint'))
    if any('://' in path for path in (output, checkpoint)):
        raise ValueError('Silver writer currently supports local filesystem paths')
    output, checkpoint = (Path(path).expanduser().resolve() for path in (output, checkpoint))
    paths = [bronze, output, checkpoint, Path(runtime.checkpoint_path).expanduser().resolve()]
    for i, path in enumerate(paths):
        if any(path == other or path in other.parents or other in path.parents for other in paths[i+1:]):
            raise ValueError('Bronze/Silver/checkpoint paths must not overlap')
    properties = {**runtime.spark_properties, 'spark.sql.session.timeZone': 'UTC'}
    properties.setdefault('spark.sql.shuffle.partitions', '4')
    properties.setdefault('spark.databricks.delta.snapshotPartitions', '4')
    runtime = replace(runtime, spark_app_name='chzzk-bronze-to-silver', spark_properties=properties)
    return SilverSettings(str(bronze), str(output), str(checkpoint), runtime, args.available_now)


@contextmanager
def writer_lock(settings):
    # Local POSIX writer; a distributed deployment needs a distributed writer lease.
    import fcntl
    output = Path(settings.output_path)
    output.mkdir(parents=True, exist_ok=True)
    with (output / '.writer.lock').open('a+') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError('Another Silver writer is already using this output') from exc
        contract = {'parser_version': PARSER_VERSION, 'bronze_path': settings.bronze_path,
                    'checkpoint_path': settings.checkpoint_path}
        path = output / '_silver_contract.json'
        if path.exists():
            if json.loads(path.read_text()) != contract:
                raise ValueError('Silver contract changed; use a new output and checkpoint')
        else:
            checkpoint = Path(settings.checkpoint_path)
            if any(output.glob('*/_delta_log')) or (checkpoint.exists() and any(checkpoint.iterdir())):
                raise ValueError('Missing Silver contract for existing output/checkpoint; use fresh paths')
            pending = output / '_silver_contract.json.tmp'
            pending.write_text(json.dumps(contract, indent=2) + '\n')
            pending.replace(path)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def create_query(spark, settings):
    frames = spark.readStream.format('delta').load(settings.bronze_path)
    writer = (frames.writeStream.option('checkpointLocation', settings.checkpoint_path)
              .foreachBatch(lambda batch, batch_id: write_batch(batch, batch_id, settings.output_path, settings.bronze_path)))
    writer = writer.trigger(availableNow=True) if settings.available_now else writer.trigger(processingTime=settings.runtime.processing_time)
    return writer.start()


def main(argv=None):
    settings = load_settings(argv)
    stopped = False
    def request_stop(_signum, _frame):
        nonlocal stopped
        stopped = True
    with writer_lock(settings):
        spark = create_spark_session(settings.runtime)
        query = None
        previous = {sig: signal.signal(sig, request_stop) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            query = create_query(spark, settings)
            print('SPARK_SILVER_READY', flush=True)
            while not stopped and not query.awaitTermination(1):
                pass
        finally:
            try:
                if query is not None:
                    query.stop()
            finally:
                spark.stop()
                for sig, handler in previous.items():
                    signal.signal(sig, handler)


if __name__ == '__main__':
    main()
