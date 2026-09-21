import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread

CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"


class CollectorMetrics:
    """Small Prometheus text exporter for collector and benchmark metrics."""

    def __init__(self):
        self.counters: dict[str, dict[tuple[tuple[str, str], ...], float]] = defaultdict(lambda: defaultdict(float))
        self.histograms: dict[str, dict[tuple[tuple[str, str], ...], list[float]]] = defaultdict(lambda: defaultdict(list))
        self._lock = Lock()

    @staticmethod
    def labels(**labels: object) -> tuple[tuple[str, str], ...]:
        return tuple(sorted((key, str(value)) for key, value in labels.items() if value is not None))

    def increment(self, name: str, amount: float = 1.0, **labels: object) -> None:
        with self._lock:
            self.counters[name][self.labels(**labels)] += amount

    def observe(self, name: str, value: float, **labels: object) -> None:
        with self._lock:
            self.histograms[name][self.labels(**labels)].append(value)

    def time(self, name: str, **labels: object):
        return _MetricTimer(self, name, labels)

    def reset(self) -> None:
        with self._lock:
            self.counters.clear()
            self.histograms.clear()

    def snapshot(self) -> dict[str, dict[str, list[dict[str, object]]]]:
        with self._lock:
            counters = {
                name: dict(values)
                for name, values in self.counters.items()
            }
            histograms = {
                name: {labels: list(values) for labels, values in grouped.items()}
                for name, grouped in self.histograms.items()
            }

        return {
            "counters": {
                name: [
                    {"labels": dict(labels), "value": value}
                    for labels, value in sorted(values.items())
                ]
                for name, values in sorted(counters.items())
            },
            "histograms": {
                name: [
                    {
                        "labels": dict(labels),
                        "count": len(values),
                        "sum": sum(values),
                        "p50": _quantile(values, 0.50),
                        "p95": _quantile(values, 0.95),
                    }
                    for labels, values in sorted(grouped.items())
                ]
                for name, grouped in sorted(histograms.items())
            },
        }

    def counter_total(self, name: str) -> float:
        with self._lock:
            return sum(self.counters.get(name, {}).values())

    def histogram_p95(self, name: str) -> float:
        with self._lock:
            values = [
                value
                for grouped_values in self.histograms.get(name, {}).values()
                for value in grouped_values
            ]
        return _quantile(values, 0.95)

    def render(self) -> bytes:
        with self._lock:
            counters = {
                name: dict(values)
                for name, values in self.counters.items()
            }
            histograms = {
                name: {labels: list(values) for labels, values in grouped.items()}
                for name, grouped in self.histograms.items()
            }

        lines = []
        for name in sorted(counters):
            for labels, value in sorted(counters[name].items()):
                lines.append(f"{name}{_format_labels(labels)} {_format_number(value)}")

        for name in sorted(histograms):
            for labels, values in sorted(histograms[name].items()):
                count = len(values)
                total = sum(values)
                lines.append(f"{name}_count{_format_labels(labels)} {count}")
                lines.append(f"{name}_sum{_format_labels(labels)} {_format_number(total)}")
        return ("\n".join(lines) + "\n").encode("utf-8")


class _MetricTimer:
    def __init__(self, metrics: CollectorMetrics, name: str, labels: dict[str, object]):
        self.metrics = metrics
        self.name = name
        self.labels = labels
        self.started_at = 0.0

    def __enter__(self):
        self.started_at = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.metrics.observe(self.name, time.perf_counter() - self.started_at, **self.labels)
        return False


def _format_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    encoded = ",".join(f'{key}="{_escape_label(value)}"' for key, value in labels)
    return "{" + encoded + "}"


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _format_number(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.9f}".rstrip("0").rstrip(".")


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * q
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    if low == high:
        return float(ordered[low])
    return float(ordered[low] + (ordered[high] - ordered[low]) * (position - low))


metrics = CollectorMetrics()


class MetricsRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?", 1)[0] != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        payload = metrics.render()
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE_LATEST)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):  # noqa: A002
        return


def start_metrics_http_server(host: str = "0.0.0.0", port: int = 8000) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), MetricsRequestHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
