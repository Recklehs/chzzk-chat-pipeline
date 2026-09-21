# WebSocket Kafka Optimization Results

## Summary

- `data/output/chat_bdy_stream`는 Delta log 기준 active parquet이 3개라 parquet compaction은 제외했다.
- Full matrix는 1개 phase당 순수 실행 시간만 약 183분이라, Docker 복구 후에는 smoke와 대표 시나리오 short run으로 먼저 gate를 걸렀다.
- Benchmark harness가 `collector.benchmark_replay`에서 Kafka producer 설정을 누락하던 문제를 발견해 수정했다.
- Phase 1의 `KAFKA_PRODUCER_LINGER_MS=20` 적용값은 `send_and_wait` 경로에서 publish latency를 크게 악화시켜 기본/benchmark 적용값을 `0`으로 되돌렸다. 설정 knob은 유지한다.
- Phase 2, 3, 4, 5는 short gate를 통과해 local `main`에 merge했다.
- Phase 6은 현재 active replay input에 control frame cmd `100/10000/10001`이 없어 효과를 관측하지 못했고, raw publish 계약 변경 가능성 때문에 merge하지 않았다.

## Benchmark Inputs

- Source snapshot: `/Users/kanghyoseung/PycharmProjects/chzzk_container/data/output/chat_bdy_stream`
- Active Delta parquet files: 3
- Generated replay input root: `data/output/benchmarks/input`
- Main short baseline roots:
  - `data/output/benchmarks/results/main-screen-5m`
  - `data/output/benchmarks/results/main-linger0-60s`
- Phase result roots:
  - `data/output/benchmarks/results/phase-2-linger0-60s`
  - `data/output/benchmarks/results/phase-3-screen`
  - `data/output/benchmarks/results/phase-4-screen`
  - `data/output/benchmarks/results/phase-5-smoke`
  - `data/output/benchmarks/results/phase-6-filter-60s`

## Merge Decisions

| Phase | Final branch | Decision | Reason |
|---|---|---|---|
| Phase 1 batching | `codex/ws-kafka-disable-linger-defaults` | partial rollback | `linger_ms=20` hurt `send_and_wait`; default/benchmark value reverted to `0`, config knob retained |
| Phase 2 compression | `codex/ws-kafka-phase2-reverify-linger0` | merged | LZ4 reduced broker bytes/event by about 64% with no failures/drops |
| Phase 3 raw payload reuse | `codex/ws-kafka-phase3-verify` | merged | JSON parse/CPU/publish p95 improved on comparable 5m burst screen |
| Phase 4 counter buckets | `codex/ws-kafka-phase4-verify-on-phase3` | merged | fanout-idle RSS improved by about 12% with no failures/drops |
| Phase 5 adaptive polling | `codex/ws-kafka-phase5-verify` | merged | NOT_LIVE poll requests dropped 60% while max re-detect interval stayed 60s |
| Phase 6 control filtering | `codex/ws-kafka-phase6-experiment-latest` | not merged | active replay had no control frames; also changes raw publish contract |

## Measured Results

### Phase 1 Linger Check

After fixing the harness so producer settings were actually applied, `KAFKA_PRODUCER_LINGER_MS=20` produced about 27ms publish p95 on a 120s `burst-batch` run. Reverting the applied default to `0` recovered publish p95 to about 4.6ms on a 60s `burst-batch` smoke. Because durations differ, this was used as a rejection signal for the applied linger value, not as a formal KPI comparison.

### Phase 2 Compression

Comparable condition: `burst-batch`, 60s duration, 10s warm-up, 1 run, `linger_ms=0`.

| Metric | Main baseline | Phase 2 LZ4 | Delta |
|---|---:|---:|---:|
| Acked events | 388,180 | 443,949 | +14.37% |
| Publish failures | 0 | 0 | 0 |
| Events dropped | 0 | 0 | 0 |
| Publish p95 | 0.004564s | 0.003607s | -20.97% |
| CPU sec / 10k events | 1.318254 | 1.165086 | -11.62% |
| Broker bytes / event | 1527.12 | 550.43 | -63.96% |

Note: `aiokafka` 0.13 checks `cramjam` for LZ4 support, so Phase 2 was fixed to install `cramjam` rather than only `lz4`.

### Phase 3 Raw Payload Reuse

Comparable condition: `burst-batch`, 5m duration, 30s warm-up, 2 runs.

| Metric | Main screen | Phase 3 | Delta |
|---|---:|---:|---:|
| JSON parse sec / 10k frames | 0.148743 | 0.135773 | -8.72% |
| CPU sec / 10k events | 0.993094 | 0.764474 | -23.02% |
| Publish p95 | 0.003070s | 0.001975s | -35.67% |
| Kafka payload bytes / event | 1345.420 | 1345.599 | +0.01% |
| Acked events / sec | 8127.265 | 10776.018 | +32.59% |

Payload bytes stayed effectively identical, so the raw publish contract was preserved.

### Phase 4 Counter Buckets

Comparable condition: `fanout-idle`, 5m duration, 30s warm-up, 2 runs.

| Metric | Main screen | Phase 4 | Delta |
|---|---:|---:|---:|
| Collector RSS | 320.8 MiB | 282.1 MiB | -12.06% |
| CPU sec / 10k events | 1.086363 | 1.104805 | +1.70% |
| Publish p95 | 0.003479s | 0.003068s | -11.82% |
| Acked events / sec | 7535.105 | 7705.790 | +2.27% |

RSS gate passed; CPU change was small and not the target KPI for this phase.

### Phase 5 Adaptive Polling

Control-plane micro benchmark: 1000 NOT_LIVE channels, 5 poll ticks at 15s intervals: `1000, 1015, 1030, 1045, 1060`.

| Metric | Main baseline | Phase 5 | Delta |
|---|---:|---:|---:|
| Live-status fetch calls | 5,000 | 2,000 | -60.00% |
| Fetch calls / channel | 5.0 | 2.0 | -60.00% |
| Elapsed wall time | 3.631s | 1.329s | -63.39% |

The NOT_LIVE channel was still polled again at 60s, so the configured max re-detect delay gate was preserved.

### Phase 6 Control Frame Filtering

Condition: `burst-batch`, 60s duration, 10s warm-up, `RAW_PUBLISH_CONTROL_FRAMES=false`.

| Metric | Result |
|---|---:|
| Frames replayed | 412,551 |
| Acked events | 579,784 |
| Publish failures | 0 |
| Events dropped | 0 |
| Publish p95 | 0.002102s |

The active `burst-batch` replay contains only cmd `93101` and `93102`, so no control frames were available to filter. Unit tests verify the filtering behavior, but there is no meaningful production-data performance delta from the current snapshot.

## Verification Commands

- `python -m pytest -q`
- `docker compose -f kafka/compose.yaml -f kafka/compose.benchmark.yaml config --quiet`
- `python -m collector.benchmark_matrix --phase main-linger0-60s --runs 1 --scenarios burst-batch --skip-input-generation --duration-override-seconds 60 --warmup-seconds 10 --sample-interval-seconds 5`
- `python -m collector.benchmark_matrix --phase phase-2-linger0-60s --runs 1 --scenarios burst-batch --skip-input-generation --duration-override-seconds 60 --warmup-seconds 10 --sample-interval-seconds 5 --baseline-results data/output/benchmarks/results/main-linger0-60s`
- `python -m collector.benchmark_matrix --phase phase-6-filter-60s --runs 1 --scenarios burst-batch --skip-input-generation --duration-override-seconds 60 --warmup-seconds 10 --sample-interval-seconds 5 --baseline-results data/output/benchmarks/results/phase-2-linger0-60s`

## Caveats

- Short screening runs are not a replacement for the original long full matrix. They were used to avoid multi-hour phase loops after the user asked to shorten the process.
- The original full baseline completed before the producer-settings harness fix, so it is retained as historical context but not used as the final merge gate for Phase 2.
- `acked_events` is throughput-sensitive in duration-based loop runs. Gate interpretation used `publish_failures == 0`, `events_dropped_total == 0`, and normalized KPI deltas rather than requiring identical ack counts.
