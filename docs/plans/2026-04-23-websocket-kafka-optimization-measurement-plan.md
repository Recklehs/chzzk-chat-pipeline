# WebSocket to Kafka 최적화 측정 및 단계별 검증 계획

## 요약

- 먼저 현재 시스템에 "비교 가능한 기준선"을 만들고, 이후 변경은 한 번에 하나씩만 적용한다.
- 기본 검증은 재현 가능한 replay 벤치마크로 하고, 마지막 확인은 실제 CHZZK live 채널로 한다.
- 측정 스택은 `collector` 내부 Prometheus 메트릭 + Kafka JMX exporter + cAdvisor + Prometheus/Grafana로 구성한다.
- 1차 목표는 `network bytes / acked event`, `CPU sec / 10k acked events`, `steady-state RSS`, `publish p95 latency`를 개선하면서 이벤트 유실과 계약 변경을 만들지 않는 것이다.

## 공개 인터페이스 및 계측 추가

- `GET /metrics` 추가: Prometheus scrape endpoint. 외부 공개 대신 Docker 내부 네트워크에서만 scrape하도록 구성한다.
- `METRICS_ENABLED`가 true일 때만 `/metrics`를 노출한다. 일반 collector 실행의 기본값은 false로 둔다.
- 기존 `GET /stats`는 유지하고, 운영용 세부 시계열은 `/metrics`로 이동한다.
- 메트릭 라벨 정책: hot-path 메트릭에는 `channel_id` 라벨을 넣지 않고 `cmd`, `result`, `backend`, `reason` 정도만 허용한다.
- Replay 입력 포맷 추가: NDJSON 한 줄당 `channel_id`, `message`, `received_at`, `scenario`를 담는 고정 포맷으로 정의한다.
- 관측 스택 추가: 기존 `kafka/compose.yaml`에 `kafka/compose.benchmark.yaml`를 override로 더해 `collector-bench`, `prometheus`, `grafana`, `cadvisor`, `kafka-jmx-exporter`, topic-init 서비스를 올린다.

## 먼저 측정할 메트릭

### 정확성 가드레일

- `ws_frames_received_total`
- `kafka_publish_attempts_total`
- `kafka_publish_success_total`
- `kafka_publish_failures_total`
- `stats_records_total`
- `events_dropped_total`
- `ws_reconnects_total`

### 지연 및 처리량

- `ws_json_parse_seconds`
- `kafka_publish_seconds`
- `ws_to_publish_ack_seconds`
- `acked_events_per_second`
- `batch_items_per_frame`

### 네트워크 효율

- `ws_frame_bytes_received_total`
- `kafka_payload_bytes_total`
- Kafka broker `BytesInPerSec`, `BytesOutPerSec`, `MessagesInPerSec`
- 파생 KPI: `collector network tx bytes / acked event`, `broker bytes in / acked event`

### CPU 및 메모리

- cAdvisor 기준 collector container CPU usage, memory working set, RSS, network rx/tx
- cAdvisor 기준 kafka container CPU usage, memory working set, network rx/tx
- 파생 KPI: `CPU sec / 10k acked events`, `RSS MiB / monitored channel`

### Kafka 효율

- Kafka `ProduceTotalTimeMs`, `RequestQueueTimeMs`, `RecordsPerRequestAvg`, `RequestBytesAvg`
- `publish_queue_depth`, `publish_inflight_requests`는 현재 `send_and_wait()` 구조에서 정확히 계측하기 어려우므로 0단계 합격 기준에서 제외하고 producer batching 단계에서 별도 검토한다.
- 통합 시나리오에서만 `consumer lag` 추가 측정

### 제어면 비용

- `live_status_requests_total`
- `live_status_request_seconds`
- `access_token_requests_total`
- `access_token_request_seconds`
- `monitored_channels`
- `poll_iterations_total`

## 기준선 측정 설계

- 실험 환경은 로컬 Docker compose를 기준으로 고정하고, 기준 측정 대상은 일반 앱 서버가 아니라 `collector-bench` 전용 컨테이너로 둔다.
- `collector-bench`는 tracked `kafka/benchmark.env`를 `/app/.env.test`로 read-only 마운트하고, benchmark Kafka bootstrap 주소는 compose 내부 주소인 `kafka:29092`로 고정한다.
- Kafka topic은 delete/recreate 대신 `BENCHMARK_RUN_ID` 기반 unique topic으로 생성한다.
- 결과물은 gitignore된 `data/output/benchmarks/` 아래에 저장한다.
- Spark는 1차 기준선에서는 끄고 collector -> Kafka 구간만 측정한다. 마지막 통합 확인에서만 Spark를 켠다.
- 모든 변경 전 현재 mainline에서 아래 3개 시나리오를 각각 3회 반복 측정한다.
- `steady-chat`: 중간 트래픽 replay, 1 busy channel, 15분 측정
- `burst-batch`: batch frame 비중이 높은 고부하 replay, 10분 측정
- `fanout-idle`: 채널 수는 많고 실제 메시지는 적은 상태를 모사해 polling/timer 비용 측정, 30분 측정
- 각 실행은 `2분 warm-up + 측정 구간`으로 나누고, 결과는 중앙값 기준으로 비교한다.
- Replay 벤치마크는 `receive_messages()` 경로를 그대로 사용하고 실제 `KafkaRawPublisher`로 발행해 hot path를 재현한다.
- Control-plane 벤치마크는 `MonitorCoordinator`와 fake live-status client를 사용해 polling 부하를 분리 측정한다.
- Live 검증은 고정된 2~3개 채널로 같은 시간대에 30~60분만 수행하고, 절대값이 아니라 `per event` 정규화 지표만 확인한다.

## 단계별 적용 순서와 검증 게이트

### 0단계: 계측만 추가하고 동작은 바꾸지 않는다

- 목표: 기준선 수집, Grafana 대시보드와 실험 리포트 포맷 확정
- 합격 조건: `/stats`와 이벤트 계약 변화 없음, 측정값이 3회 반복에서 안정적으로 수집됨

### 1단계: Kafka producer batching 설정만 적용한다

- 범위: `linger_ms`, `batch_size`, 필요 시 `acks`/inflight 설정 조정
- 기대 효과: produce request 수 감소, 네트워크 bytes/event 개선, CPU 절감
- 합격 조건: `network tx bytes / event` 또는 `CPU sec / 10k events` 10% 이상 개선, p95 publish latency 15% 이상 악화 없음

### 2단계: Kafka compression만 적용한다

- 기본값: `lz4` 우선, `zstd`는 별도 실험값으로 비교
- 기대 효과: broker/collector 네트워크 사용량 감소
- 합격 조건: `broker bytes in / event` 15% 이상 개선, collector CPU가 10% 이상 악화되지 않음

### 3단계: WebSocket raw 문자열/bytes 재사용으로 파싱 후 재직렬화를 제거한다

- 목표: Kafka value는 raw frame 그대로 쓰고, 통계용 최소 파싱만 남긴다
- 예상 내부 인터페이스 변경: 후속 단계에서 publisher API를 `publish(channel_id, payload_dict)`에서 raw bytes/string을 받을 수 있는 형태로 확장한다. 0단계에서는 현재 계약을 유지한다.
- 기대 효과: collector CPU와 allocation 감소, publish latency 개선
- 합격 조건: `ws_json_parse_seconds`와 `CPU sec / 10k events` 개선, payload 계약 동일

### 4단계: `CmdCounter`와 최근 이벤트 집계 구조를 경량화한다

- 목표: deque/timestamp 중심 구조를 bucket 기반으로 바꿔 메모리와 prune 비용 절감
- 기대 효과: steady-state RSS와 idle/fanout CPU 감소
- 합격 조건: `fanout-idle`에서 RSS 10% 이상 개선, `/stats` 의미 변화 없음

### 5단계: polling/timer 최적화를 적용한다

- 범위: adaptive polling, timer 통합, idle 채널 wake-up 수 감소
- 기대 효과: 채널 수 증가 시 CPU와 외부 API 호출량 감소
- 합격 조건: `live_status_requests_total`과 collector CPU 개선, 방송 시작 감지 지연이 허용 범위 내 유지

### 6단계: control frame 필터링은 마지막 별도 실험으로 둔다

- 이유: 현재 raw frame 전체 발행 계약 변경 가능성이 있어 성능 실험과 정책 결정을 분리해야 한다
- 합격 조건: 데이터 계약 변경 승인이 있어야만 진행

## 테스트 및 리포트 방식

### 단위 테스트

- `/metrics` 노출, metric 값 증가, label cardinality 제한 검증
- replay runner가 `receive_messages()` 경로를 실제로 타는지 검증
- batching/compression/raw reuse 후에도 raw publish 성공/실패 semantics가 유지되는지 검증

### 통합 테스트

- collector + kafka + observability compose 기동 검증
- Kafka JMX exporter와 Prometheus scrape 성공 검증
- 통합 시나리오에서 Spark를 켰을 때 consumer lag 비정상 증가가 없는지 확인

### 실험 리포트

- 변경마다 "baseline 대비 delta" 표를 남긴다.
- 필수 컬럼은 `acked_events`, `publish_failures`, `CPU sec / 10k events`, `RSS MiB`, `network tx bytes / event`, `publish p95`, `broker bytes in / event`이다.
- 한 단계라도 정확성 가드레일이 깨지면 다음 단계로 진행하지 않고 해당 변경을 되돌리거나 보류한다.

## 가정과 기본값

- 주된 최적화 범위는 `collector`의 WebSocket -> Kafka 구간이며 Spark는 1차 목표가 아니다.
- 기준선과 합격 판정은 replay 시나리오를 우선 사용하고, live 채널 측정은 최종 sanity check로만 사용한다.
- 메트릭 endpoint는 운영 공개 API가 아니라 내부 관측용 endpoint로 취급한다.
- `control frame 필터링`은 성능상 유리해도 데이터 계약 승인 전에는 기본 적용하지 않는다.
