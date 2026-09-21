# CHZZK Collector + Pub/Sub Raw / Spark Bronze Spec

## 목표

- collector는 채널 모니터링, HTTP 기반 채널 제어, raw 이벤트 발행만 담당한다.
- collector의 기본 이벤트 백엔드는 Google Pub/Sub 이고 `EVENT_BUS_BACKEND=kafka` 로 Kafka fallback 을 사용할 수 있다.
- Spark는 별도 수동 실행 런타임으로 유지하며, 현재는 Kafka raw topic만 읽어 Bronze / Dead Letter Delta를 적재한다.
- collector 실행 프로필과 raw 이벤트 백엔드 선택은 루트 `.env` 의 `APP_ENV=test|prod`, `EVENT_BUS_BACKEND=pubsub|kafka` 로 고르고 실제 연결값은 `.env.test` 또는 `.env.prod` 에서 읽는다.

## 아키텍처

### 실시간 수집 경로

- `chzzk_collector_server.py`
- `collector/app.py`
- `collector/runtime.py`
- `collector/publisher.py`

### 처리 단계

1. CHZZK live status polling으로 방송 상태를 확인한다.
2. 방송이 `OPEN` 인 채널만 WebSocket에 연결한다.
3. 수신한 raw frame 전체를 compact JSON UTF-8 bytes 로 이벤트 버스에 발행한다.
4. publish 성공 후 통계를 반영한다.
5. Spark는 별도로 Kafka raw topic을 소비해 Bronze / Dead Letter 출력을 갱신한다.

## Collector 런타임 계약

### 채널 제어

- 공식 제어 인터페이스는 HTTP API와 dashboard다.
- `POST /channels`
- `PATCH /channels/{channel_id}/enabled`
- `DELETE /channels`
- `GET /channels`
- `GET /stats`
- `GET /dashboard`

### Pub/Sub raw 계약

- topic: `PUBSUB_RAW_TOPIC`
- message `data`: CHZZK WebSocket raw frame 전체를 compact JSON UTF-8 bytes 로 직렬화한 값
- message attribute: `channel_id`
- ordering key: `channel_id`

### Kafka fallback 계약

- topic: `KAFKA_TOPIC`
- key: `channel_id`
- value: compact JSON UTF-8 bytes
- payload: CHZZK WebSocket raw frame 전체

## Env 프로필 계약

- 루트 `.env`
  - `APP_ENV=test|prod`
  - `HOST`
  - `PORT`
  - `EVENT_BUS_BACKEND=pubsub|kafka`
- `.env.test`
  - Pub/Sub emulator 또는 Kafka 개발 연결값
- `.env.prod`
  - 실제 GCP Pub/Sub 또는 Kafka 운영 연결값
- `APP_ENV=test` 이고 `EVENT_BUS_BACKEND=pubsub` 이면 collector 시작 전에 raw topic bootstrap 스크립트를 실행한다.
- `APP_ENV=prod` 에서는 topic 자동 생성 없이 존재 검증만 수행한다.
- macOS에서 Pub/Sub gRPC DNS 이슈가 있으면 `GRPC_DNS_RESOLVER=native` 를 사용한다.
- 로컬 Kafka 브로커와 UI는 `kafka/compose.yaml` 로 별도 실행할 수 있다.

## 보조 도구

- `collector/bootstrap_pubsub_raw_topic.py`
  - test + Pub/Sub 모드에서 raw topic을 idempotent 하게 생성한다.
- `collector/watch_pubsub_raw_messages.py`
  - 현재 `APP_ENV` 프로필을 읽어 raw topic을 구독하는 debug watcher를 실행한다.
- `collector/watcher_pubsub_raw_messages.py`
  - 위 watcher의 alias entrypoint다.

## Spark 계약

- Spark는 아직 Pub/Sub를 직접 읽지 않는다.
- `spark/kafka_raw_to_bronze.py` 가 Kafka raw topic을 읽는다.
- base URI 입력 예시: `gs://my-bucket/chzzk/local`
- Bronze: `<base>/bronze`
- Dead Letter: `<base>/dead_letter`
- Checkpoint: `<base>/checkpoint`

## 검증 체크리스트

- [ ] 앱 startup 시 선택된 raw publisher가 시작된다.
- [ ] `APP_ENV=test` + `EVENT_BUS_BACKEND=pubsub` 에서 raw topic bootstrap 이 선행된다.
- [ ] 단일 frame은 raw bus 1회 publish + stats 1건으로 반영된다.
- [ ] batch frame은 raw bus 1회 publish + stats batch item 수로 반영된다.
- [ ] raw publish 실패 시 stats가 증가하지 않고 수신 루프가 종료된다.
- [ ] dashboard 로그인은 browser `multipart/form-data` 제출로도 성공한다.
- [ ] watcher는 현재 `APP_ENV` 기준 `.env.test` 또는 `.env.prod` 를 읽는다.
- [ ] Spark는 Kafka raw topic을 읽어 Bronze / Dead Letter를 적재한다.
