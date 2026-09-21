# CHZZK Collector + Spark Bronze Pipeline

치지직(CHZZK) 방송 채널을 등록해 방송 상태를 확인하고, 방송 중일 때 채팅 WebSocket 이벤트를 메시지 버스로 발행하는 `collector` 와, 별도로 실행하는 `spark` Bronze 적재 런타임을 분리해서 관리하는 프로젝트입니다.

현재 구조의 핵심은 아래 두 가지입니다.

- `collector` 는 채널 모니터링, CHZZK 메시지 수집, raw 이벤트 발행과 HTTP 기반 채널 제어에만 집중합니다.
- `spark` 는 `local[*] + spark-submit + properties file + GCS connector + ADC` 흐름으로만 Bronze 적재를 담당합니다.

## 프로젝트 구조

```text
.
├── collector/
│   ├── app.py
│   ├── config.py
│   ├── control.py
│   ├── publisher.py
│   ├── runtime.py
│   └── templates/
├── spark/
│   ├── config.py
│   ├── session.py
│   ├── job.py
│   ├── delta_maintenance.py
│   ├── kafka_raw_to_bronze.py
│   ├── submit.ps1
│   └── conf/
│       ├── local.properties
│       ├── dev.properties
│       └── prod.properties
├── chzzk_collector_server.py
├── chzzk_control.py
├── kafka_raw_publisher.py
├── run_collector.ps1
├── run_server.sh
├── .env.example
└── tests/
```

## Collector 동작

- 등록 채널은 `data/control.db` 에 저장됩니다.
- 활성화된 채널의 방송 상태를 주기적으로 확인합니다.
- 방송이 `OPEN` 인 채널만 CHZZK WebSocket 에 연결합니다.
- 기본 백엔드는 Google Pub/Sub 이며, `EVENT_BUS_BACKEND=kafka` 로 Kafka fallback 을 사용할 수 있습니다.
- raw 이벤트 본문은 CHZZK WebSocket raw frame 전체를 compact JSON UTF-8 bytes 로 유지합니다.
- 채널 제어는 FastAPI HTTP API만 사용합니다.

Pub/Sub 기본 계약:

- Raw topic: CHZZK WebSocket raw frame 발행
- Raw attributes: `channel_id`
- Raw ordering key: `channel_id`

Kafka fallback 계약:

- Topic: `chzzk.events.raw`
- Key: `channel_id`
- Value: compact JSON UTF-8 bytes
- Payload: CHZZK WebSocket raw frame 전체

## Spark 동작

- `spark/kafka_raw_to_bronze.py` 가 Kafka raw topic 을 읽어 Bronze / dead-letter Delta 를 적재합니다.
- Windows PowerShell 수동 실행은 `spark-submit --properties-file ...` 경로를 사용합니다.
- `spark.master=local[*]` 와 Delta auto compaction / optimize write 설정은 유지합니다.
- GCS 경로를 쓰면 ADC 기반 인증과 GCS connector 설정을 적용합니다.

## Spark 경로 규칙

`spark/conf/*.properties` 에는 개별 Bronze/checkpoint 경로 대신 base URI 하나만 넣습니다.

예:

```properties
app.bucket.uri=gs://my-bucket/chzzk/local
```

그러면 아래 sibling 경로를 자동으로 사용합니다.

- Bronze: `gs://my-bucket/chzzk/local/bronze`
- Dead letter: `gs://my-bucket/chzzk/local/dead_letter`
- Checkpoint: `gs://my-bucket/chzzk/local/checkpoint`

`your-bucket` 같은 placeholder 가 남아 있으면 시작 전에 fail-fast 합니다.

## Spark properties 파일

Windows PowerShell 수동 실행은 `spark/conf/*.properties` 를 기준으로 합니다.

필수 app 키:

- `app.kafka.bootstrap.servers`
- `app.kafka.topic`
- `app.kafka.startingOffsets`
- `app.bucket.uri`

기본 공통 설정:

- `spark.master=local[*]`
- `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`
- `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`
- `spark.databricks.delta.autoCompact.enabled=true`
- `spark.databricks.delta.optimizeWrite.enabled=true`
- `spark.hadoop.fs.gs.auth.type=APPLICATION_DEFAULT`

## 환경 변수

collector는 루트 `.env`에서 실행 프로필을 고르고, 실제 collector 설정은 `.env.test` 또는 `.env.prod`에서 읽습니다.

처음에는 아래처럼 복사해서 시작하면 됩니다.

```bash
cp .env.example .env
cp .env.test.example .env.test
cp .env.prod.example .env.prod
```

루트 `.env` 예시:

```env
APP_ENV=test
HOST=0.0.0.0
PORT=8000
EVENT_BUS_BACKEND=pubsub
# EVENT_BUS_BACKEND=kafka
```

`APP_ENV=test` 이면 `.env.test`, `APP_ENV=prod` 이면 `.env.prod` 를 읽습니다. raw 이벤트 발행 백엔드는 루트 `.env`의 `EVENT_BUS_BACKEND=pubsub|kafka` 로 선택합니다.

`.env.test` 예시:

```env
API_KEY=your-secret-key
SESSION_SECRET_KEY=change-this-for-dashboard-session
PUBSUB_PROJECT_ID=local-project
PUBSUB_RAW_TOPIC=chzzk-events-raw
PUBSUB_EMULATOR_HOST=127.0.0.1:8085
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=chzzk.events.raw
KAFKA_CLIENT_ID=chzzk-collector
COLLECTOR_INSTANCE_ID=collector-local
CHZZK_API_BASE_URL=https://api.chzzk.naver.com
CHZZK_API_TIMEOUT_SECONDS=10
CHZZK_LIVE_POLL_SECONDS=15
CONTROL_DB_PATH=data/control.db
DASHBOARD_REFRESH_SECONDS=5
REDIS_ENABLED=false
# REDIS_URL=redis://localhost:6379/0
# REDIS_HOST=localhost
# REDIS_PORT=6379
# REDIS_DB=0
# REDIS_PASSWORD=
# REDIS_SSL=false
# REDIS_SOCKET_TIMEOUT_SECONDS=0.2
```

`.env.prod` 에서는 emulator 변수 없이 실제 GCP 프로젝트와 topic 이름을 사용합니다. macOS에서 Pub/Sub startup이 DNS 해석 단계에서 멈추면 `GRPC_DNS_RESOLVER=native` 를 `.env.prod` 에 추가하세요.

대시보드 API Redis 캐시는 `REDIS_URL` 이 있거나 `REDIS_ENABLED=true` 일 때 활성화됩니다. Redis 장애나 JSON parse 실패는 API 실패로 전파하지 않고 캐시 miss로 처리합니다.

Kafka를 쓰려면 `.env.test` 또는 `.env.prod` 안의 `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_CLIENT_ID` 값을 채운 뒤, 루트 `.env`에서 `EVENT_BUS_BACKEND=kafka` 로 바꾸면 됩니다.

선택값:

- `GOOGLE_APPLICATION_CREDENTIALS`
- `GOOGLE_CLOUD_PROJECT`
- `GCS_BUCKET_URI`
- `SPARK_APP_NAME`
- `SPARK_BRONZE_PROCESSING_TIME`

`GCS_BUCKET_URI` 를 env-mode Spark에서 쓰면 아래 경로를 자동 파생합니다.

- `<base>/bronze`
- `<base>/dead_letter`
- `<base>/checkpoint`

## 빠른 시작

### 1. Pub/Sub emulator + collector

로컬에서 Pub/Sub 경로를 확인하려면 아래 순서로 실행합니다.

1. 예제 파일을 실제 env 파일로 복사합니다.

```bash
cp .env.example .env
cp .env.test.example .env.test
```

2. 필요하면 `.env` 와 `.env.test` 값을 수정합니다.

루트 `.env` 에서는 아래처럼 Pub/Sub를 선택합니다.

```env
EVENT_BUS_BACKEND=pubsub
```

3. emulator를 실행합니다.

```bash
gcloud beta emulators pubsub start --project=local-project --host-port=127.0.0.1:8085
```

4. 새 터미널에서 collector를 실행합니다.

```bash
./run_server.sh
```

`APP_ENV=test` 이고 `EVENT_BUS_BACKEND=pubsub` 이면 `run_server.sh` 가 collector 시작 전에 raw topic 생성 스크립트를 자동 실행합니다. topic이 이미 있어도 그대로 진행합니다.

대시보드:

- `http://127.0.0.1:8000/dashboard`

채널 제어는 대시보드 또는 아래 HTTP API를 사용합니다.

```bash
curl -X POST "http://127.0.0.1:8000/channels?channel_input=채널ID" -H "X-API-Key: change-me"
curl -X PATCH "http://127.0.0.1:8000/channels/채널ID/enabled" -H "X-API-Key: change-me" -H "Content-Type: application/json" -d '{"enabled": false}'
curl -X DELETE "http://127.0.0.1:8000/channels?channel_id=채널ID" -H "X-API-Key: change-me"
```

raw 메시지를 emulator에서 계속 보고 싶다면 별도 터미널에서 아래 watcher를 실행합니다.

```bash
./.venv/bin/python -m collector.watch_pubsub_raw_messages
```

또는 아래 alias 이름으로 실행해도 됩니다.

```bash
./.venv/bin/python -m collector.watcher_pubsub_raw_messages
```

watcher는 루트 `.env`의 `APP_ENV`를 읽고 `.env.test` 또는 `.env.prod`를 자동 적용한 뒤, 그 설정의 `PUBSUB_RAW_TOPIC` 기준 `<topic>-debug` subscription을 생성해서 계속 출력합니다.

watcher와 raw topic bootstrap은 `EVENT_BUS_BACKEND=pubsub` 일 때만 유효합니다.

### 2. Kafka + collector

로컬에서 Kafka 경로를 확인하려면 저장소의 `kafka/compose.yaml` 로 브로커와 UI를 먼저 올립니다.

1. 예제 파일을 실제 env 파일로 복사합니다.

```bash
cp .env.example .env
cp .env.test.example .env.test
```

2. 루트 `.env` 에서 Kafka를 선택합니다.

```env
EVENT_BUS_BACKEND=kafka
```

3. 필요하면 `.env.test` 의 Kafka 연결 값을 수정합니다. host에서 collector를 실행할 때는 `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` 를 사용합니다.

4. Kafka와 Kafka UI를 실행합니다.

```bash
docker compose -f kafka/compose.yaml up -d
```

5. 새 터미널에서 collector를 실행합니다.

```bash
./run_server.sh
```

Kafka UI:

- `http://127.0.0.1:18080`

### 3. WebSocket -> Kafka benchmark

Phase 0 기준선 측정은 일반 collector 앱이 아니라 `collector-bench` 전용 컨테이너에서 실행합니다. 이 컨테이너는 실제 `receive_messages()` + `KafkaRawPublisher` 경로를 사용하고, Kafka topic은 `BENCHMARK_RUN_ID`가 붙은 unique topic으로 생성합니다.

기본 smoke replay:

```bash
BENCHMARK_RUN_ID=steady-smoke docker compose \
  -f kafka/compose.yaml \
  -f kafka/compose.benchmark.yaml \
  up --build collector-bench prometheus cadvisor kafka-jmx-exporter
```

대용량 replay 입력은 gitignore된 `data/output/benchmarks/input/` 아래에 두고, 실행 시 `BENCHMARK_INPUT`으로 지정합니다.

```bash
BENCHMARK_RUN_ID=steady-001 \
BENCHMARK_INPUT=/app/data/output/benchmarks/input/steady-chat.ndjson \
docker compose -f kafka/compose.yaml -f kafka/compose.benchmark.yaml up --build collector-bench
```

결과 요약 JSON은 `data/output/benchmarks/results/`에 저장됩니다. Prometheus, Grafana, cAdvisor, Kafka JMX exporter 포트는 각각 `19090`, `13000`, `18081`, `19404`입니다.

## Windows PowerShell 실행

### 1. collector 실행

`.env.example`, `.env.test.example`, `.env.prod.example` 을 각각 `.env`, `.env.test`, `.env.prod` 로 복사합니다.

루트 `.env` 에서 `EVENT_BUS_BACKEND=pubsub` 또는 `EVENT_BUS_BACKEND=kafka` 를 선택합니다.

Pub/Sub emulator 예시:

```powershell
$env:APP_ENV="test"
```

```powershell
.\run_collector.ps1
```

`APP_ENV=test` 이고 `EVENT_BUS_BACKEND=pubsub` 이면 PowerShell 스크립트도 collector 시작 전에 raw topic 생성 스크립트를 자동 실행합니다.

Kafka를 쓰면 `docker compose -f kafka/compose.yaml up -d` 로 브로커를 먼저 띄운 뒤, `.env.test` 또는 `.env.prod` 의 `KAFKA_BOOTSTRAP_SERVERS` 값을 host 기준 주소로 맞춥니다.

기본 주소:

- dashboard: `http://127.0.0.1:8000/dashboard`

### 2. Spark 수동 실행

`spark/conf/local.properties` 에서 `app.bucket.uri` 를 실제 GCS base URI 로 바꾼 뒤 실행합니다.

```powershell
.\spark\submit.ps1 -Env local
```

다른 env:

```powershell
.\spark\submit.ps1 -Env dev
.\spark\submit.ps1 -Env prod
```

직접 `spark-submit` 을 쓰고 싶다면:

```powershell
spark-submit --properties-file .\spark\conf\local.properties .\spark\kafka_raw_to_bronze.py --properties-file .\spark\conf\local.properties
```

ADC 는 보통 둘 중 하나로 준비합니다.

- `gcloud auth application-default login`
- `GOOGLE_APPLICATION_CREDENTIALS` 환경변수 설정

## 개발용 실행

기존 bash 실행 경로도 유지하며, collector 전용 의존성만 설치합니다.

```bash
./run_server.sh
```

## 테스트

현재 개발환경에서는 Windows 네이티브 실행, ADC 인증, GCS write, 실제 `spark-submit` end-to-end 는 강제 검증하지 않습니다.

대신 아래를 검증합니다.

- collector API 회귀 테스트
- Spark runtime 설정 / session 테스트
- Spark Bronze job 단위 테스트

권장 실행:

```bash
.venv/bin/pytest -q
```

## 참고

- Spark configuration: [spark.apache.org](https://spark.apache.org/docs/latest/configuration.html)
- GCS connector configuration: [GitHub](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md)
- Cloud Storage connector docs: [cloud.google.com](https://docs.cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage)
