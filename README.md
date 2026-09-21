# CHZZK Collector + Spark Bronze Pipeline

치지직(CHZZK) 방송 채널을 등록해 방송 상태를 확인하고, 방송 중일 때 채팅 WebSocket 이벤트를 메시지 버스로 발행하는 `collector` 와, 별도로 실행하는 `spark` Bronze 적재 런타임을 분리해서 관리하는 프로젝트입니다.

현재 구조의 핵심은 아래 두 가지입니다.

- `collector` 는 채널 모니터링, CHZZK 메시지 수집, raw 이벤트 발행과 HTTP 기반 채널 제어에만 집중합니다.
- 기본 경로는 **Collector → Kafka → Spark → 로컬 Delta** 입니다. GCS 버킷이나 Google 인증 없이 실행합니다.
- `spark` 는 `local[*] + spark-submit + properties file` 흐름으로 Bronze 적재를 담당합니다.

## 프로젝트 구조

전체 시스템 구성도와 모듈별 연결은 [아키텍처 문서](docs/architecture.md)를 참고하세요.

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
- 기본 백엔드는 Kafka 입니다. 기존 Pub/Sub 경로는 `EVENT_BUS_BACKEND=pubsub` 로 명시했을 때만 사용합니다.
- raw 이벤트 본문은 CHZZK WebSocket raw frame 전체를 compact JSON UTF-8 bytes 로 유지합니다.
- 채널 제어는 FastAPI HTTP API만 사용합니다.

Pub/Sub 기본 계약:

- Raw topic: CHZZK WebSocket raw frame 발행
- Raw attributes: `channel_id`
- Raw ordering key: `channel_id`

Kafka 계약:

- Topic: `chzzk.events.raw`
- Key: `channel_id`
- Value: compact JSON UTF-8 bytes
- Payload: CHZZK WebSocket raw frame 전체
- 연결 응답(`cmd=10100`)은 발행 대상에서 제외합니다. 성공 응답(`retCode=0`)을 받으면 SQLite `monitoring_sessions.started_at`에 모니터링 시작 시각을 기록합니다. 같은 모니터링 세션의 재연결은 시작 시각을 덮어쓰지 않습니다.

## Spark 동작

- `spark/kafka_raw_to_bronze.py`는 Kafka 메시지 한 개를 Bronze Delta의 한 행으로 저장합니다. `bdy` 배열은 펼치지 않습니다.
- `payload_json`에 수신 프레임 전체를 보존하고, 조회용 `cmd`와 수집 메타데이터만 추출합니다. 채팅 여러 건이 담겨도 원본은 한 번만 저장합니다.
- `94008` 같은 객체 본문, 미지원 cmd, 빈 본문도 보존합니다. Kafka에 들어온 JSON이 깨졌거나 cmd를 정수로 변환할 수 없으면 `cmd=null`로 원문을 저장하며, Bronze에서 DLQ로 분기하지 않습니다.
- `10100`은 Collector에서 SQLite 연결 이력으로 처리하며, 과거 Kafka 데이터의 `10100`도 Bronze 적재에서 제외합니다.
- 메시지별 분리, `msgTime` 등의 상세 파싱과 검증은 이후 Silver의 책임입니다. Silver 작업은 아직 구현하지 않았습니다.
- `ingested_at`은 Spark 처리 시각, `event_date`는 Kafka 메시지 날짜입니다. 메시지 발생 시각은 `payload_json` 안의 `msgTime`으로 보존합니다.
- Windows PowerShell 수동 실행은 `spark-submit --properties-file ...` 경로를 사용합니다.
- `spark.master=local[*]` 와 Delta auto compaction / optimize write 설정은 유지합니다.
- 기본 `local.properties` 는 로컬 디스크에 저장합니다. GCS 설정은 필요하지 않습니다.

Bronze 컬럼:

| 컬럼 | 타입 | 의미 |
|---|---|---|
| `channel_id` | string | Kafka key의 수집 대상 채널 |
| `cmd` | int | 조회용 이벤트 코드, 추출 실패 시 null |
| `payload_json` | string | 수신 프레임 전체, 원문 유지 |
| `topic` / `partition` / `offset` | string / int / long | Kafka 원본 위치 |
| `kafka_timestamp` | timestamp | Kafka 메시지 시각 |
| `ingested_at` | timestamp | Spark 처리 시각 |
| `event_date` | date | Kafka 메시지 날짜, Delta 파티션 |

## Spark 경로 규칙

`spark/conf/*.properties` 에는 개별 Bronze/checkpoint 경로 대신 base URI 하나만 넣습니다.

예:

```properties
app.bucket.uri=data/output/local/frames
```

그러면 아래 sibling 경로를 자동으로 사용합니다.

- Bronze: `data/output/local/frames/bronze`
- Checkpoint: `data/output/local/frames/checkpoint`

기존 `data/output/local/bronze`, `dead_letter`, `checkpoint`는 그대로 보존합니다. 프레임 형식은 기존 테이블에 스키마를 병합하거나 기존 체크포인트를 재사용하지 않고 새 경로에서 시작합니다. `dead_letter` 경로 설정은 이전 설정과의 호환을 위해 남아 있지만 새 작업에서는 쓰지 않습니다.

`app.bucket.uri` 는 기존 설정 이름이며 로컬 디렉터리도 지원합니다. 상대 경로가 동일한 위치를 가리키도록 저장소 루트에서 실행하세요.

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
EVENT_BUS_BACKEND=kafka
# EVENT_BUS_BACKEND=pubsub
```

`APP_ENV=test` 이면 `.env.test`, `APP_ENV=prod` 이면 `.env.prod` 를 읽습니다. raw 이벤트 발행 백엔드는 루트 `.env`의 `EVENT_BUS_BACKEND=pubsub|kafka` 로 선택합니다.

`.env.test` 예시:

```env
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
- `<base>/dead_letter` (이전 설정 호환용, 새 Bronze 작업은 쓰지 않음)
- `<base>/checkpoint`

## 빠른 시작

### 기본: Collector → Kafka → Spark → 로컬 Delta

저장소 루트에서 실행합니다. Python 3.10+와 JDK 17, Docker가 필요합니다.

1. 환경설정과 의존성을 준비합니다. 기존 `.env`가 있다면 `EVENT_BUS_BACKEND=kafka`로 변경하세요.

```bash
cp -n .env.example .env
cp -n .env.test.example .env.test
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt -r requirements-dev.txt
```

2. Kafka를 시작하고 raw topic을 생성합니다.

```bash
docker compose -f kafka/compose.yaml up -d --wait
docker compose -f kafka/compose.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic chzzk.events.raw --partitions 1 --replication-factor 1
```

3. 별도 터미널에서 Spark를 시작합니다. macOS Homebrew JDK 17 설치 환경은 먼저 `export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"`를 실행합니다.

```bash
source .venv/bin/activate
.venv/bin/spark-submit --properties-file spark/conf/local.properties \
  spark/kafka_raw_to_bronze.py --properties-file spark/conf/local.properties
```

`SPARK_BRONZE_READY` 출력 뒤 다음 단계로 진행합니다. 최초 실행은 Maven에서 Kafka/Delta JAR을 내려받습니다.

4. 다른 터미널에서 Collector를 시작한 뒤 대시보드에서 방송 중인 채널을 등록합니다.

```bash
./run_server.sh
```

- 대시보드: http://127.0.0.1:8000/dashboard (로그인 없이 접속)
- Kafka UI: http://127.0.0.1:18080
- Spark 출력: `data/output/local/frames/bronze`, `data/output/local/frames/checkpoint`
- 기본 처리 주기는 30초입니다. 로컬 설정의 최초 실행은 `app.kafka.startingOffsets=earliest`로 Kafka에 남아 있는 가장 오래된 데이터부터 읽습니다. 재시작할 때는 새 형식의 체크포인트를 사용하며, 이미 Kafka에서 만료된 데이터까지 복구하지는 않습니다.
- 기존 작업에서 전환할 때는 실행 중인 Spark를 종료한 뒤 같은 명령으로 다시 시작합니다. Collector도 재시작해야 앞서 수정한 `10100`의 SQLite 전용 처리가 적용됩니다.
- Collector와 Spark 종료: 각 터미널에서 `Ctrl+C`. Kafka 종료: `docker compose -f kafka/compose.yaml down` (볼륨은 유지).

### 선택 사항: Pub/Sub emulator + collector

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
curl -X POST "http://127.0.0.1:8000/channels?channel_input=채널ID"
curl -X PATCH "http://127.0.0.1:8000/channels/채널ID/enabled" -H "Content-Type: application/json" -d '{"enabled": false}'
curl -X DELETE "http://127.0.0.1:8000/channels?channel_id=채널ID"
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

### Kafka + collector만 실행

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

### WebSocket -> Kafka benchmark

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

저장소 루트에서 실행하면 `spark/conf/local.properties` 의 로컬 디렉터리에 적재합니다. GCS 설정은 필요하지 않습니다.

```powershell
.\spark\submit.ps1 -Env local
```

기존 GCS용 dev/prod 프로필은 선택 사항이며 별도 경로와 인증 설정이 필요합니다:

```powershell
.\spark\submit.ps1 -Env dev
.\spark\submit.ps1 -Env prod
```

직접 `spark-submit` 을 쓰고 싶다면:

```powershell
spark-submit --properties-file .\spark\conf\local.properties .\spark\kafka_raw_to_bronze.py --properties-file .\spark\conf\local.properties
```

GCS용 프로필을 명시적으로 사용할 때만 ADC를 준비합니다. 기본 로컬 경로에는 해당하지 않습니다.

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

실제 Spark/Delta 스트리밍으로 프레임 원문 보존, 다양한 본문 형식, `10100` 제외, 체크포인트 재시작 시 중복 방지를 확인하려면 JDK 17의 `JAVA_HOME`을 설정한 뒤 실행합니다. 임시 테이블을 사용합니다.

```bash
.venv/bin/python tests/check_bronze_schema.py
```

## 참고

- Spark configuration: [spark.apache.org](https://spark.apache.org/docs/latest/configuration.html)
- GCS connector configuration: [GitHub](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md)
- Cloud Storage connector docs: [cloud.google.com](https://docs.cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage)
