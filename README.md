# CHZZK Collector + Spark Bronze / Silver / Gold Pipeline

치지직(CHZZK) 방송 채널의 채팅 WebSocket 이벤트를 수집하는 `collector`와, 원본을 보존하는 Spark Bronze, 이벤트를 정제하는 Spark Silver, 방송별 지표를 집계하는 Spark Gold 작업을 분리해서 관리합니다.

현재 구조의 핵심은 다음과 같습니다.

- `collector` 는 채널 모니터링, CHZZK 메시지 수집, raw 이벤트 발행과 HTTP 기반 채널 제어에만 집중합니다.
- 기본 경로는 **Collector → Kafka → Bronze Delta → Silver Delta → Gold Delta**입니다. GCS 버킷이나 Google 인증 없이 실행합니다.
- Bronze, Silver, Gold는 각각 별도 `spark-submit` 스트리밍 프로세스와 체크포인트를 사용합니다. Gold는 트리거마다 바뀐 방송 회차만 다시 계산합니다.

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
│   ├── bronze_to_silver.py
│   ├── silver_parser.py
│   ├── silver_store.py
│   ├── silver_native.py
│   ├── silver_to_gold.py
│   ├── submit.ps1
│   └── conf/
│       ├── local.properties
│       ├── dev.properties
│       └── prod.properties
├── dashboard/
│   ├── app.py
│   └── templates/
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
- 연결 응답(`cmd=10100`)은 발행 대상에서 제외합니다. 성공 응답(`retCode=0`)을 받으면 SQLite `monitoring_sessions.started_at`에 모니터링 시작 시각을 기록합니다. 같은 모니터링 세션의 재연결은 시작 시각을 덮어쓰지 않습니다. 연결 성공 응답을 받을 때마다 CHZZK `live-detail`의 방송 ID(`liveId`)를 조회해 `monitoring_sessions.live_id`에 저장합니다. Collector를 재시작해도 직전 세션과 `liveId`가 같으면 새 세션을 만들지 않고 그 세션을 다시 열어, 방송 하나가 세션 하나로 유지됩니다. 재연결이 새 방송으로 이어지면 열린 세션을 닫고 새 세션을 엽니다. `liveId`를 받지 못하면 경고를 남기고 새 세션을 엽니다.

## Spark 동작

- `spark/kafka_raw_to_bronze.py`는 Kafka 메시지 한 개를 Bronze Delta의 한 행으로 저장합니다. `bdy` 배열은 펼치지 않습니다.
- `payload_json`에 수신 프레임 전체를 보존하고, 조회용 `cmd`와 수집 메타데이터만 추출합니다. 채팅 여러 건이 담겨도 원본은 한 번만 저장합니다.
- `94008` 같은 객체 본문, 미지원 cmd, 빈 본문도 보존합니다. Kafka에 들어온 JSON이 깨졌거나 cmd를 정수로 변환할 수 없으면 `cmd=null`로 원문을 저장하며, Bronze에서 DLQ로 분기하지 않습니다.
- `10100`은 Collector에서 SQLite 연결 이력으로 처리하며, 과거 Kafka 데이터의 `10100`도 Bronze 적재에서 제외합니다.
- 메시지별 분리, `msgTime` 등의 상세 파싱과 검증은 별도 Silver 작업이 담당합니다.
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

Silver는 다음 여섯 Delta 테이블을 만듭니다. 분석용 네 테이블과 `unclassified_events`에는 채널·발생 시각·원본 좌표 등 공통 컬럼을 각각 저장합니다. 집계는 수행하지 않습니다. [전체 스키마와 품질 규칙](docs/silver-data-contract.md)을 참고하세요.

| 테이블 | 저장 대상 |
|---|---|
| `chat_messages` | 일반 채팅 |
| `donations` | CHAT 후원, 금액은 원본 단위 |
| `subscription_gifts` | 수신자별 구독 선물 알림 |
| `subscription_notifications` | 구독 개월 수 알림, 신규 가입·갱신·결제 구분은 미확정 |
| `unclassified_events` | 유효하지만 아직 지원하지 않는 유형과 시스템 안내 |
| `quarantine` | 필수값 오류 및 원본·업무 키 충돌 |

동일 원본의 재처리와 후원·선물 업무 중복을 제거합니다. 상충하는 후원·선물은 이미 저장된 정상 행도 제외하고 충돌 키를 격리에 남깁니다. 여섯 테이블의 저장은 원자적이지 않으며, 일부 실패 시 같은 배치 재시도로 복구합니다. Gold는 이 삭제를 진행 중인 회차에서는 자동으로, 끝난 회차에서는 `--full` 재계산으로 반영합니다.

같은 Kafka 좌표의 원문·채널·Kafka 시각이 Bronze에서 달라지면 쓰기 전에 배치를 중단합니다. 이미 업무 중복으로 생략한 원본도 검사합니다. Bronze를 수정·삭제하거나 Kafka 토픽을 재생성해 좌표를 재사용할 때는 기존 원천과 분리된 경로로 운영해야 합니다.

## Spark 경로 규칙

`spark/conf/*.properties` 에는 개별 Bronze/checkpoint 경로 대신 base URI 하나만 넣습니다.

예:

```properties
app.bucket.uri=data/output/local/frames
```

그러면 아래 sibling 경로를 자동으로 사용합니다.

- Bronze: `data/output/local/frames/bronze`
- Checkpoint: `data/output/local/frames/checkpoint`
- Silver: `data/output/local/frames/silver/<table>`
- Silver checkpoint: `data/output/local/frames/silver_checkpoint`
- Gold: `data/output/local/frames/gold/<table>`

Silver만 다른 경로를 쓰려면 `app.silver.path`, `app.silver.checkpoint.path`를 properties 파일에 지정합니다. Gold 출력은 `app.gold.path`, Gold가 읽는 Collector SQLite는 `app.control.db.path`(기본 `data/control.db`)로 바꿀 수 있습니다. 입력·출력·각 체크포인트 경로는 겹칠 수 없습니다. 데이터, 체크포인트와 로그는 모두 Git에서 제외된 `data/` 아래에 둡니다.

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
- 로컬 설정의 처리 주기는 Bronze 5초, Silver 10초, Gold 10초입니다. properties에 간격이 없으면 30초입니다. 로컬 설정의 최초 실행은 `app.kafka.startingOffsets=earliest`로 Kafka에 남아 있는 가장 오래된 데이터부터 읽습니다. 재시작할 때는 새 형식의 체크포인트를 사용하며, 이미 Kafka에서 만료된 데이터까지 복구하지는 않습니다.
- 기존 작업에서 전환할 때는 실행 중인 Spark를 종료한 뒤 같은 명령으로 다시 시작합니다. Collector도 재시작해야 앞서 수정한 `10100`의 SQLite 전용 처리가 적용됩니다.
- Collector와 Spark 종료: 각 터미널에서 `Ctrl+C`. Kafka 종료: `docker compose -f kafka/compose.yaml down` (볼륨은 유지).

5. Bronze 테이블이 생성된 뒤 다른 터미널에서 Silver를 시작합니다. 활성화한 Python 가상환경과 JDK 17을 그대로 사용합니다.

```bash
.venv/bin/spark-submit --properties-file spark/conf/local.properties \
  spark/bronze_to_silver.py --properties-file spark/conf/local.properties
```

`SPARK_SILVER_READY`는 스트림 시작을 뜻합니다. 각 배치 완료는 `[spark-silver]` JSON 로그의 `status: completed`로 확인합니다. 최초 실행은 기존 Bronze부터 적재하고 이후 새 데이터를 이어 처리합니다. 실행 시점까지의 데이터만 처리하고 종료하려면 끝에 `--available-now`를 붙입니다. 이후 같은 출력·체크포인트로 연속 실행할 수 있습니다.

현재 Silver는 macOS/Linux의 로컬 파일시스템과 단일 writer를 지원합니다. 출력 경로의 파일 잠금으로 중복 실행을 막고, 파서 버전·입력·체크포인트를 `_silver_contract.json`에 고정합니다. 파서 계약을 바꿔 재구축할 때는 새 Silver 출력과 새 체크포인트를 함께 지정하세요. Bronze와 기존 출력은 보존합니다.

파싱·중복 판정·충돌 격리는 `spark/silver_native.py`가 Spark 식으로 처리합니다. JSON은 VARIANT로 읽어 타입을 구분하고, 키 비교는 join과 groupBy로 executor가 나눠 처리하므로 driver에 행을 모으지 않습니다. 밀린 Bronze를 한 배치로 받아도 driver 메모리를 늘릴 필요가 없습니다. Python 구현(`silver_parser.py`, `silver_store.py`)은 규칙의 기준 답안으로 남아 있고, 아래 `check_silver_native.py`가 두 구현의 결과가 같은지 확인합니다. 바꾼 이유와 검증 결과는 [Silver Spark 네이티브 전환](docs/decisions/2026-10-20-silver-native.md)에 있습니다. 대시보드는 현재 Collector 현황만 보여줍니다.

6. Silver 테이블이 생성된 뒤 다른 터미널에서 Gold를 시작합니다. Gold는 Silver와 Collector의 방송 회차 기록(`data/control.db`)을 읽고, 트리거마다 새 이벤트가 들어온 회차와 진행 중인 회차만 다시 계산해 두 테이블의 해당 행을 교체합니다.

```bash
.venv/bin/spark-submit --properties-file spark/conf/local.properties \
  spark/silver_to_gold.py --properties-file spark/conf/local.properties
```

- `gold/channel_minute_activity`: 채널 × 1분 채팅·참여자·후원·구독 알림 집계
- `gold/broadcast_sessions`: 방송 회차별 요약과 수집 공백 표시(`data_status`)
- `SPARK_GOLD_READY`는 스트림 시작을 뜻합니다. 갱신마다 `[spark-gold]` 로그와 `gold/_gold_manifest.json`에 계산 시각과 다시 계산한 회차가 남습니다. 체크포인트는 `gold_checkpoint`입니다.
- Gold 테이블이 없으면 첫 갱신이 전체 계산을 합니다.

전체를 다시 계산해야 할 때는 Silver와 Gold 스트림을 멈춘 뒤 `--full`로 한 번 실행합니다. 집계 규칙을 바꿨을 때, 또는 Silver 로그의 `removed`가 0보다 클 때(후원·선물 충돌로 이미 끝난 회차의 행이 지워진 경우) 필요합니다.

운영 주기: 하루 방송이 모두 끝난 뒤 Silver 로그에서 `"removed":0`이 아닌 배치가 있었는지 확인하고, 있으면 `--full`을 한 번 실행합니다. 진행 중인 회차의 삭제는 스트림이 자동으로 반영하므로 끝난 회차만 대상입니다.

```bash
grep -h '\[spark-silver\]' data/logs/silver-*.log | grep -v '"removed":0'
```

Gold가 `Sessions … overlap` 또는 `… ends before it starts` 오류로 멈추면, 메시지에 적힌 채널의 `monitoring_sessions` 행을 고칩니다. Collector가 같은 시계로 기록하므로 SQLite를 손으로 고쳤거나 시계가 뒤로 점프했을 때만 생깁니다. 겹친 회차의 `started_at`/`ended_at`을 실제 수집 구간에 맞추거나 잘못 만든 행을 지운 뒤, `--full`을 한 번 실행하고 Gold 스트림을 다시 시작합니다. 회차 행을 손으로 지운 경우에도 `--full`이 Gold의 해당 행을 정리합니다.

```bash
.venv/bin/spark-submit --properties-file spark/conf/local.properties \
  spark/silver_to_gold.py --properties-file spark/conf/local.properties --full
```

배치 간격은 Bronze `spark.streaming.trigger.interval`(로컬 5초), Silver `app.silver.trigger.interval`(10초), Gold `app.gold.trigger.interval`(10초)입니다. 층별 키가 없으면 Bronze 값을 따릅니다. 간격을 정한 근거와 검토한 다른 방식은 [Gold 실시간 갱신 방식 결정](docs/decisions/2026-10-18-gold-live-refresh.md)에, 지표 정의와 방송 날짜·수집 공백 규칙은 [Gold 계약](docs/gold-data-contract.md)에 있습니다.

7. 다른 터미널에서 Gold 대시보드를 시작합니다. Collector와 별도 프로세스라 Kafka나 Collector가 꺼져 있어도 Gold 테이블만 있으면 열립니다.

```bash
.venv/bin/uvicorn dashboard.app:app --port 8001
```

- 대시보드: http://127.0.0.1:8001 (Collector 대시보드 상단의 "방송 분석 보기"로도 이동. 대시보드 아래 "운영자 화면"은 Collector 대시보드로 연결)
- 화면 흐름: 홈(지금 방송 중인 채널, 채널 목록, 최근 방송) → 채널(방송당 평균, 후원, 가장 붐빈 순간, 방송별 채팅 차트, 방송 기록) → 방송(분 단위 채팅·채팅한 사람·후원·구독 알림 차트, 비어 있는 구간 표시, 시간대별 차트). 주소가 화면마다 달라(`#/channel/<id>`, `#/channel/<id>/broadcast/<id>`) 링크로 공유할 수 있고, 10초마다 다시 읽습니다.
- 상단 "지금 채팅 속도"는 Collector의 최근 60초 통계(`/channels`)입니다. Collector가 꺼져 있으면 그 칸만 꺼짐으로 표시합니다.
- 환경 변수: `GOLD_PATH`(기본 `data/output/local/frames/gold`), `COLLECTOR_URL`(서버가 Collector `/channels`를 부르는 주소, 기본 `http://127.0.0.1:8000`), `COLLECTOR_PUBLIC_URL`(브라우저의 "수집 대시보드" 링크 주소, 기본은 `COLLECTOR_URL`과 같음), `GOLD_DASHBOARD_REFRESH_SECONDS`(기본 10). Collector 쪽 링크 주소는 `GOLD_DASHBOARD_URL`(기본 `http://127.0.0.1:8001`, 빈 값이면 숨김)입니다.
- 대시보드에는 인증이 없고 채널별 채팅·후원 통계를 그대로 보여줍니다. 위 명령은 `127.0.0.1`에만 열리므로, 다른 컴퓨터에서 보려고 `--host 0.0.0.0`으로 열 때는 앞단에 인증이 있는 프록시를 두세요.
- 대시보드 의존성(`deltalake` 등)은 `requirements-dashboard.txt`에 있습니다. `requirements.txt`가 함께 설치하며, Collector Docker 이미지에는 들어가지 않습니다.

8. Delta 옛 파일 정리를 예약합니다. Delta는 행을 다시 쓸 때 교체된 파일을 바로 지우지 않으므로, `VACUUM`을 따로 돌려야 디스크가 줄어듭니다. `spark/delta_maintenance.py`는 properties의 경로로 프레임 Bronze·Silver·Gold 테이블을 찾아 보존 기간을 맞추고 `VACUUM`합니다.

```bash
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
.venv/bin/python spark/delta_maintenance.py run --dry-run   # 보존 기간은 실제로 맞추고, 테이블별 지울 파일 수만 출력
.venv/bin/python spark/delta_maintenance.py install-launchd   # 매일 03:00 실행 등록
launchctl print gui/$(id -u)/com.chzzk.delta-vacuum | head     # 등록 확인
launchctl kickstart gui/$(id -u)/com.chzzk.delta-vacuum         # 예약과 같은 방식으로 지금 한 번 실행
```

- 예약은 `~/Library/LaunchAgents/com.chzzk.delta-vacuum.plist` launchd 에이전트입니다. cron과 달리 맥이 잠든 사이 지나간 03:00 실행을 깨어난 뒤 한 번 돌립니다. cron은 놓친 실행을 그냥 건너뛰어서 노트북에서는 정리가 빠졌습니다. 예전에 `install-cron`으로 등록했다면 같은 시각에 두 번 돌지 않게 옛 줄을 지우세요: `crontab -l | grep -v chzzk-delta-vacuum | crontab -`.
- launchd는 셸의 `JAVA_HOME`을 물려받지 않으므로 `install-launchd`가 등록할 때 찾은 JDK를 `SPARK_JAVA_HOME`으로 plist에 넣습니다. JDK를 바꾸면 다시 등록하세요. 등록 내용은 `print-launchd`로 미리 볼 수 있습니다.

- 보존 기간: Bronze·Silver는 지운 파일 7일·로그 30일, Gold는 둘 다 1일입니다. Gold는 10초마다 행을 다시 쓰고 `--full`로 다시 만들 수 있어 길게 보존할 이유가 없습니다. Silver와 Gold의 스트림 체크포인트가 각각 Bronze·Silver 버전을 가리키므로, 스트림을 7일 넘게 멈출 계획이 아니라면 Bronze·Silver 보존 기간을 줄이지 마세요.
- 기본 설정 파일은 `spark/conf/local.properties`이고 `--properties-file`로 바꿀 수 있습니다(`delta_maintenance.py --properties-file <파일> install-launchd`처럼 명령 앞에 둡니다). `install-launchd`에 지정하면 그 절대 경로가 plist에 고정됩니다. 상대 경로를 쓰므로 저장소 루트에서 실행합니다(launchd 에이전트도 루트에서 실행합니다). 실행 로그는 `data/logs/delta_maintenance.log`이고, 테이블마다 `<경로>: vacuumed` 줄이 남습니다.
- 보존 기간 속성은 값이 다를 때만 바꿉니다. 속성 변경은 메타데이터 커밋이라 그 순간 실행 중인 스트림의 쓰기와 충돌할 수 있으므로, 처음 한 번은 스트림을 멈춘 상태에서 실행하는 편이 안전합니다.
- 다른 테이블만 정리하려면 `run --table-path <경로>`를 반복해 지정합니다. 이때는 `--deleted-file-retention`, `--log-retention` 값이 적용됩니다.

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
- Silver 분류·검증·중복·충돌 처리 및 실행 설정 테스트
- Gold 방송 회차 입력, 갱신 대상 판정, 층별 배치 간격 설정 검증

권장 실행:

```bash
.venv/bin/pytest -q
```

실제 Spark/Delta 스트리밍으로 프레임 원문 보존, 다양한 본문 형식, `10100` 제외, 체크포인트 재시작 시 중복 방지를 확인하려면 JDK 17의 `JAVA_HOME`을 설정한 뒤 실행합니다. 임시 테이블을 사용합니다.

```bash
.venv/bin/python tests/check_bronze_schema.py
```

Silver의 실제 Delta 재처리, 격리 저장 직후 장애 복구, Bronze 스트림 체크포인트 재시작과 새 입력 처리는 아래 검사로 확인합니다. 임시 디렉터리에서 실행하며 기존 데이터를 변경하지 않습니다.

```bash
.venv/bin/python tests/check_silver.py
```

Spark 네이티브 Silver가 Python 기준 구현과 같은 결과를 내는지는 아래 검사로 확인합니다. 한 필드씩 깨뜨린 합성 프레임 약 6,500개의 파싱 결과와, 중복·재전달·충돌·이전 격리가 섞인 배치 시퀀스의 여섯 테이블을 비교합니다. `--real`을 붙이면 로컬 Bronze 전체를 새 방식으로 처리해 지금의 Silver와도 비교합니다(임시 디렉터리에 쓰며 기존 데이터는 바꾸지 않습니다).

```bash
.venv/bin/python tests/check_silver_native.py --real
```

Gold의 회차 경계, 지표 정의, 수집 공백 판정, 스트림 증분 결과와 전체 재계산의 일치, 이벤트 없는 회차 종료 반영, Silver 삭제 커밋에서의 스트림 유지와 `--full` 보정은 아래 검사로 확인합니다.

```bash
.venv/bin/python tests/check_gold.py
```

## 참고

- Spark configuration: [spark.apache.org](https://spark.apache.org/docs/latest/configuration.html)
- GCS connector configuration: [GitHub](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md)
- Cloud Storage connector docs: [cloud.google.com](https://docs.cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage)
