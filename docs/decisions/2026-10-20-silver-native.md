# Silver Spark 네이티브 전환

- 날짜: 2026-10-20
- 상태: 채택
- 관련: [Silver 계약](../silver-data-contract.md), `spark/silver_native.py`, `tests/check_silver_native.py`

## 결정

Silver의 파싱, 중복 판정, 충돌 격리를 Spark 식으로 옮긴다. 스트림(`spark/bronze_to_silver.py`)은 `spark/silver_native.py`를 쓴다. 기존 Python 구현(`silver_parser.py`, `silver_store.py`)은 지우지 않고 **규칙의 기준 답안**으로 둔다. 두 구현이 같은 입력에서 같은 여섯 테이블을 내는지 `tests/check_silver_native.py`로 확인한다.

## 배경

기존 Silver는 마이크로배치의 Bronze 행을 `toLocalIterator()`로 driver에 모은 뒤 Python으로 한 줄씩 파싱하고 1만 건씩 판정했다. Spark는 읽기와 MERGE만 했다.

- 코어 하나만 일했다. 판정용 기존 행도 `collect()`로 driver에 모았다.
- Silver가 꺼져 있는 동안 Bronze가 쌓이면 다시 켤 때 첫 배치에 모두 몰렸다. 9월 23~24일 데이터를 복구할 때는 9만 6천 행짜리 파일 하나가 driver 기본 메모리(1GB)를 넘어 `--driver-memory 8g`로 다시 띄워야 했다.

## 검토한 선택지

| 선택지 | 내용 | 판단 |
|---|---|---|
| 메모리 증설 | 밀렸을 때만 driver 메모리를 늘림 | 즉시 해결되지만 밀린 양이 더 커지면 다시 부족 |
| `maxBytesPerTrigger` | 밀린 Bronze를 여러 배치로 나눠 받음 | 한 줄 변경. 큰 파일 하나는 여전히 한 번에 옴 |
| B: pandas UDF, 파싱만 | 파싱을 executor의 Python으로 | 판정을 위해 다시 driver로 모으므로 메모리 문제는 남음 |
| B: pandas UDF, 채널별 | 파싱은 코어마다, 판정은 `channel_id`로 묶어 채널마다 | 출처 키는 Kafka 메시지 하나, 즉 채널 하나를 가리키고 업무 키에는 `channel_id`가 있어 충돌 연쇄가 채널을 넘지 않는다. 기존 함수를 그대로 쓸 수 있으나 동시 처리 수가 채널 수로 묶이고, 한 채널의 배치는 Python 프로세스 하나에 들어가야 함 |
| **A: Spark 네이티브** | 규칙 전체를 컬럼 식, join, groupBy로 번역 | **채택.** 한 채널에 몰려도 코어 전체가 나눠 처리하고 메모리 상한이 사실상 없음. 포트폴리오에서 Spark 정제를 그대로 보여줄 수 있음. 대신 규칙 전체를 다시 쓰고 동일성을 증명해야 함 |

## 번역 방법

### JSON 타입을 Python과 똑같이 구분하기

기존 파서는 JSON 값의 타입을 엄격하게 본다. 정수 `123`과 숫자 문자열 `"123"`은 받고 `true`, `1.0`, 범위를 넘는 정수는 거부한다. 키가 없는 것과 값이 `null`인 것도 구분한다. `from_json`은 타입이 안 맞으면 조용히 null을 돌려줘 이 구분이 사라진다. 그래서 Spark 4.0의 VARIANT로 읽었다.

| Python 검사 | Spark 식 |
|---|---|
| `json.loads` 성공 여부 | `try_parse_json`이 null인지 |
| `isinstance(v, dict)` | `try_variant_get(v, '$', 'map<string,variant>')`가 null이 아닌지 |
| `isinstance(v, str)` | `schema_of_variant(v) = 'STRING'` |
| `type(v) is int` (bool 제외) | `schema_of_variant(v) = 'BIGINT'`. `1.0`은 `DECIMAL`, `true`는 `BOOLEAN`, 2⁶³ 이상은 `DECIMAL`/`DOUBLE`로 나와 구분됨 |
| 숫자 문자열 `[+-]?[0-9]+` | `\A[+-]?[0-9]+\z` 정규식 뒤 `try_cast`. Java의 `$`는 끝 줄바꿈 앞에서도 맞으므로 `\z`를 씀 |
| `'key' in d` / `d.get(key) is None` | `try_variant_get(v, '$.key', 'variant')`: 키가 없으면 SQL NULL, JSON null이면 `is_variant_null` |
| `str.strip()`이 비었는지 | Python `str.isspace()`가 참인 29개 문자로 만든 정규식 |
| 중복 키는 마지막 값 | `spark.sql.variant.allowDuplicateKeys=true` |
| `datetime` 범위 밖이면 `OverflowError` | 0001-01-01 ~ 9999-12-31 마이크로초 범위 검사, KST 변환이 넘치는 마지막 9시간도 검사 |

### 판정

- **키 문자열:** 격리 테이블의 `conflict_keys`에 저장되는 키는 Python `json.dumps` 결과와 글자까지 같아야 이전 격리 기록과 맞물린다. 출처 키는 `concat`으로, 업무 키는 `to_json(array(...))`로 만든다. 이스케이프 결과가 같은지 확인했다.
- **충돌 감지:** 출처 키별 서명(처리 시각을 뺀 전체 값) 종류 수, 업무 키별 핵심 필드 종류 수를 `groupBy`와 `countDistinct`로 센다. 이전 격리 기록은 출처 좌표와 `conflict_keys`로 찾아 같은 코드로 다시 막는다.
- **연쇄 차단:** 키를 공유하는 행끼리 이어진 묶음(connected components)을 찾는다. 노드마다 가장 작은 키를 이름표로 두고, 이름표가 더 바뀌지 않을 때까지 join을 반복한다. 충돌이나 이전 격리가 없는 배치(대부분)에서는 이 반복을 건너뛴다.
- **격리 행 합치기:** 같은 `(출처 키, parser_version)`이면 먼저 나온 행의 값을 쓰고 배열은 합집합, 감지 시각은 가장 이른 값을 쓴다. 순서는 파싱 실패 → 배치 행 → 기존 행이다.
- **쓰기 순서:** 기존과 같다. 격리 MERGE → 테이블별 삭제 → 추가. 격리를 쓴 직후 장애가 나도 재시도가 삭제를 끝낸다.

### 계획 크기

중간 결과마다 `localCheckpoint()`로 계획을 끊는다. 처음에는 끊지 않았더니, 기존 행 조회 다섯 개가 각각 파싱 계획 전체를 품었다(테이블당 26만~53만 자). 이 계획을 합치는 분석 단계(`DeduplicateRelations`)에서 한 행짜리 배치도 1GB driver 메모리가 부족했다. 끊은 뒤에는 2천~6천 자로 줄었다.

체크포인트는 배치가 끝나면 직접 해제한다(`release_checkpoints`). 처음에는 Spark의 자동 정리에 맡겼는데, 자동 정리는 일부만 지워서 배치마다 체크포인트가 남았다(배치 5개에 11~16개). 라이브 스트림을 약 50분 돌리자 block manager가 차서 작업 결과를 잃고(`TaskResultLost`) Silver가 멈췄다. 해제한 뒤에는 배치가 끝날 때마다 남는 체크포인트가 0개이고, `check_silver_native.py`가 시나리오마다 이를 확인한다.

### broadcast 힌트를 쓰지 않음

기존 행과 이전 격리 기록을 찾는 join에는 broadcast 힌트를 걸지 않는다. 키 집합은 배치 크기만큼 커지는데, 힌트가 있으면 그 집합을 driver에 모은 뒤 다시 뿌린다. 원본 무결성 검사(`validate_bronze_source`)의 좌표 join도 같은 이유로 힌트를 뺐다. Spark가 크기를 보고 broadcast나 sort-merge를 고른다. 기준 구현의 나머지 broadcast는 1만 건 단위라 크기가 정해져 있어 그대로 둔다.

## 동일성 검증

`tests/check_silver_native.py`가 세 단계로 비교한다. 처리 시각(`silver_processed_at`, `detected_at`)만 빼고 모든 컬럼을 본다.

| 단계 | 입력 | 결과 |
|---|---|---|
| 파싱 | 일곱 가지 이벤트 유형마다 필드 하나씩을 43가지 값(키 없음 포함)으로 깨뜨린 합성 프레임 6,540개. 프레임 수준 오류, 시각 경계, 채널 값 포함 | 6,575건 같음 |
| 쓰기 | check_silver 시나리오, 같은 배치 안 중복, 출처 키 충돌, 연쇄 충돌, 무작위 12배치(후원 ID 충돌, 재전달, 잘못된 금액) | 다섯 시나리오 모두 여섯 테이블 같음 |
| 실제 데이터 | 로컬 Bronze 전체를 배치 하나로 처리한 결과와, 스트림이 배치마다 쌓은 지금의 Silver. 라이브 스트림이 돌고 있어도 어긋나지 않도록, Silver 체크포인트의 마지막 커밋 배치가 읽은 Bronze 버전과 그 배치가 끝난 시점의 Silver 버전끼리 비교한다 | 246,504프레임(Python 구현이 861배치로 만든 Silver): 채팅 542,118행 등 차이 0. 라이브 수집 뒤 250,314프레임: 채팅 547,519행 등 차이 0 |

`tests/check_silver.py`의 원본 무결성, 격리 직후 장애 복구, 스트림 재시작 검사도 새 구현으로 통과했다.

## 성능 (로컬, 2026-10-20)

| 상황 | Python 기준 구현 | Spark 네이티브 |
|---|---|---|
| Bronze 전체를 한 배치로 | 246,504프레임에 driver 8g 필요 | 250,314프레임 85초, 기본 driver 1g |
| 테이블이 다 찬 상태에서 200프레임 배치 | 1.6초 (다른 작업이 돌 때 2.8초) | 3.3초 (다른 작업이 돌 때 4.8초) |

처음 기록한 "54초, driver 1g"는 검사 스크립트가 driver 메모리를 6g로 잡은 상태에서 잰 값이어서 고쳤다. 지금 검사는 일부러 기본값 1g로 돌린다.

작은 배치에서는 네이티브가 더 느리다. 배치마다 띄우는 Spark 작업(체크포인트, 개수 세기, 테이블별 MERGE 확인)이 고정 비용으로 붙기 때문이다. Silver 트리거 10초 안에는 충분히 들어온다. 줄이려면 테이블별 확인을 한 번의 집계로 묶는 정도가 다음 단계다.

## 의도된 차이

| 차이 | 내용 | 판단 |
|---|---|---|
| `NaN`, `Infinity` | Python `json.loads`는 받고, VARIANT는 JSON 해석을 실패해 프레임 전체가 `INVALID_JSON`이 된다 | 표준 JSON이 아니며 CHZZK가 보내지 않는다 |
| 짝 없는 서로게이트(`\ud800`) | Python은 그대로 두고, VARIANT는 `?`로 바꾼다 | Python 쪽도 Delta에 저장할 때 UTF-8로 바꿀 수 없어 실패한다 |
| 1만 건 경계 | 기준 구현은 배치를 1만 건씩 나눠 차례로 커밋하므로, 경계를 넘는 충돌 묶음은 격리 행의 `related_sources` 예시가 달라질 수 있다. 네이티브는 배치 전체를 한 번에 판정한다 | 어느 행을 막고 지우는지는 같다. 동일성 검사는 1만 건 미만 배치로 한다 |
| 로그의 `quarantine_upserts` | 기준 구현은 1만 건 단위마다 세고, 네이티브는 배치당 한 번 센다 | 로그 숫자일 뿐 테이블에는 영향 없음 |

## 이후

- 규칙을 바꿀 때는 Python 기준 구현과 네이티브를 같이 고치고 `check_silver_native.py`로 확인한다. 파서 버전이 바뀌면 지금처럼 새 출력과 체크포인트로 다시 정제한다.
- Python 구현을 지우려면 그 역할을 대신할 기대 결과 묶음(합성 입력과 정답 테이블)을 테스트로 먼저 고정해야 한다.
