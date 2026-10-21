# Gold 실시간 갱신 방식 결정

작성일: 2026-10-18. 상태: **채택, 구현 완료**(`spark/silver_to_gold.py`). 진행 중인 회차를 처음부터 다시 계산하던 부분은 [분 단위 증분 갱신 결정](2026-10-21-gold-minute-incremental.md)(2026-10-21)으로 바뀌었다. 회차 전체 고유 참여자를 빼고, 회차 행을 분 단위 행에서 만든다. 데이터 계약은 [Gold 계약](../gold-data-contract.md)에 있다. 그림 설명은 [방송 성적표 갱신](../explainers/gold-session-refresh.html)에 있다.

Gold를 어떻게 갱신할지 정하면서 검토한 선택지와 근거를 남긴다. 숫자는 2026-10-14~18 로컬 데이터(Bronze 프레임 88,129개, Silver 채팅 181,207건, 후원 710건, 방송 회차 8개)로 직접 측정하거나 실행한 결과다.

## 1. 결정 요약

| 항목 | 결정 |
|---|---|
| Gold 갱신 방식 | Silver를 스트리밍으로 읽고, 트리거마다 **바뀐 방송 회차만 처음부터 다시 계산**해 Gold 행을 교체한다 |
| 전체 재계산 | `--full` 옵션으로 유지한다. 최초 구축, 집계 규칙 변경, Silver 삭제 보정에만 쓴다 |
| Silver 삭제 반영 | 스트림은 `skipChangeCommits`로 삭제 커밋을 건너뛴다. Silver 로그의 `removed`가 0보다 크면 `--full`을 실행한다. CDF는 쓰지 않는다 |
| 이벤트가 없을 때 | 드라이버 타이머가 트리거 간격마다 진행 중·종료된 회차를 갱신한다 |
| 실행 단위 | Gold는 Silver와 별도 프로세스·체크포인트다 |
| 트리거 간격 | Bronze 5초, Silver 10초, Gold 10초. 예상 지연 평균 약 20초 |
| 초 단위 화면 | Collector의 기존 최근 60초 통계를 대시보드에 함께 보여준다(빠른 길). Gold와 로직을 공유하지 않는다 |

## 2. 요구사항

1. 포트폴리오 데모용으로 라이브 대시보드에서 방송 흐름을 보여준다.
2. 고유 참여자 수, 가장 긴 수집 공백, 피크 분, 데이터가 0건인 회차(`NO_DATA`)를 정확하게 보여준다.
3. 늦게 도착한 이벤트를 버리지 않는다([Silver 계약](../silver-data-contract.md) 2.1절).
4. Silver가 행을 지우면 Gold도 따라가야 한다.
5. 데이터가 쌓여도 갱신 한 번의 비용이 커지지 않아야 한다.
6. 같은 지표 계산 로직을 두 벌 만들지 않는다.

## 3. 현재 구조에서 출발한 사실

- Bronze와 Silver는 이미 체크포인트 기반 스트리밍이다. 새로 들어온 행만 처리한다.
- Silver는 MERGE로 쓴다. 기존 정상 행을 지우는 경로는 후원·선물의 **업무 키 충돌 하나뿐**이다(같은 후원 ID에 금액이나 시각이 다름). 2026-10-14 기준 후원 710건 중 0건이다. 같은 Kafka 좌표에 다른 내용이 오는 경우는 Silver가 쓰기 전에 Bronze 무결성 검사에서 배치를 멈추므로 삭제까지 가지 않는다.
- 방송 회차의 시작·종료는 Silver가 아니라 Collector SQLite(`monitoring_sessions`)에 있고, 진행 중인 회차는 나중에 종료 시각이 채워진다.

## 4. 검토한 선택지

### 4.1 전체 재계산 배치 (첫 구현)

Silver 전체와 SQLite를 읽어 Gold 두 테이블을 매번 덮어쓴다.

- 장점: 가장 단순하고, 삭제·늦은 이벤트·회차 종료를 항상 정확히 반영한다.
- 단점: 비용이 Silver 누적량에 비례한다. 측정치는 18만 건에 약 12초다. Silver 스트림이 멈춘 뒤 실행하는 배치라 라이브가 아니다.
- 판단: 주 경로로는 쓰지 않고 `--full`로 남긴다. dbt도 증분 모델은 느려졌을 때 도입하고 전체 재계산을 기준값으로 두라고 권장한다.

### 4.2 Spark 스트리밍 집계 + 워터마크

Silver를 `readStream`으로 읽고 `groupBy(window)`로 누적 집계한다. 처음 떠올린 방식이다.

직접 실행한 결과(Spark 4.0.1):

| 연산 | 결과 |
|---|---|
| 분당 `count`, `sum` | 됨 |
| 분당 `countDistinct` (고유 참여자) | 실패: `Distinct aggregations are not supported on streaming DataFrames/Datasets` |
| 분당 `approx_count_distinct` | 됨. 근사치 |
| `dropDuplicatesWithinWatermark(분, 사용자)` 후 `count` | 됨. 정확한 값. 다만 연산을 이어 붙이면 늦은 데이터 판정이 한 배치 늦게 적용되어 10분 넘게 늦은 사용자가 포함됐다(기대 3명, 결과 4명) |
| `lag` (공백 계산) | 실패: `NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING` |

- 연산을 이어 붙인 정확한 집계는 append 모드만 허용된다. 그래서 결과가 **워터마크만큼 늦게** 화면에 나온다. 워터마크 10분이면 22:40의 값은 이벤트 시각이 22:51을 넘어야 나온다. 워터마크를 줄이면 그보다 늦은 데이터를 버린다.
- 이벤트가 없으면 결과 행이 생기지 않아 `NO_DATA`를 만들 수 없다. 피크 분, 회차 요약도 만들 수 없다.
- Silver가 행을 지우면 Delta 스트리밍 원본은 기본 동작으로 오류를 낸다. `skipChangeCommits`로 건너뛰면 이미 더한 값을 뺄 수 없다.
- 판단: 기각. 처음에는 삭제 문제를 가장 큰 이유로 들었지만, 삭제는 관측되지 않은 경우다. 결정적인 이유는 **우리 지표 대부분을 스트리밍 집계로 계산할 수 없고, 정확한 사람 수는 워터마크만큼 늦어진다는 점**이다.

### 4.3 Continuous Processing

`trigger(continuous=...)`로 ms 단위 지연을 노렸다.

| 시험 | 결과 |
|---|---|
| 필터 후 콘솔 출력 | 됨 |
| Delta에 쓰기 (Bronze) | 실패 |
| Delta에서 읽기 (Silver 입력) | 실패: `Continuous processing does not support StreamingRelation operations` |
| `foreachBatch` (Silver 쓰기) | 실패: `'foreachBatch' is not supported with continuous trigger` |
| `groupBy` 집계 (Gold) | 실패: `Continuous processing does not support Aggregate operations` |

- Spark 문서상 실험 기능이고, Kafka와 rate 입력, Kafka와 메모리·콘솔 출력, 행 단위 변환만 지원한다. 실패한 작업을 자동 재시도하지 않는다.
- 판단: 기각. Bronze, Silver, Gold가 모두 Delta라 어느 단계에도 쓸 수 없다.

### 4.4 Databricks Real-Time Mode

문서상 수백 ms 지연을 낸다. Databricks 런타임 전용이고 Public Preview다. 오픈소스 Spark로 로컬에서 실행하는 이 프로젝트에서는 쓸 수 없어 기각했다.

### 4.5 바뀐 회차만 다시 계산하는 스트림 (채택)

Silver를 스트리밍으로 읽되, 누적 집계를 하지 않고 "새 이벤트가 왔다"를 "이 회차를 다시 계산하라"는 신호로 쓴다. 다시 계산은 일반 배치 쿼리라 모든 함수를 쓸 수 있다.

- 정확도: 전체 재계산과 같은 함수를 쓰므로 고유 참여자, 공백, 피크, `NO_DATA`가 모두 정확하다. 늦은 이벤트는 해당 회차를 다시 계산해 반영한다.
- 비용: 바뀐 회차 크기에 비례한다. 끝난 방송은 다시 건드리지 않으므로 누적량이 늘어도 한 번의 비용이 일정하다. 채팅 8.8만 건짜리 회차 하나를 다시 계산하는 데 약 1.9초다.
- 지연: 트리거 간격만큼이다. 진행 중인 분의 값도 바로 보인다.
- 로직: 전체 재계산과 증분 갱신이 같은 함수를 쓰므로 두 벌이 아니다.

구현 중 바뀐 점:

- 처음에는 다시 읽을 범위를 `(채널, 시작, 끝)` 조건 수십 개의 OR 식으로 만들었다. 실제 데이터 첫 배치(회차 4개 + 회차 밖 분 47개)에서 Spark 생성 코드가 너무 길어져 컴파일에 실패했고, 해석 모드로 우회해 결과는 맞았지만 느렸다. 회차 밖 분은 채널별 범위 하나로 묶고, 읽을 범위는 작은 브로드캐스트 테이블과 조인해 고르도록 바꿨다. 이후 같은 실행에서 생성 코드 오류는 0건이다.
- PR 리뷰에서 회차 종료 시각이 줄어드는 경우를 점검하다 버그를 찾았다. 진행 중이던 회차에 들어간 이벤트보다 앞선 시각으로 종료가 확정되면 그 이벤트가 Gold에서 사라졌고, 반대로 회차 밖 행이었던 이벤트를 나중에 생긴 회차가 덮으면 두 번 집계될 수 있었다. 다시 계산하는 회차의 구간과 예전 첫·마지막 이벤트까지의 회차 밖 행도 함께 다시 쓰도록 고쳤고, 두 경우를 `check_gold.py`에 넣었다.
- Silver에 새 행이 없으면 마이크로배치가 돌지 않는다. 그러면 방송이 끝났거나 수집이 끊긴 회차가 갱신되지 않는다. 특히 수집이 끊긴 상황(`NO_DATA`, `CHECK_GAP`)이 정작 화면에 안 보이게 된다. 그래서 드라이버 타이머가 트리거 간격마다 새 이벤트 없이 갱신을 실행한다.

### 4.6 Silver 삭제 반영: CDF vs `skipChangeCommits` + `--full`

- CDF(Change Data Feed)는 Delta 테이블의 추가·삭제·수정 기록을 읽게 해 주는 기능이다. 삭제까지 자동으로 따라갈 수 있다. 오픈소스 Delta에서는 테이블마다 `delta.enableChangeDataFeed = true`를 켜야 하고, 켠 뒤의 변경만 기록된다.
- 삭제 경로는 업무 키 충돌 하나뿐이고 관측된 적이 없다. 관측되지 않은 경우를 위해 Silver 테이블 설정을 바꾸기보다, 이미 있는 전체 재계산으로 보정하는 쪽이 단순하다.
- 진행 중인 회차는 트리거마다 다시 계산되므로 그 회차 안의 삭제는 자동으로 반영된다. 남는 건 이미 끝난 회차의 삭제뿐이다.
- 판단: `skipChangeCommits` + `--full`을 채택했다. 삭제가 실제로 자주 생기면 CDF로 바꾼다. 읽는 옵션과 테이블 설정만 달라지고 재계산 로직은 같다.

### 4.7 Gold를 Silver 배치 끝에 붙이기 vs 별도 스트림

- Silver `foreachBatch` 끝에서 Gold를 갱신하면, Silver가 자기가 넣고 지운 행을 정확히 알아 CDF 없이 삭제까지 반영되고 지연도 한 단계 줄어든다. 대신 Gold 실패가 Silver 배치를 다시 돌리게 만든다.
- 별도 스트림은 층마다 따로 재시작하고 장애를 격리할 수 있다. 대신 Silver 여섯 테이블이 원자적으로 커밋되지 않아 한 트리거 동안 채팅과 후원이 한 배치 어긋나 보일 수 있다.
- 판단: 메달리온 층 분리를 유지하기 위해 별도 스트림을 택했다. 어긋남은 다음 트리거에서 맞춰진다.

### 4.8 Lambda vs Kappa

- 정통 Lambda는 배치 레이어와 스피드 레이어가 같은 지표를 따로 계산하고, 최근 구간을 스피드 값으로 보여주다가 배치 값으로 교체한다. 같은 로직을 두 번 구현하고 맞춰야 한다(Jay Kreps의 비판).
- 이 구조의 Bronze → Silver는 스트리밍 경로 하나에, 재처리가 필요하면 원본을 다시 흘리는 방식이라 Kappa에 가깝다. 파서를 v3.2로 바꿀 때 Bronze 전체를 다시 흘려 Silver를 재구축한 것이 그 예다.
- 판단: Kappa에 가까운 메달리온을 유지하고, 초 단위가 필요한 "지금 채팅 속도"만 Collector의 기존 통계를 그대로 보여준다. 새 계산 로직이 없으므로 Lambda의 중복 비용이 생기지 않는다.

## 5. 트리거 간격

배치 하나의 처리 시간을 실제 크기의 데이터로 쟀다(3회 중앙값).

| 단계 | 배치 크기 | 처리 시간 |
|---|---|---|
| Bronze (Delta append) | 60~300 프레임 | 0.3~0.6초 |
| Silver (정제·중복 판정·MERGE) | 10 / 60 / 300 프레임 | 3.8 / 3.3 / 3.3초 + Bronze 무결성 검사 0.26초 |
| Gold (회차 하나 재계산) | 채팅 8.8만 건 회차 | 1.9초 |

- Silver는 배치 크기와 거의 무관하게 약 3.5초다. 테이블 6개를 읽고 MERGE하는 고정 비용이 대부분이다.
- 간격을 처리 시간보다 짧게 잡으면 Spark는 쉬지 않고 다음 배치를 돌리므로 실제 간격은 처리 시간이 된다.

| 설정 (Bronze / Silver / Gold) | 평균 지연 | 최악 |
|---|---|---|
| 이전: 30 / 30 / 30초 | 약 1분 | 약 1분 40초 |
| **채택: 5 / 10 / 10초** | **약 20초** | 약 30초 |
| 공격적: 2 / 5 / 5초 | 약 14초 | 약 20초 |
| 한계 (연속 실행) | 약 10초 | — |

지연은 측정한 처리 시간에 평균 대기(간격의 절반)를 더한 계산값이다. 파이프라인 전체를 연결해 잰 값은 아니다.

5초 아래로 줄이지 않은 이유:

- 배치마다 Delta 파일과 로그 커밋이 생긴다. Silver 5초면 테이블마다 하루 약 1만 7천 커밋이다. 자동 압축은 켜져 있지만 로그 정리(VACUUM)가 필요해진다. `spark/delta_maintenance.py`는 예전 경로를 대상으로 하므로 손봐야 한다.
- Bronze, Silver, Gold가 각자 `local[*]`로 노트북 코어를 나눠 쓴다.
- Silver는 배치마다 기존 테이블 전체에서 중복 후보를 찾는다(`load_matches`). 데이터가 쌓이면 3.5초도 늘어난다.

## 6. 같은 설계 과정에서 함께 정한 것

| 주제 | 결정 | 근거 |
|---|---|---|
| 날짜 기준 | 방송 날짜는 회차 시작 시각의 KST 날짜 | 늦은 오후에 시작해 새벽에 끝나는 방송이 많다(관측 17:09–03:40). 자정으로 자르면 방송 하나가 둘로 나뉜다 |
| Gold 테이블 | 채널×1분, 방송 회차 1건 두 개. 일 단위 테이블은 두지 않음 | 차트와 요약 카드에 각각 대응. 고유 참여자는 회차 간 합산할 수 없다 |
| 회차 경계 | 이벤트 시각으로 회차를 먼저 정한 뒤 분으로 묶음 | 회차가 분 중간(19:58:30)에 시작하면 분 시작 시각 기준으로는 첫 분이 회차 밖이 된다 |
| 수집 상태 | 이벤트 0건이면 `NO_DATA`, 가장 긴 공백 10분 이상이면 `CHECK_GAP` | 조용한 방송과 수집 누락을 구분. 실제 데이터에서 세 채널 동시 66분 공백(01:01–02:07)과 Bronze 중단 이후 회차 4개를 잡아냈다. 10분은 근거 없는 초기값이고, 여러 채널 동시 공백 판정은 미룸 |
| 고유 후원자 | 지표에서 제외 | 후원 약 85%가 익명 |
| 후원 금액 이름 | `donation_amount_currency` | 처음에는 `pay_type=CURRENCY`의 단위가 확인되지 않아 원화로 표기하지 않았다. 이후 운영자가 원 단위임을 확인해 화면에는 원으로 표시한다. 컬럼 이름은 원본 `pay_type`을 드러내도록 그대로 둔다 |
| 대시보드 위치 | Collector와 분리된 FastAPI 프로세스 | Collector 앱은 시작할 때 Kafka 발행기를 띄우고 활성 채널 수집을 복구한다. 화면만 보려고 Kafka와 수집을 켤 수 없다 |
| 대시보드 데이터 | `deltalake`로 Gold Delta를 직접 읽음 | JSON 복사본은 의존성이 없지만 저장이 두 군데가 된다. 대시보드 전용 requirements에만 넣어 Collector 이미지는 그대로 둔다. Streamlit은 기존 화면과 스타일이 달라 택하지 않음 |
| Silver 재구축 | 파서 `silver-v3.2`로 기본 경로에 다시 만들고 v3.1 출력은 `silver_v3_1`로 보관 | 처음 받는 사람 기준으로는 기본 설정이 맞으므로 레포 설정은 바꾸지 않고 로컬 폴더만 정리 |
| Bronze 무결성 검사 | 유지하고 배치 오프셋 범위로 읽을 파일을 좁힘 | 토픽을 다시 만들고 Bronze 체크포인트만 지우면 같은 오프셋에 다른 메시지가 생긴다. 일어난 적은 없지만 피해가 조용히 생기고, 범위로 좁히면 읽는 파일이 10개에서 3개로 준다. [설명 페이지](../explainers/bronze-integrity-check.html) |

## 7. 검증

- `tests/test_gold.py`: 회차 읽기, 갱신 대상 판정(진행 중·새 회차·기록 변경만), `replaceWhere` 조건, 간격 파싱과 층별 기본값, 경로 겹침 거부
- `tests/check_gold.py`(실제 Spark/Delta):
  - 회차 경계, 지표 정의, 수집 상태
  - 스트림 첫 배치와 이후 배치(늦은 이벤트, 다른 채널, 회차 밖 이벤트)의 결과가 전체 재계산과 같음
  - 새 이벤트 없이 회차가 끝나도 타이머 갱신으로 반영됨
  - Silver 삭제 커밋에서 스트림이 멈추지 않고, `--full`이 보정함
- 버그 주입: 회차 밖 분 처리를 빼면 위 검사가 "증분 결과가 전체 재계산과 다름"으로 실패한다
- 실제 데이터: 스트림이 만든 Gold와 전체 재계산 결과가 같다(분 테이블 1,574행, 회차 8행, 차이 0행)

## 8. 다시 검토할 조건

| 신호 | 다음 조치 |
|---|---|
| Silver 로그 `removed`가 자주 0보다 큼 | CDF로 삭제를 자동 반영 |
| Silver 배치 처리 시간이 트리거 간격에 근접 | `load_matches`를 배치 오프셋 범위로 좁히기 |
| 대시보드에 10초 미만 갱신이 필요 | 빠른 길(Collector 통계)의 범위를 넓히기. 메달리온 간격은 위 비용을 먼저 해결한 뒤 줄이기 |
| 동시에 진행 중인 회차가 많아 트리거마다 재계산이 길어짐 | 진행 중인 회차는 마지막 계산 이후 이벤트만 반영하도록 범위 좁히기 |
| Delta 로그와 작은 파일이 늘어 조회가 느려짐 | `delta_maintenance.py`를 새 경로에 맞추고 주기 실행 |
| `CHECK_GAP`이 조용한 방송에서 자주 뜸 | 10분 기준 조정 또는 여러 채널 동시 공백 판정 추가 |

## 9. 참고 문서

- [What is the medallion lakehouse architecture? — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion)
- [Incremental refresh for materialized views — Azure Databricks](https://learn.microsoft.com/en-us/Azure/databricks/ldp/incremental-refresh)
- [Delta Lake table streaming reads and writes — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/delta-lake)
- [Use change data feed — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta/delta-change-data-feed), [Change data feed — Delta Lake](https://docs.delta.io/latest/delta-change-data-feed.html)
- [Structured Streaming Programming Guide — Apache Spark](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html), [Performance tips (Continuous Processing)](https://spark.apache.org/docs/latest/streaming/performance-tips.html)
- [Real-time mode — Databricks](https://docs.databricks.com/aws/en/structured-streaming/real-time/concepts)
- [Spark Declarative Pipelines — AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/spark-declarative-pipelines.html), [Medallion architecture with Iceberg materialized views — AWS Big Data Blog](https://aws.amazon.com/blogs/big-data/building-medallion-architecture-with-iceberg-materialized-views-in-amazon-sagemaker/)
- [About incremental models — dbt](https://docs.getdbt.com/docs/build/incremental-models-overview)
- [Big Data Architectures — Azure Architecture Center](https://learn.microsoft.com/en-us/azure/architecture/databases/guide/big-data-architectures), [Questioning the Lambda Architecture — O'Reilly](https://www.oreilly.com/ideas/questioning-the-lambda-architecture)
