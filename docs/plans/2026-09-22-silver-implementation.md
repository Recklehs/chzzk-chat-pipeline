# Silver 구현 계획

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task-by-task. 현재 세션에서 직접 구현한다.

**Goal:** Bronze 원본에서 6개 Silver Delta 테이블을 만들고 재처리·부분 실패·충돌 후에도 데이터 계약을 유지한다.

**Architecture:** 표준 라이브러리 JSON 파서로 정수/누락/익명 규칙을 엄격히 검증하고 Spark 배치에서 실행한다. 기존 출력과 입력의 원본·업무 키를 비교해 충돌을 격리한 뒤 정상 행을 MERGE한다. 하나의 로컬 실행 작업과 체크포인트로 운영한다.

**Tech Stack:** 기존 Python 3.12, PySpark 4.0.1, Delta Lake 4.0.1, pytest. 새 의존성 없음.

**Spec:** [Silver 데이터 계약 v3](../silver-data-contract.md)

## Global Constraints

- Bronze·Collector는 변경하지 않으며 Silver에는 원문 profile/extras/토큰을 복사하지 않는다.
- 정상 5개 테이블과 quarantine 1개. 개별 이벤트를 저장하고 집계는 하지 않는다.
- 원본 키 `(topic, partition, offset, body_index)`와 후원·선물 업무 키를 구분한다.
- 일부 테이블만 저장된 배치는 실패하며 같은 입력 재시도 결과가 수렴해야 한다.
- 단일 writer, 로컬 Delta 출력. 데이터와 체크포인트는 기존 `/data/` Git 제외 범위에 둔다.

## Review Focus

- 배열 일부 오류·명시적 잘못된 item cmd가 정상 이웃을 버리거나 프레임 cmd로 우회하지 않음.
- bool/float/overflow를 금액·시간 정수로 받아들이지 않음; KST 경계와 늦은 데이터 보존.
- 같은 원본 위치가 다른 테이블/업무 키로 바뀌어도 원본 충돌로 처리.
  - 최종 검토 반영: 원본 내용 자체가 변한 경우는 쓰기 전 Bronze 무결성 검사로 작업을 중단한다. 버린 업무 중복 좌표·빈 본문·범위 밖 cmd도 포함한다. 같은 원문에서 적재 시각만 달라진 경우는 중복이다.
- 기존 정상 후원과 새 충돌의 격리 저장 후 장애가 나도 재시작 시 정상 행 제외 완료.
- 같은 선물 ID의 다른 수신자는 유지; 같은 문장·같은 구독 개월 수를 임의 중복 제거하지 않음.

## Task 1: 파싱 및 데이터 계약

Files: `spark/silver_parser.py`, `tests/test_silver.py`.

Interface: `parse_frame(frame, processed_at) -> list[dict]`; 각 dict는 내부 목적지 `table`과 그 테이블의 정제 컬럼을 가진다. 집계용 빈 본문/범위 밖 기록은 내부 `_metric` 목적지로 반환한다.

- [x] 실제 데이터의 식별자를 복사하지 않은 입력으로 실패 검증을 먼저 실행한다.
- [x] 공통 시간·원본 키, 정수 엄격 변환, 유형별 필수값, 선택 필드 경고, 익명/프로필 불일치 처리를 구현한다.
- [x] type=11/30, extras.month와 profile 구독 차이, 94010 제외를 검증한다.

```python
rows = parse_frame(frame_with_one_bad_array_item, processed_at)
assert [(r['table'], r['body_index']) for r in rows] == [('chat_messages', 0), ('quarantine', 1), ('chat_messages', 2)]
```

Run: `.venv/bin/python -m pytest tests/test_silver.py -q`.

## Task 2: 멱등 저장 및 충돌 복구

Files: `spark/silver_store.py`, `tests/test_silver.py`, `tests/check_silver.py`.

Interfaces: `reconcile(incoming, existing, quarantine)`는 삽입/삭제/격리와 배치 통계를 반환한다. `write_batch(batch, batch_id, output_path)`는 실제 Delta를 읽고 결과를 적용한다. 테이블 스키마는 같은 모듈에서 명시한다.

- [x] 같은 원본 입력·같은 업무 이벤트의 반복, 동일 키의 상충 값, 다른 수신자와 다른 offset의 동일 채팅을 먼저 검사한다.
- [x] 실제 Bronze 이력으로 배치 좌표의 원문·채널·Kafka 시각 불변성을 확인하며 NULL도 비교에 포함한다. 원문 변조는 출력 생성 전에 실패시킨다.
- [x] 기존 행 및 배치 내 충돌을 함께 처리하고 관련 원본/업무 키를 quarantine에 보존한다. 한 항목의 여러 충돌 키는 `conflict_keys` 배열로 보존한다.
- [x] quarantine MERGE → 충돌 행 삭제 → 정상 insert-only MERGE 순서를 구현한다.
- [x] 실제 로컬 Delta에서 quarantine 저장 직후 예외를 발생시킨 뒤 같은 배치를 다시 실행해 기존 정상 행 제외와 재삽입 차단을 확인한다.

```python
write_batch(first_batch, 0, output)
write_batch(first_batch, 0, output)
assert spark.read.format('delta').load(output + '/donations').count() == 1
# 같은 후원 ID의 상충 금액 배치를 재시도한 후 정상 합계에서 제외됨을 확인한다.
```

Run: `JAVA_HOME=<JDK17> .venv/bin/python tests/check_silver.py`.

## Task 3: 실행·문서·실제 표본 검증

Files: `spark/bronze_to_silver.py`, `README.md`, `docs/architecture.md`, `docs/silver-data-contract.md`.

- [x] 기존 properties 파서와 Spark 세션 생성을 재사용한다. output/checkpoint는 Bronze 경로의 형제로 파생한다.
- [x] `--available-now` 초기 적재와 연속 실행을 제공하고 로컬 writer 잠금 및 파서 버전 혼용 검사를 추가한다.
- [x] 테스트의 로컬 Bronze 스트림에서 중단·재시작을 확인한다.
- [x] 고정 v475를 별도 검사 출력에 적용해 당일 50,055 / 195 / 1 / 9 / 3 / 0행을 대조한다.
- [x] 전체 pytest와 필요한 실제 Delta 검증을 통과한 뒤 로컬 Silver를 초기 적재하고 연속 실행한다. 운영 로그에 배치별 출력·격리·범위 밖 건수를 남긴다.
- [x] README 실행법과 아키텍처·계약의 구현 상태를 갱신하고 변경사항을 검토한다.

```sh
.venv/bin/python -m pytest -q
.venv/bin/spark-submit --properties-file spark/conf/local.properties spark/bronze_to_silver.py --properties-file spark/conf/local.properties --available-now
```

커밋/푸시는 이번 요청의 완료 조건이 아니며 작업 트리에 검토 가능한 변경을 남긴다.
