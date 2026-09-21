# Bronze Delta 전환 구현 메모

## 구현 범위
- 실시간 수집 저장소를 Bronze Delta로 전환
- `cmd` registry 기반 검증 추가
- invalid row DLQ Delta 적재 추가
- benchmark source를 Bronze Delta로 변경

## 주요 파일
- `bronze_delta_writer.py`
- `schema_registry.py`
- `schemas/cmd_registry.yml`
- `chzzk_collector_server.py`
- `benchmark_storage_spark.py`

## 테스트
- `tests/test_schema_registry.py`
- `tests/test_bronze_delta_writer.py`
- `tests/test_collector_server.py`
