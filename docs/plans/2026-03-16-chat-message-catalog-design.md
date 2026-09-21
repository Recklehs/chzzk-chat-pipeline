# Chat Message Catalog Design

## Goal
- Bronze Delta snapshot 기준으로 채팅 메시지 계열 양식을 빠짐없이 확인할 수 있는 보고서를 생성한다.
- 범위는 `cmd=93101`, `cmd=93102`, 그리고 `93102` 안의 system notice 계열까지 포함한다.

## Why Existing Coarse Report Is Not Enough
- 기존 coarse 리포트는 `cmd`별 semantic group 요약에는 적합하다.
- 하지만 `93101`, `93102`의 `profile` 존재 여부와 `extras` key-set 차이를 한눈에 보는 전용 카탈로그는 아니다.
- 특히 사람이 “채팅 메시지 양식”만 집중해서 확인하려면 비채팅 cmd를 제거하고, variant 식별 기준을 채팅 payload에 맞게 재정의할 필요가 있다.

## Report Scope
- 입력: `data/bronze/events` Delta snapshot
- 포함 대상:
  - `93101` 일반 채팅
  - `93102` 후원/구독/시스템 공지형 채팅
- 제외 대상:
  - moderation (`94008`)
  - session control (`10100`)
  - 일반 system event (`93006`)

## Variant Definition
- 양식 variant는 아래 축으로 구분한다.
  - `cmd`
  - semantic group
  - `bdy.msgTypeCode`
  - `bdy.msgStatusType`
  - `profile_present`
  - `profile_keys`
  - `extras_present`
  - `extras_keys`
  - top-level / `bdy` key-set
- 깊은 내부 값 차이(`extras.emojis` 내부 실제 key들, `profile.streamingProperty` 내부 값)는 variant 분리 기준에서 제외한다.
- 이유:
  - 보고서 목적은 “양식” 확인이지 데이터 값 카디널리티 폭발이 아니다.
  - key-set 차이까지만 봐도 스키마 관점의 실질적인 분기는 대부분 설명된다.

## Outputs
- Markdown: 사람이 읽는 카탈로그
- CSV: 필터/정렬/집계용 테이블
- JSON: 전체 variant 원본

## Output Contents
- snapshot 메타데이터:
  - scan time
  - Delta version
  - row count
- summary:
  - 전체 chat rows
  - `cmd`별 rows
  - semantic group별 rows
- variant catalog:
  - variant id
  - row count / share
  - semantic group
  - discriminators
  - `profile_present`, `profile_keys`
  - `extras_present`, `extras_keys`
  - top-level keys / `bdy` keys
  - sample payload / sample location

## Implementation Shape
- 새 모듈 `chat_message_catalog.py`를 추가한다.
- Bronze Delta를 직접 읽고 채팅 계열 row만 필터링한다.
- 기존 `event_semantics.py`, `profile_json_raw_shapes.py`의 helper를 재사용해 payload expansion과 semantic 분류를 맞춘다.

## Verification
- 테스트로 다음을 검증한다.
  - 채팅 계열 cmd만 포함되는지
  - `profile_present` / `extras_keys` 차이가 variant 분리 기준에 반영되는지
  - Markdown/CSV/JSON 산출물이 기대 필드를 포함하는지
- 실제 Bronze snapshot 대상으로 리포트를 생성해 파일 존재와 헤더를 확인한다.
