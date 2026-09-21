# 1. 기본 이미지 선택 (Python 3.13)
FROM --platform=linux/amd64 python:3.13-slim

# 2. 컨테이너 내의 작업 디렉토리 설정
WORKDIR /app

ENV PYTHONUNBUFFERED=1

# 4. 의존성 파일 먼저 복사
COPY requirements-collector.txt .

# 5. 의존성 설치
RUN pip install --no-cache-dir -r requirements-collector.txt

# 6. 프로젝트의 모든 소스 코드(.py 파일 등)를 작업 디렉토리로 복사
COPY . .

# 7. 런타임 데이터 디렉터리 생성
RUN mkdir -p /app/data

# 8. 컨테이너가 시작될 때 실행할 기본 명령어
CMD ["python", "chzzk_collector_server.py"]
