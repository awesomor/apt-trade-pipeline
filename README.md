# 🏠 아파트 실거래가 데이터 파이프라인

공공데이터포털 아파트 실거래가 API를 수집하여 Oracle Cloud DB에 적재하고, dbt로 변환 후 Apache Superset으로 시각화하는 엔드-투-엔드 데이터 파이프라인 프로젝트입니다.

> 개인 맥북 UTM으로 생성한 Ubuntu 22.04 VM 위에서 Docker Compose로 전체 스택을 직접 구축했습니다.

---

## 아키텍처

```mermaid
flowchart LR
    A[공공데이터포털\n아파트 실거래가 API] -->|HTTP 수집| B[Airflow\napt_trade_initial_collect DAG]
    B -->|Raw 적재| C[(Oracle Cloud\nAutonomous DB)]
    B -->|DAG Trigger| D[Airflow\napt_trade_dbt DAG]
    D -->|dbt run| C
    C -->|조회| E[Apache Superset\n대시보드]

    subgraph Ubuntu 22.04 VM @ UTM
        subgraph Docker Compose
            B
            D
            E
            F[(PostgreSQL\nAirflow 메타 DB)]
        end
    end
```

### 데이터 흐름

```
공공데이터포털 API (국토교통부 아파트 매매 실거래가 자료)
    ↓ 시군구 코드 254개 × 월별 순회, 일 1만 건 한도로 약 2주간 초기 수집 (2006~2026년)
APT_TRADE_RAW  —  Oracle Cloud Autonomous DB (Raw 테이블)
    ↓ dbt staging  —  타입 변환, 금액 단위 정규화, 공백 제거
stg_apt_trades  (View)
    ↓ dbt marts  —  시군구 × 거래월 집계
mart_sigungu_stat  (Table)
    ↓
Apache Superset 대시보드
```

---

## 기술 스택

| 영역 | 기술 |
|---|---|
| 워크플로우 오케스트레이션 | Apache Airflow 2.8 |
| 데이터 변환 | dbt (Oracle adapter) |
| 데이터베이스 | Oracle Cloud Autonomous DB (Always Free) |
| 시각화 | Apache Superset |
| 인프라 | Docker Compose / Ubuntu 22.04 VM (UTM) |
| 언어 | Python 3.8 |

---

## 주요 구현 내용

### 1. Airflow DAG 설계 — 초기 수집과 갱신 수집 분리

초기 수집(2006~2026년 전체)과 일별 갱신 수집을 하나의 DAG 안에서 `BranchPythonOperator`로 분기하도록 설계했습니다. `IS_INITIAL_LOAD` 플래그 하나로 모드를 전환할 수 있어, 초기 수집 완료 후 DAG 구조 변경 없이 운영 모드로 전환됩니다.

```
start
  → branch_by_mode (BranchPythonOperator)
      → [초기] collect_batch        → check_progress → trigger_dbt → end
      → [갱신] renew_collect_batch  ↗
```

수집 완료 후에는 `TriggerDagRunOperator`로 dbt DAG를 자동 트리거하여 파이프라인이 연속으로 실행됩니다.

### 2. API 트래픽 제한 대응 — 상태 기반 점진적 수집

공공데이터포털 API의 일일 호출 제한(1만 회)에 맞춰, 전체 수집 작업을 중단 없이 이어갈 수 있도록 설계했습니다.

- `collection_log` 테이블로 `(시군구 코드 × 거래연월)` 조합별 수집 상태(`done` / `skip` / `fail` 등)를 관리
- 매일 자정 DAG 실행 시 미수집 조합을 이어서 처리 — 컨테이너 재시작이나 오류 발생 시에도 수집 위치가 보존됨
- 전국 254개 시군구 코드 × 약 20년치 월별 데이터를 약 2주에 걸쳐 안정적으로 초기 적재 완료

### 3. dbt 모델링 — Staging → Mart 레이어 분리

| 모델 | 구체화 방식 | 처리 내용 |
|---|---|---|
| `stg_apt_trades` | View | 금액 단위 통일(만 원→원), 날짜 컬럼 파싱, 문자열 공백 제거 |
| `mart_sigungu_stat` | Table | 시군구(`LAWD_CD`) × 거래월(`DEAL_YMD`) 기준 거래 건수·총액 집계 |

dbt DAG는 `schedule_interval=None`으로 설정하여 수집 DAG의 트리거로만 실행되며, staging → marts → test 순으로 실행됩니다.

### 4. Superset–Oracle 호환성 문제 해결

Superset은 내부적으로 `cx_Oracle`을 사용하지만, Oracle이 `cx_Oracle`을 `oracledb`로 대체한 이후 직접 연결이 불가능합니다. 이를 `superset_config.py`에서 `oracledb`를 `cx_Oracle`로 monkey-patch하는 방식으로 해결했습니다.

```python
# superset_config.py
_original_connect = oracledb.connect

def _patched_connect(*args, **kwargs):
    kwargs.setdefault('wallet_location', os.getenv("WALLET_LOCATION"))
    kwargs.setdefault('wallet_password', os.getenv("WALLET_PASSWORD"))
    return _original_connect(*args, **kwargs)

oracledb.connect = _patched_connect
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb   # Superset이 cx_Oracle로 import하도록
```

Oracle Wallet 기반 mTLS 인증 설정도 여기서 함께 처리하여, Superset 컨테이너에서 별도 설정 없이 Autonomous DB에 접속할 수 있습니다.

---

## 프로젝트 구조

```
apt-trade-pipeline/
├── dags/apt-trade/
│   ├── apt_trade_dag.py        # 메인 수집 DAG (초기/갱신 브랜치)
│   ├── apt_trade_dbt_dag.py    # dbt 실행 DAG (트리거 전용)
│   ├── collector.py            # 초기 수집 로직
│   ├── renew_collector.py      # 갱신 수집 로직
│   ├── config.py               # 환경변수 로드
│   ├── db.py                   # Oracle DB 연결
│   └── data/                   # 시군구 코드 파일 (캐싱)
├── dbt/apt_mart/
│   ├── models/
│   │   ├── staging/            # stg_apt_trades (View)
│   │   └── marts/              # mart_sigungu_stat (Table)
│   └── profiles.yml            # DB 연결 (env_var 참조)
├── docker-compose.yml          # Airflow / Superset / PostgreSQL
├── Dockerfile
├── Dockerfile.superset         # oracledb 설치 포함
├── superset_config.py          # cx_Oracle 호환 패치
├── .env.example
└── .gitignore
```

---

## 실행 방법

### 1. 환경변수 설정

```bash
cp .env.example .env
cp dags/apt-trade/.env.example dags/apt-trade/.env
```

`.env` 필수 항목:

```env
# Oracle DB
DB_USER=
DB_PASSWORD=
DB_HOST=                        # Oracle Cloud DB 호스트
DB_SERVICE=                     # Oracle Cloud 서비스명
DB_DSN=
DB_CONFIG_DIR=
DB_WALLET_LOCATION=
DB_WALLET_PASSWORD=

# Airflow
AIRFLOW_ADMIN_USER=
AIRFLOW_ADMIN_PASSWORD=
AIRFLOW_CORE_FERNET_KEY=
POSTGRES_PASSWORD=
MY_EMAIL=

# Superset
SUPERSET_SECRET_KEY=
SUPERSET_DB_POSTGRES_PASSWORD=
```

### 2. Oracle Wallet 배치

```bash
# wallet/ 디렉토리에 Oracle Wallet 파일 배치
# (tnsnames.ora, cwallet.sso, ewallet.p12 등)
```

### 3. 컨테이너 실행

```bash
docker compose up -d
```

| 서비스 | 접속 주소 |
|---|---|
| Airflow | http://localhost:8080 |
| Superset | http://localhost:8088 |

### 4. DAG 실행

Airflow UI에서 `apt_trade_initial_collect` DAG를 활성화합니다. `IS_INITIAL_LOAD` 플래그로 초기/갱신 모드를 전환할 수 있습니다.

```python
# dags/apt-trade/apt_trade_dag.py
IS_INITIAL_LOAD = True   # 초기 수집
IS_INITIAL_LOAD = False  # 갱신 수집 (초기 수집 완료 후)
```
