# Network Log ETL Platform - Airflow 배치 처리 및 Data Mart 설계 문서

## 1. 시스템 전체 아키텍처 (System Architecture)

```text
[MySQL Raw Layer] (network_logs)
        │
        │ Extract & Transform (Airflow가 MySQL 내부 연산 오케스트레이션)
        ▼
 [Apache Airflow] (Scheduler & Worker가 1시간 주기로 집계 쿼리 실행)
        │
        │ Load & Upsert (ON DUPLICATE KEY UPDATE 문을 활용한 데이터 보정)
        ▼
[MySQL Data Mart] ── hourly_device_stats      (장비별 세부 이벤트 통계)
                  ├── threat_candidates         (보안/차단 위협 출발지 IP 식별)
                  ├── hourly_device_type_trend (인프라 제품군별 트래픽 추이)
                  └── hourly_top_talkers      (시간대별 이벤트 발생량 TOP 10 출발지 IP)
```

- **설명**: 본 배치 파이프라인 아키텍처는 데이터 가공 연산(집계, 정렬 등)을 Airflow 메모리 내부가 아닌 강력한 RDBMS인 MySQL 데이터베이스 자체 엔진의 컴퓨팅 자원을 빌려 처리하는 **ELT(Extract-Load-Transform) 패러다임**을 준수합니다.

---

## 2. 각 컴포넌트 역할 (Component Roles)

- **Apache Airflow Scheduler**:
  - 정의된 DAG(Directed Acyclic Graph)의 스케줄 설정(`@hourly`)에 따라 주기적으로 인스턴스를 실행 큐에 대기시키고 태스크의 의존성을 조율합니다.
- **Apache Airflow Worker**:
  - 스케줄러로부터 할당받은 연산 태스크를 실제로 실행합니다. Worker는 대량 데이터를 직접 처리하지 않고, SQL 실행 요청과 Task 상태 관리 중심으로 동작하여 데이터 처리 부하를 MySQL 엔진으로 위임합니다. (단, DAG 실행/SQL Operator 호출/DB Connection 관리/Task 로그 처리 등에 필요한 CPU·메모리는 Worker에서도 소모됩니다.)
- **MySQL Data Mart**:
  - 원시 로그 테이블(`network_logs`)의 방대한 데이터에서 통계, 모니터링, 위협 탐지에 유용한 주요 차원(Dimension)과 측정값(Measure)을 사전에 집계하여 영속화해 두는 별도 분석 영역입니다.
  - 대시보드 조회 시 Raw 데이터 Full Scan을 방지하고, 사전 집계된 데이터를 활용하여 Grafana, Superset 등 시각화 도구의 조회 응답 시간을 개선합니다. (실제 응답 속도는 데이터 규모·인덱스·쿼리 패턴에 따라 달라짐)

---

## 3. 데이터 흐름 및 멱등성 보장 (Data Flow & Idempotency)

- **ELT 방식의 강점**:
  - 대량의 로그 데이터를 Airflow 메모리 내부로 로드하여 처리(ETL)하는 것은 Worker 노드의 OOM(Out Of Memory)을 유발하며 불필요한 네트워크 I/O를 초래합니다.
  - 본 시스템은 Raw 데이터(`network_logs`)를 먼저 MySQL에 저장한 후, Airflow가 SQL 기반 변환 작업을 오케스트레이션하는 ELT 형태의 Batch Analytics Pipeline입니다. 대량 데이터 이동 없이 MySQL Query Engine의 집계 처리 능력을 활용하여 변환 작업을 수행함으로써 Worker 부하를 최소화합니다.
- **멱등성(Idempotency) 보장 흐름**:
  - 배치 파이프라인에서 멱등성은 **"어떤 이유로든 동일한 배치를 여러 번 재실행해도 적재되는 최종 데이터가 한 번만 성공했을 때와 정확히 일치하는 설계"**를 뜻합니다.
  - 이를 위해 쿼리의 시간 필터 범위 조건으로 `NOW()` 같은 동적 함수를 사용하지 않고, Airflow Jinja 템플릿 변수인 `{{ data_interval_start }}`와 `{{ data_interval_end }}`를 명시적으로 전달합니다.
  - 대상 테이블에 데이터가 이미 적재되어 있을 때 중복 행 에러를 방지하고 최신 정보로 갱신하기 위해 `ON DUPLICATE KEY UPDATE` 패턴(Upsert)을 사용하여 멱등성을 하드웨어/스키마 레벨에서 보장합니다.

> **💡 설계 팁: `event_hour`의 정체 및 버킷 가공 정책**
> - 본 파이프라인에서 생성되는 `event_hour` 값은 개별 원시 로그가 수집된 실제 초 단위 시각이 아닙니다.
> - 대신 **"배치 실행 구간의 시작 기준 시각(시간 버킷)"**을 의미하며, 예를 들어 `13:00:00`부터 `13:59:59` 사이에 발생한 모든 로그는 본 배치 쿼리를 거치며 모두 `13:00:00`이라는 고정된 시간 버킷 단위로 그루핑 및 삽입됩니다. 시간 단위 버킷팅(Time Bucketing)을 적용하여 동일 시간대 데이터를 하나의 집계 단위로 관리함으로써 대규모 트렌드 분석 조회 성능을 가속합니다.

---

## 4. Data Mart 테이블 설계 (Data Mart Schema Design)

RDBMS의 무결성 제약 조건(Primary Key 및 Unique Index)을 이용해 중복 입력을 사전에 원천 차단하고, 효율적인 집계 처리를 진행하기 위한 테이블 DDL 설계입니다.

- **설명**: 본 설계 명세에 따른 데이터 마트 전용 DDL 스키마는 프로젝트 내의 **`sql/02_data_mart_tables.sql`** 파일에 독립적으로 물리 구조화되어 버전 관리됩니다.

### 4.1. 시간별 장비 이벤트 통계 (`hourly_device_stats`)
- **목적**: 장비별, 시간별, 이벤트 유형별 발생 건수를 저장합니다. 이 테이블을 기반으로 일별/월별 통계를 가볍게 SUM 연산하여 조회합니다.
- **PK(복합키)**: `(event_hour, device_name, event_type)` 복합 키를 PK로 설정하여 중복 적재를 차단하고 Upsert 대상 기준으로 작동시킵니다.

```sql
CREATE TABLE IF NOT EXISTS hourly_device_stats (
    event_hour DATETIME NOT NULL,
    device_name VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_count BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (event_hour, device_name, event_type),
    INDEX idx_event_hour (event_hour),
    INDEX idx_device_name (device_name)
);
```

### 4.2. 의심 IP 후보 목록 (`threat_candidates`)
- **목적**: 1시간 배치 주기 내에서 주요 보안 및 비정상 차단 관련 이벤트(`DROP`, `PORT_SCAN`, `DDOS_DETECTED`, `LOGIN_FAIL`)가 임계값(`HAVING COUNT(*) >= 10`) 이상 발생한 IP를 "의심 후보"로 걸러냅니다. AI 기반 탐지나 Threat Intelligence 연동이 아닌, 단순 임계값 기반 필터링임을 명확히 한다.
- **PK(복합키)**: `(event_hour, src_ip, event_type)` 복합 키를 PK로 지정하여, 어떤 시각에 어떤 위협 행동(이벤트 타입)으로 특정 IP가 적발되었는지 다각도로 상세 분석할 수 있도록 멱등성을 확보합니다.
- **임계값(`>= 10`) 산정 근거**: PoC 단계에서는 임계값을 10건으로 고정하였으며, 운영 환경 적용 시에는 장비 타입/트래픽 규모에 따라 조정 가능한 값으로 남겨둔다.

```sql
CREATE TABLE IF NOT EXISTS threat_candidates (
    event_hour DATETIME NOT NULL,
    src_ip VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    occurrence_count BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (event_hour, src_ip, event_type),
    INDEX idx_event_hour (event_hour),
    INDEX idx_src_ip (src_ip),
    INDEX idx_event_type (event_type)
);
```

> **💡 보안 이벤트 매핑 참고**: `event_type` 필터 목록은 Generator(`config.yaml`)가 실제로 발생시키는 이벤트 종류(`ALLOW`, `DROP`, `PORT_SCAN`, `DDOS_DETECTED`, `LOGIN_FAIL`, `MAC_LEARNED`, `PORT_UP`, `PORT_DOWN`)와 일치시켜야 한다. Generator에 없는 이벤트 타입을 필터 조건에 넣으면 해당 조건은 영구적으로 0건만 반환되므로, 새 이벤트 타입을 필터에 추가하려면 Generator 설정에 먼저 반영한다.

### 4.3. 인프라 제품군별 시간당 트래픽 추이 (`hourly_device_type_trend`)
- **목적**: 개별 장비 단위보다 더 넓은 추상화 범위인 '장비 타입(Firewall, Switch, Server)' 단위별 통합 트래픽 추세를 분석하여 장기 리소스 산정 및 이상 증폭 흐름을 모니터링합니다.
- **PK(복합키)**: `(event_hour, device_type)` 복합 키를 PK로 설정합니다.

```sql
CREATE TABLE IF NOT EXISTS hourly_device_type_trend (
    event_hour DATETIME NOT NULL,
    device_type VARCHAR(50) NOT NULL, -- 'firewall', 'switch', 'server'
    total_events BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (event_hour, device_type),
    INDEX idx_event_hour (event_hour)
);
```

> **📐 아키텍처 확장성 및 정규화 Trade-off**:
> - 현 구조에서는 `device_name` 패턴 매칭(`LIKE 'fw-%'`) 기반의 실시간 가공 연산(`CASE WHEN`)을 통해 `device_type`을 파생시킵니다. 이 방식은 Raw 스키마를 고도로 단순하게 유지하는 장점이 있습니다.
> - 그러나 장비 수가 기하급수적으로 늘어나거나 네이밍 패턴 규칙이 다양해질 경우, `network_logs` (Raw) 테이블 자체에 수집 시점부터 `device_type` 컬럼을 명시적으로 포함하거나, 별도의 공통 `device_lookup` 마스터 테이블을 두어 `JOIN` 연산으로 해결하는 것이 데이터베이스 부하 분산 및 정규화 관점에서 더욱 우수합니다. Generator가 이미 장비 생성 시점에 타입을 알고 있으므로, 후속 개선 시 Raw 테이블에 `device_type` 컬럼을 직접 적재하는 방향을 우선 검토한다.

### 4.4. 시간별 Top Talkers (`hourly_top_talkers`)
- **목적**: 시간 버킷별로 이벤트 발생량이 가장 많은 출발지 IP 상위 10개를 랭킹으로 보관합니다. `threat_candidates`가 특정 위협 이벤트 유형 기준의 탐지라면, 이 테이블은 이벤트 종류와 무관하게 트래픽 총량 기준 상위 IP를 보여줍니다.
- **정확히 10개만 필요**하므로 동점 시 같은 순위를 부여하는 `RANK()`가 아닌, 동점이어도 순위를 유일하게 부여하는 `ROW_NUMBER()`를 사용합니다 (`RANK()`는 동점이 여러 건이면 `WHERE rank_no <= 10` 조건에서 10개를 초과하는 행이 나올 수 있음). 또한 `COUNT(*)`가 동률일 경우 정렬 순서가 보장되지 않으므로 `src_ip`를 2차 정렬 기준으로 추가하여, 재실행해도 동일한 결과가 나오도록(멱등성) 합니다.
- **PK(복합키)**: `(event_hour, src_ip)` 복합 키로 설정합니다.

```sql
CREATE TABLE IF NOT EXISTS hourly_top_talkers (
    event_hour DATETIME NOT NULL,
    src_ip VARCHAR(50) NOT NULL,
    event_count BIGINT DEFAULT 0,
    rank_no INT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (event_hour, src_ip),
    INDEX idx_event_hour (event_hour)
);
```

---

## 5. Airflow DAG 상세 설계 (Airflow DAG Specification)

Airflow 2.7+ 버전에서 권장되는 `SQLExecuteQueryOperator`와 현대적 DAG 정의 방식을 준용합니다.

### 5.1. Task 흐름도
```text
                        [start_task] (EmptyOperator)
                               │
     ┌──────────────┬──────────────┬──────────────┐
     ▼              ▼              ▼              ▼
[aggregate_stats] [detect_threat_candidates] [analyze_device_trends] [rank_top_talkers]
(SQLExecuteQuery) (SQLExecuteQuery)     (SQLExecuteQuery)       (SQLExecuteQuery)
     └──────────────┴──────────────┴──────────────┘
                               ▼
                        [end_task] (EmptyOperator)
```

- **Schedule**: `@hourly` (매 시간 실행)

### 5.2. DAG 파이썬 코드 예시 (`airflow/dags/network_log_batch_analytics.py`)
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,  # 지수 백오프 적용 (5분, 10분, 20분 간격 재시도)
    'max_retry_delay': timedelta(minutes=30),
}

with DAG(
    dag_id='network_log_batch_analytics',
    default_args=default_args,
    description='Network logs batch aggregation, threat detection, device trends, and top talkers',
    schedule='@hourly',  # Airflow 2.7+ 부터는 schedule_interval 대신 schedule 사용 권장
    catchup=False,
    max_active_runs=1,
) as dag:

    start_task = EmptyOperator(task_id='start')

    # 1. 시간별 장비 이벤트 집계 (ELT)
    aggregate_stats = SQLExecuteQueryOperator(
        task_id='aggregate_stats',
        conn_id='mysql_conn',
        sql="""
            INSERT INTO hourly_device_stats (event_hour, device_name, event_type, event_count)
            SELECT 
                DATE_FORMAT('{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}', '%Y-%m-%d %H:00:00') AS event_hour,
                device_name,
                event_type,
                COUNT(*) AS event_count
            FROM network_logs
            WHERE event_time >= '{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}'
              AND event_time < '{{ data_interval_end.strftime("%Y-%m-%d %H:%M:%S") }}'
            GROUP BY device_name, event_type
            ON DUPLICATE KEY UPDATE
                event_count = VALUES(event_count);
        """,
    )

    # 2. 비정상 보안/위협 IP 탐지 (1시간 내 주요 차단 이벤트가 10회 이상 발생한 대상)
    detect_threat_candidates = SQLExecuteQueryOperator(
        task_id='detect_threat_candidates',
        conn_id='mysql_conn',
        sql="""
            INSERT INTO threat_candidates (event_hour, src_ip, event_type, occurrence_count)
            SELECT 
                DATE_FORMAT('{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}', '%Y-%m-%d %H:00:00') AS event_hour,
                src_ip,
                event_type,
                COUNT(*) AS occurrence_count
            FROM network_logs
            WHERE event_time >= '{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}'
              AND event_time < '{{ data_interval_end.strftime("%Y-%m-%d %H:%M:%S") }}'
              AND event_type IN ('DROP', 'PORT_SCAN', 'DDOS_DETECTED', 'LOGIN_FAIL')
            GROUP BY src_ip, event_type
            HAVING occurrence_count >= 10
            ON DUPLICATE KEY UPDATE
                occurrence_count = VALUES(occurrence_count);
        """,
    )

    # 3. 장비 인프라 제품군별 트래픽 이벤트 추이 분석
    analyze_device_trends = SQLExecuteQueryOperator(
        task_id='analyze_device_trends',
        conn_id='mysql_conn',
        sql="""
            INSERT INTO hourly_device_type_trend (event_hour, device_type, total_events)
            SELECT 
                DATE_FORMAT('{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}', '%Y-%m-%d %H:00:00') AS event_hour,
                CASE 
                    WHEN device_name LIKE 'fw-%' THEN 'firewall'
                    WHEN device_name LIKE 'sw-%' THEN 'switch'
                    WHEN device_name LIKE 'server-%' THEN 'server'
                    ELSE 'unknown'
                END AS device_type,
                COUNT(*) AS total_events
            FROM network_logs
            WHERE event_time >= '{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}'
              AND event_time < '{{ data_interval_end.strftime("%Y-%m-%d %H:%M:%S") }}'
            GROUP BY device_type
            ON DUPLICATE KEY UPDATE
                total_events = VALUES(total_events);
        """,
    )

    # 4. 시간대별 이벤트 발생량 TOP 10 출발지 IP 랭킹
    rank_top_talkers = SQLExecuteQueryOperator(
        task_id='rank_top_talkers',
        conn_id='mysql_conn',
        sql="""
            INSERT INTO hourly_top_talkers (event_hour, src_ip, event_count, rank_no)
            SELECT
                event_hour,
                src_ip,
                event_count,
                rank_no
            FROM (
                SELECT
                    DATE_FORMAT('{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}', '%Y-%m-%d %H:00:00') AS event_hour,
                    src_ip,
                    COUNT(*) AS event_count,
                    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, src_ip ASC) AS rank_no
                FROM network_logs
                WHERE event_time >= '{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}'
                  AND event_time < '{{ data_interval_end.strftime("%Y-%m-%d %H:%M:%S") }}'
                GROUP BY src_ip
            ) ranked
            WHERE rank_no <= 10
            ON DUPLICATE KEY UPDATE
                event_count = VALUES(event_count),
                rank_no = VALUES(rank_no);
        """,
    )

    end_task = EmptyOperator(task_id='end')

    start_task >> [aggregate_stats, detect_threat_candidates, analyze_device_trends, rank_top_talkers] >> end_task
```

---

## 6. 예외 처리 및 안정성 고도화 (Exception Handling & Reliability)

- **지수 백오프 Retry 전략**:
  - 대용량 DB 적재 및 트래픽 폭증 시, 커넥션 풀 부족이나 일시적인 네트워크 지연으로 DB 쓰기 쿼리가 실패할 수 있습니다.
  - 이를 방지하기 위해 기본 `retries` 설정을 `3`회로 두고, `'retry_exponential_backoff': True`를 명시적으로 설정합니다.
  - 지수 백오프 알고리즘을 사용함으로써 재시도 간격이 점진적으로 증가(`5분 -> 10분 -> 20분`)하여 일시적인 대상 데이터베이스의 부하 부담을 덜어내고, 일시적인 네트워크 순단 장애 상황에서 자동 재시도를 수행하여 배치 성공 가능성을 높입니다.
- **ON DUPLICATE KEY UPDATE를 활용한 데이터 일관성 획득**:
  - 배치 장애로 특정 시점의 데이터에 유실 의심이 생겨 해당 시간 영역을 강제로 **다시 실행(Clear/Backfill)**하더라도, 마트 테이블은 이미 데이터가 적재되어 있다면 SQL 엔진의 PK 수준에서 판단하여 누적 카운트를 무지성으로 더하는 대신 해당 시점의 정확한 계산값으로 덮어씁니다(`VALUES(event_count)`).
  - 이는 파이프라인 정지나 중복 적재 오동작으로 인한 데이터 오염 및 데이터 왜곡을 원천 예방합니다.
- **`VALUES()` 함수 Deprecation 관련 참고**:
  - MySQL 8.0.20부터 `INSERT ... ON DUPLICATE KEY UPDATE` 구문의 `VALUES()` 함수가 Deprecated 상태로 전환되었으나, 아직 제거되지 않았고 현재 버전에서도 정상 동작한다. 향후 완전한 대체 문법(레코드 별칭 방식)이 안정화되면 마이그레이션을 검토하되, 현재는 넓은 호환성을 위해 `VALUES()`를 그대로 사용한다.

---

## 7. 향후 로드맵 (Future Roadmap)

- **실시간/배치 Alerting 시스템 도입**:
  - Airflow 내에서 위협 IP 탐지(`detect_threat_candidates`) 완료 후, 특정 출발지 IP의 차단 수가 과도할 경우(예: 1시간 100회 이상) `SlackWebhookOperator`를 연동해 실시간 경고 메시지를 보안관제 채널로 발송하도록 고도화합니다.
- **데이터 무결성 검증 (Data Quality Validation)**:
  - `Great Expectations` 혹은 `Soda SQL`과 같은 오픈소스 데이터 품질 라이브러리를 Airflow DAG 내에 임베딩하여, 마트 테이블 적재 전/후로 "Null 데이터 검증", "카운트 일치성 체크", "데이터 타입 체크" 등의 수치 검증 단계를 추가하여 이상 데이터 발생 시 후속 가공을 차단합니다.
- **Raw 테이블 `device_type` 컬럼화**:
  - 현재는 `hourly_device_type_trend` 집계 시점에 `device_name` 패턴 매칭으로 `device_type`을 파생시키지만, Generator가 이미 장비 타입을 알고 있으므로 Raw 테이블(`network_logs`) 수집 시점부터 `device_type` 컬럼을 직접 적재하는 구조로 개선한다.
- **BI 대시보드 연동 최적화**:
  - Apache Superset 또는 Grafana를 Data Mart 영역(`hourly_device_stats`, `threat_candidates`, `hourly_device_type_trend`, `hourly_top_talkers`)에 직접 질의 연결하여, Raw 데이터 조회에 따른 CPU 병목 없이 장비 통계 추이 대시보드와 위협 탐지 토폴로지 맵을 시각화합니다.