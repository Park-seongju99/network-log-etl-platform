# Network Log ETL Platform - 데이터 흐름 및 NiFi 설계 문서

## 0. Current Status & Roadmap (현재 진행 상황 및 로드맵)

### Completed (현재 완료 단계)
- [x] **Log Generation**: 파이썬 기반의 가짜 네트워크 로그 생성기(`generator/src/main.py`) 개발 완료.
- [x] **Ingestion Verification**: NiFi의 `ListenTCP` 프로세서를 사용해 TCP 포트(7070)를 열고, 전송되는 Raw 텍스트 로그를 정상 수신하여 파일(`PutFile`)로 생성 검증 완료.

### Next Steps (차기 개발 단계)
- [ ] **Log Parsing & Schema Application**: 정규표현식(`ExtractText`)을 적용해 수신된 로그의 항목별 속성 추출.
- [ ] **JSON Conversion**: 추출한 FlowFile 속성들을 깔끔한 JSON 형태의 본문으로 직렬화(`AttributesToJSON`).
- [ ] **RDBMS Load**: JDBC 연동 및 DBCP Connection Pool 서비스를 활용하여 수집 데이터를 MySQL에 실시간 고속 적재(`PutDatabaseRecord`).
- [ ] **Batch Analytics DAG**: MySQL의 Raw 데이터를 에어플로우를 통해 집계/분석하여 Data Mart 적재 처리 DAG 개발.

---

## 1. 시스템 전체 아키텍처 (System Architecture)

```text
[Python Generator]
        |
        | TCP (Port 7070)
        ↓
   [Apache NiFi]
        |
        | JDBC (Port 3306)
        ↓
 [MySQL Raw Layer] <--- (원천 로그 저장 영역)
        |
        | Airflow DAG (배치 주기 처리)
        ↓
 [MySQL Data Mart] <--- (분석 및 지표 영역)
```
- **환경**: 모든 컴포넌트는 Docker Compose 기반의 `network-platform` 브릿지 네트워크 내에서 유기적으로 실행됩니다.

## 2. 각 컴포넌트 역할 (Component Roles)
- **Python Generator**: 가상의 네트워크 장비 이벤트 로그(스위치, 방화벽, 서버 등)를 지정된 초당 이벤트 수에 맞춰 생성하고, TCP 소켓 스트림으로 실시간 전송합니다.
- **Apache NiFi**: 외부 TCP 연결을 수신하고, 수신 데이터를 FlowFile로 생성한 뒤, 정규표현식을 통해 파싱하고 속성을 추출한 뒤 고성능 JSON 변환을 거쳐 MySQL 데이터베이스에 밀어 넣습니다.
- **MySQL (Raw Layer & Data Mart)**: 
  - **Raw Layer 영역**: NiFi가 파싱한 네트워크 로그 원본을 가감 없이 신속하게 적재하는 원천 관계형 데이터베이스 저장 영역입니다.
  - **Data Mart 영역**: Airflow 배치 처리를 통과한 정제 및 집계 통계 데이터를 소유하여 최종 대시보드나 리포트 등의 소스로 활용합니다.
- **Apache Airflow**: MySQL Raw Layer에 적재된 다량의 데이터를 주기적(예: 시간/일 단위)으로 집계 및 비정상 탐지 분석을 처리하여 Data Mart 영역으로 전송하는 파이프라인 관리 도구입니다.

## 3. 데이터 흐름 (Data Flow)
1. **Log Generation**: `generator/src/main.py`가 소켓을 통해 무작위 네트워크 로그를 생성 및 전송.
2. **Ingestion**: NiFi `ListenTCP` 프로세서가 7070 포트를 열어두고 TCP 연결을 수신하고, 수신된 데이터를 FlowFile Content로 저장.
3. **Parsing**: `ExtractText` 프로세서에서 정규표현식 매칭을 통해 FlowFile의 Attribute 영역에 로그 상세 항목(시간, 장비명, 이벤트 타입 등)을 변수로 주입.
4. **Transformation**: `AttributesToJSON` 프로세서가 추출된 FlowFile Attributes들을 본문(FlowFile Content)으로 전환해 완전한 JSON 포맷 데이터 생성.
5. **Load**: NiFi `PutDatabaseRecord` 프로세서와 `JsonTreeReader` 컨트롤러 서비스를 통해 MySQL `network_logs` 테이블에 삽입.
6. **Batch Processing**: Airflow DAG가 주기적으로 트리거되어 일일 트래픽 패턴, 장비별 장애 로그, 비정상 포트 접근 등의 집계 쿼리를 실행하고 Data Mart 테이블에 적재.

---

## 4. NiFi Processor 구성 및 연결 관계 (NiFi Flow Design)

NiFi 프로세서는 Record 기반 처리 구조를 사용하여 수집(Ingestion) → 파싱(Parsing) → 변환(Transformation) → 적재(Load) 과정을 수행한다.

또한 ExtractText에서 생성한 Attribute명을 MySQL 컬럼명과 동일하게 맞춰, 후속 JSON 변환 및 DB 적재 과정을 단순화하였다.

### 4.1. FlowFile Content 및 Attribute 상태 추이 다이어그램

NiFi의 데이터 가공 파이프라인 상에서 FlowFile의 메타데이터(Attribute)와 실제 물리적 바디(Content)가 변화하는 흐름은 다음과 같습니다.

```text
[ListenTCP]
 ├── FlowFile Content   : 2026-07-31 07:32:02 sw-seoul-01 MAC_LEARNED src=57.1.215.175 dst=102.121.172.160 sport=64361 dport=3306 protocol=ICMP
 └── FlowFile Attributes: { filename: "tcp-7070-uuid", uuid: "..." }
       │
       ▼
[ExtractText] (Java 정규표현식 파싱 처리)
 ├── FlowFile Content   : (수정 없음 - 원본 로그 텍스트 유지)
 └── FlowFile Attributes: 정규표현식을 통해 DB 컬럼명과 1:1 매핑되는 속성 생성
        ├── event_time  : "2026-07-31 07:32:02"
        ├── device_name : "sw-seoul-01"
        ├── event_type  : "MAC_LEARNED"
        ├── src_ip      : "57.1.215.175"
        ├── dst_ip      : "102.121.172.160"
        ├── src_port    : "64361"
        ├── dst_port    : "3306"
        └── protocol    : "ICMP"
       │
       ▼
[AttributesToJSON] (속성의 본문 직렬화 및 1:1 키 값 생성)
 ├── FlowFile Content   : (기존 평문 텍스트 본문이 DB 구조와 일치하는 JSON 포맷으로 대체됨)
 │     {
 │       "event_time": "2026-07-31 07:32:02",
 │       "device_name": "sw-seoul-01",
 │       "event_type": "MAC_LEARNED",
 │       "src_ip": "57.1.215.175",
 │       "dst_ip": "102.121.172.160",
 │       "src_port": "64361",
 │       "dst_port": "3306",
 │       "protocol": "ICMP"
 │     }
 └── FlowFile Attributes: { filename: "tcp-7070-uuid", uuid: "...", event_time: "..." }
       │
       ▼
[PutDatabaseRecord] (레코드 기반 DB 적재 엔진)
 ├── JsonTreeReader를 활용해 Content의 JSON 데이터 키값과 DB 컬럼명을 1:1로 자동 대응
 └── DBCPConnectionPool 커넥션을 사용해 MySQL raw 테이블(network_logs)로 영속화
```

### 4.2. 프로세서 목록 및 흐름도
```text
[ListenTCP] 
     │ (success)
     ▼
[ExtractText] 
     │ (matched)
     ▼
[AttributesToJSON] 
     │ (success)
     ▼
[PutDatabaseRecord] ──(success)──────────> [Success Logger (Optional)]
     │ (failure)
     ▼
[Error Queue (DLQ)] ──(LogAttribute)──> [troubleshooting/error-log (PutFile)]
```

### 4.3. 프로세서 상세 설정 (Configuration)

- **ListenTCP**
  - **Port**: `7070`
  - **Backpressure**: FlowFile Queue 한계값 제어 (대량의 로그 급증 시 수집 버퍼 보호 및 데이터 유실 유예 확보)
  - **Concurrent Tasks**: 호스트 성능 튜닝에 따른 자원 동시성 스레드 할당 조정
  - **Success Relation**: `ExtractText` 로 연결

- **ExtractText**
  - 정밀 파싱을 위해 텍스트에서 필요한 네트워크 정보를 속성으로 추출합니다. Java 정규식 엔진의 안정성과 정확한 표현 매칭을 위해 명시적인 캐릭터 클래스로 구성합니다. DB 컬럼명과 1:1 매칭하도록 속성명을 일치시킵니다.
  - **Regex Properties**:
    - `event_time`: `^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})` (MySQL 예약어를 피해 타겟 테이블 컬럼명과 완전 일치)
    - `device_name`: `^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\s+(\S+)`
    - `event_type`: `^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\s+\S+\s+(\S+)`
    - `src_ip`: `src=(\S+)`
    - `dst_ip`: `dst=(\S+)`
    - `src_port`: `sport=([0-9]+)`
    - `dst_port`: `dport=([0-9]+)`
    - `protocol`: `protocol=(\S+)`
  - **Matched Relation**: `AttributesToJSON` 으로 연결

- **AttributesToJSON**
  - FlowFile의 메타데이터(Attribute)인 파싱 결과값들을 RDBMS에 Insert하기 용이하도록 실제 JSON 바디 데이터로 전환합니다.
  - **Attributes List**: `event_time, device_name, event_type, src_ip, dst_ip, src_port, dst_port, protocol`
  - **Destination**: `flowfile-content` (본문 내용 자체를 JSON 포맷으로 대체)
  - **Null 처리**: Empty String 사용 또는 DB 기본값 우회 정의
  - **Success Relation**: `PutDatabaseRecord` 로 연결

- **PutDatabaseRecord**
  - **설명**: Record Reader를 통해 FlowFile Content의 JSON 데이터를 Record 구조로 변환하고, JDBC 기반으로 대상 테이블에 INSERT 작업을 수행합니다. Record 기반 처리 방식으로 개별 SQL 조립 로직을 제거하고 데이터 처리 구조를 정형화하여 성능 최적화를 구축합니다.
  - **Record Reader**: `JsonTreeReader` 컨트롤러 서비스 (JSON 구조 자동 해석)
  - **Database Connection Pooling Service**: `DBCPConnectionPool`
    - Database Connection URL: `jdbc:mysql://mysql:3306/${MYSQL_DATABASE}`
    - Database Driver Class Name: `com.mysql.cj.jdbc.Driver`
  - **Statement Type**: `INSERT`
  - **Table Name**: `network_logs`
  - **Success Relation**: 연결 종료 (또는 모니터링을 위한 LogAttribute 연결)
  - **Failure Relation**: `Error Queue (DLQ)` 및 장애 대응용 디렉토리 격리 연결

### 4.4. 예외 처리 및 DLQ (Dead Letter Queue) 설계
PutDatabaseRecord 프로세서 수행 도중 DB 다운타임 발생, 혹은 잘못된 형식의 가공 불가능한 로그 포맷 유입 시 데이터 유실을 완벽히 격리하기 위한 장애 극복(Failover) 흐름을 구성합니다.
- **Error Queue**: PutDatabaseRecord의 `failure` 관계는 격리용 에러 세션 큐로 이동합니다.
- **LogAttribute**: 에러 메시지의 진단 데이터를 사내 로그 수집기에 기입합니다.
- **PutFile (DLQ)**: 문제의 원본 JSON 세션을 서버 물리 스토리지 내의 `troubleshooting/error-log/` 디렉토리에 오프라인 스토리지 아카이빙 형태로 생성합니다. 이는 장애 해결 이후 수동 및 배치성 데이터 재처리(Replay) 작업의 원천 소스로 쓰입니다.

---

## 5. MySQL 저장 구조 (Table Design)

MySQL 예약어 충돌 및 데이터 모델링 무결성을 위해 `timestamp` 대신 `event_time` 컬럼명을 채택했으며, 대량 조회 및 분석에 대비해 인덱스 튜닝을 고도화했습니다.

### 5.1. Raw 데이터베이스 적재 스키마 (`sql/01_create_tables.sql`)
```sql
CREATE DATABASE IF NOT EXISTS etl_db;
USE etl_db;

CREATE TABLE IF NOT EXISTS network_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_time DATETIME NOT NULL,
    device_name VARCHAR(100),
    event_type VARCHAR(50),
    src_ip VARCHAR(50),
    dst_ip VARCHAR(50),
    src_port INT,
    dst_port INT,
    protocol VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_event_time (event_time),
    INDEX idx_device_name (device_name),
    INDEX idx_event_type (event_type),
    INDEX idx_src_ip (src_ip)
);
```
- **최적화 이유**: 
  - `event_time`: Airflow가 시간 영역 단위로 타겟 범위의 로그 데이터를 로드할 때 Full Table Scan을 방지합니다.
  - `device_name`: 특정 원격지 서버나 방화벽 장비 장애 지표를 묶어 분석하는 쿼리의 룩업 속도를 향상합니다.
  - `event_type`: `DROP`, `PROCESS_STOP` 등 특정 비정상 이벤트만 필터링하는 질의어의 오버헤드를 낮춥니다.
  - `src_ip`: 공격자 후보 식별 및 대역별 트래픽 분석 등의 쿼리에서 분산 I/O 성능을 보장합니다.

---

## 6. 에어플로우 처리 흐름 (Airflow Batch Analytics)

현재 가짜 로그 생성기가 발행하는 대표적인 보안 및 네트워크 장비 로그들(`MAC_LEARNED`, `PORT_UP`, `PORT_DOWN`, `ALLOW`, `DROP`, `PROCESS_START`, `PROCESS_STOP` 등)에 기반하여 분석 및 요약 파이프라인을 실행합니다.

### 6.1. 분석 및 집계 시나리오
1. **장비별 이벤트 발생 빈도 (Device Event Stats)**: 스위치, 방화벽, 서버 등 각 인프라별 특정 이벤트 발생량을 집계하여 시스템 부하 상태 모니터링.
2. **이벤트 타입 분포 및 추이 (Event Type Trends)**: 일일 혹은 시간 단위별로 이벤트 형태 분포 분석 및 비정상 증폭 패턴 확인.
3. **비정상 유입 IP 감지 (Suspicious DROP Detection)**: 방화벽에서 짧은 시간(예: 1시간) 동안 비정상적으로 다수의 `DROP` 이벤트를 발생시킨 출발지 IP(`src_ip`) 식별 및 격리 후보 목록 생성.
4. **Suspicious Port Access (비정상 포트 접근 탐지)**: 특정 프로토콜과 무관한 임의의 포트로 집중적인 요청이 발생했는지 감지.
5. **시간대별 트래픽 이벤트 분석**: 전체 네트워크의 사용 트렌드 및 유휴 시간 분석용 마트 테이블 구축.

### 6.2. 배치 처리 단계
1. **Extract**: `network_logs` 테이블에서 전날(T-1) 혹은 이전 1시간 동안 적재된 원본 로그 데이터를 조회.
2. **Transform**: 위 시나리오별 데이터를 가공 및 정렬 처리 (SQL 기반 집계 또는 Python 기반 분석).
3. **Load**: 가공된 데이터를 MySQL Data Mart 영역의 통계용 결과 테이블(예: `daily_device_stats`, `suspicious_ips` 등)에 `INSERT INTO ... ON DUPLICATE KEY UPDATE` 방식으로 저장.

---

## 7. 향후 확장 및 아키텍처 다각화 (Future Roadmap)

향후 비즈니스 및 로그 물량 급증 시 아래 문서를 참조하여 설계 방향을 다각화할 수 있습니다.

```text
docs/architecture/
├── overview.md (전체 네트워크 아키텍처)
├── nifi-flow-design.md (NiFi 기반 스트리밍 수집 설계) - [본 문서]
├── mysql-schema.md (MySQL 스키마 및 인덱스 최적화 상세 설계)
└── airflow-design.md (배치 분석 목적 및 대시보드 데이터 마트 설계)
```

1. **메시지 큐 버퍼 도입**: 소스 장비와 NiFi 사이에 Apache Kafka를 두어 일시적 대량 트래픽에 대한 유실 방지 및 부하 완충(Backpressure 제어).
2. **콜드/핫 스토리지 이중화**: 최근 데이터(30일)는 MySQL RDBMS에 적재하고, 전체 장기 아카이브 데이터는 Elasticsearch나 S3 오브젝트 스토리지로 분기 적재.
3. **실시간 이상 감지 경보**: NiFi 내에서 특정 심각도가 높은 실시간 이벤트(예: Critical Server Stop) 감지 시 `InvokeHTTP` 프로세서를 통해 Slack Webhook 또는 사내 알림 장치로 즉각 긴급 알림 발송.
