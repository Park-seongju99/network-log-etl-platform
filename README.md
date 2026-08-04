# Network Log ETL Platform (네트워크 로그 실시간 수집 및 배치 분석 플랫폼)

본 플랫폼은 분산 인프라 환경에서 발생하는 대용량 네트워크 장비 로그를 **실시간으로 수집, 정제, 적재**하고, 주기적으로 **배치 분석 파이프라인(Airflow DAG)을 수행**하여 보안 위협 IP 탐지 및 장비별 통계를 도출하는 고성능 End-to-End 데이터 엔지니어링 플랫폼입니다.

---

## 1. 시스템 전체 아키텍처 및 흐름 (Architecture Flow)

본 플랫폼은 데이터 가공 연산(집계, 정렬 등)을 Airflow 메모리 내부가 아닌 강력한 RDBMS인 MySQL 데이터베이스 자체 엔진의 컴퓨팅 자원을 활용해 처리하는 **ELT(Extract-Load-Transform) 패러다임**을 적극 준수합니다.

```text
               +----------------------------------+
               |      [Python Log Generator]      |
               | (firewall, switch, server logs)  |
               +----------------------------------+
                                |
                                | TCP Stream (Port: 7070)
                                v
               +----------------------------------+
               |          [Apache NiFi]           |
               |  - ListenTCP (7070)              |
               |  - ExtractText (Regex Parsing)   |
               |  - AttributesToJSON (JSON 화)    |
               |  - PutDatabaseRecord (DB 적재)   |
               +----------------------------------+
                                |
                                | JDBC (Port: 3306)
                                v
               +----------------------------------+
               |        [MySQL Raw Layer]         |
               |   - Database: etl_db             |
               |   - Table: network_logs          |
               +----------------------------------+
                                |
                                | Batch ETL Pipeline (Port: 8080)
                                v
               +----------------------------------+
               |         [Apache Airflow]         |
               |  - Scheduled DAGs (@hourly)      |
               |  - Statistics & Threat Detection |
               |  - Load into Data Mart Tables    |
               +----------------------------------+
```

### 데이터 수명 주기 (Data Lifecycle) 및 컴포넌트 역할
1. **로그 생성 (Log Generation)**: `generator` 컨테이너 내 파이썬 스크립트가 가상의 네트워크 장비(Firewall, Switch, Server) 로그를 대량 발생시켜 TCP 소켓(7070 포트)을 통해 실시간 스트림으로 전송합니다.
2. **실시간 수집 및 정제 (Ingestion & ETL)**: `Apache NiFi`가 TCP 연결을 수신하고, 유입되는 Raw 로그 본문을 FlowFile로 변환합니다. 정규표현식(`ExtractText`)을 사용해 메타데이터 속성을 추출하고, `AttributesToJSON`을 통해 완전한 JSON 포맷 데이터로 직렬화한 후, `PutDatabaseRecord` 프로세서를 통해 MySQL의 `network_logs` 테이블에 밀어 넣습니다.
3. **배치 분석 및 마트 구성 (Batch Analytics)**: `Apache Airflow` 스케줄러가 매 시간마다 배치 파이프라인을 트리거하여 `network_logs` 테이블의 데이터를 집계/변환하고, 보안 위협 IP 탐지 및 인프라 통계 목적의 **Data Mart 테이블**에 고속 적재(Upsert 기반 멱등성 보장)합니다.

---

## 2. 디렉토리 구조 (Directory Structure)

```text
/home/user/network-log-etl-platform/
├── compose.yaml                # 통합 컨테이너 오케스트레이션 정의 파일
├── .gitignore                  # Git 추적 예외 파일 설정 리스트
├── .env                        # 환경 변수 정의 파일 (포트, 데이터베이스 설정 등)
├── mysql-connector-j-8.3.0.jar # NiFi RDBMS 연결용 MySQL JDBC 드라이버 파일
├── README.md                   # 프로젝트 메인 가이드 문서 (본 문서)
│
├── airflow/                    # Apache Airflow 관련 리소스
│   └── dags/                   # 배치 분석용 DAG 보관 폴더
│       └── etl_dag.py          # 시간별 데이터 마트 적재용 Airflow DAG 파일
│
├── docker/                     # 추가 Docker 및 관련 설정 보관 디렉토리
│   └── docker-compose.yml      # 보조/이전용 Docker Compose 파일
│
├── docs/                       # 기술 설계 및 상세 가이드 문서 아카이브
│   ├── architecture/
│   │   ├── airflow-design.md   # Airflow 배치 처리 및 데이터 마트 상세 설계서
│   │   └── nifi-flow-design.md # NiFi 파싱 규칙 및 데이터 흐름 상세 설계서
│   ├── images/                 # 아키텍처 및 설정 이미지 보관 디렉토리
│   ├── performance/            # 성능 튜닝 및 벤치마크 가이드
│   └── troubleshooting/        # 에러 분석 및 트러블슈팅 가이드
│
├── generator/                  # 파이썬 기반 네트워크 가짜 로그 생성 엔진
│   ├── src/
│   │   ├── main.py             # 실시간 로그 전송 프로세스 메인 엔트리
│   │   └── test.py             # 로컬 테스트/디버깅 목적 로그 생성기
│   ├── config.yaml             # 장비 설정 가중치 및 네트워크 포트 매핑 메타데이터
│   ├── requirements.txt        # 의존성 라이브러리 목록 (Faker, PyYAML 등)
│   └── Dockerfile              # 로그 생성기 경량 도커화 설정 파일
│
├── nifi/                       # Apache NiFi 관련 백업 리소스
│   └── templates/
│       └── Network_Log_ETL.json # 전체 데이터 파이프라인 NiFi 템플릿 파일
│
└── sql/                        # 데이터베이스 구축 스키마 스크립트
    ├── 01_create_tables.sql    # 원천 로그 테이블 및 데이터 마트 스키마 설계서
    └── 02_data_mart_tables.sql # 4개의 핵심 데이터 마트 테이블 스키마 정의서
```

---

## 3. 설치 및 구성 (Environment Setup)

### 3.1. 사전 요구사항 (Prerequisites)
* Docker 및 Docker Compose 환경 설치 필요
* 호스트 시스템 포트 점유 여부 확인: 8080(Airflow Web), 8443(NiFi HTTPS), 7070(ListenTCP), 3306(MySQL)

### 3.2. 환경 변수 설정 (`.env`)
프로젝트 루트 디렉토리에 `.env` 파일을 구성하여 기밀 정보와 구성 포트들을 통합 관리합니다. 

```env
PROJECT_NAME=network-log-etl
NIFI_HOST=nifi
NIFI_TCP_PORT=7070
NIFI_HTTPS_PORT=8443
NIFI_USERNAME=admin
NIFI_PASSWORD=YourStrongPasswordHere_Minimum12Chars
NIFI_PROXY_HOST=localhost

MYSQL_PORT=3306
MYSQL_ROOT_PASSWORD=YourRootPasswordHere
MYSQL_DATABASE=etl_db
MYSQL_USER=network
MYSQL_PASSWORD=YourUserPasswordHere
```

---

## 4. 실행 방법 (How to Run)

### 4.1. 플랫폼 전체 기동 (Docker Compose)
통합 서비스들을 한 번에 빌드하고 실행하기 위해 프로젝트 루트에서 백그라운드 모드로 컨테이너를 기동합니다.

```bash
docker compose up -d --build
```

실행 후 아래 명령어로 모든 컴포넌트가 안정적으로 구동되었는지 상태를 검증합니다:
```bash
docker compose ps
```

### 4.2. Apache NiFi 파이프라인 활성화
1. **Web UI 접속**: 브라우저에서 `https://localhost:8443/nifi`에 접속합니다.
   * `.env` 파일에 기재된 `NIFI_USERNAME` 및 `NIFI_PASSWORD`를 활용하여 로그인합니다.
2. **템플릿 임포트 (Import Template)**:
   * NiFi 작업 캔버스 빈 곳을 우클릭한 뒤 `Upload Template`을 클릭합니다.
   * `nifi/templates/Network_Log_ETL.json` 파일을 등록합니다.
   * 상단 컴포넌트 툴바에서 템플릿(Template) 아이콘을 드래그 앤 드롭하여 캔버스에 안착시킵니다.
3. **JDBC 컨트롤러 서비스 활성화 및 설정 수정**:
   * **DBCPConnectionPool** 서비스 설정을 열고 `Properties` 탭에서 **Password**를 입력합니다 (`.env` 내 `MYSQL_PASSWORD`와 일치).
   * **Database Driver Locations** 경로가 `/tmp/mysql-connector-j-8.3.0.jar`로 정확히 매핑되었는지 확인합니다 (볼륨 마운트를 통해 컨테이너 내 해당 위치로 드라이버가 연결됩니다).
   * 컨트롤러 서비스를 활성화(Enable) 처리합니다.
4. **프로세서 그룹 시작**:
   * 작업 그룹을 우클릭하여 `Start`를 누르고, 모든 프로세서가 녹색 화살표(실행 중) 상태로 전환되었는지 확인합니다.

### 4.3. Apache Airflow 배치 관리
1. **Web UI 접속**: 브라우저에서 `http://localhost:8080`에 접속합니다. (기본 계정: `airflow` / `airflow`)
2. **MySQL Connection 설정**:
   * 상단 메뉴에서 `Admin -> Connections`로 이동합니다.
   * `mysql_conn`을 추가하고 아래 내용을 기입합니다.
     - **Connection Id**: `mysql_conn`
     - **Connection Type**: `MySQL`
     - **Host**: `network-mysql` (또는 `compose.yaml`에 정의된 mysql 컨테이너 서비스명)
     - **Schema**: `etl_db`
     - **Login**: `network` (또는 `.env` 내 `MYSQL_USER`)
     - **Password**: `.env` 내 `MYSQL_PASSWORD`
     - **Port**: `3306`
3. **DAG 활성화**:
   * `network_log_etl_pipeline` DAG를 찾아서 활성화(Toggle On) 처리합니다.
   * `@hourly` 주기마다 배치 파이프라인이 정상 트리거되는지 모니터링합니다.

---

## 5. 데이터베이스 스키마 및 마트 구조 (Database & Data Mart)

### 5.1. Raw Layer (`network_logs` 테이블)
수집된 모든 네트워크 이벤트를 원본 유실 없이 영속화하는 원천 테이블입니다.
* `event_time`: 이벤트 실제 발생 시각
* `device_name`: 장비명 (예: `fw-seoul-01`, `sw-seoul-01`, `server-seoul-01`)
* `event_type`: 이벤트 종류 (예: `ALLOW`, `DENY`, `DROP`, `PORT_UP`, `LOGIN_FAIL` 등)
* `src_ip` / `dst_ip`: 출발지 및 목적지 IP 주소
* `src_port` / `dst_port`: 출발지 및 목적지 포트 번호
* `protocol`: 네트워크 프로토콜 (`TCP`, `UDP`, `ICMP`)

### 5.2. Data Mart Layers (4종)
배치 변환 쿼리를 거쳐 인프라 모니터링 및 보안 분석용으로 최적화된 마트 테이블입니다. 모든 적재 작업에는 `ON DUPLICATE KEY UPDATE` 패턴(Upsert)을 사용하여 **완벽한 데이터 멱등성**을 보장합니다.

1. **시간별 장비 이벤트 통계 (`hourly_device_stats`)**
   - 장비 및 이벤트 유형별 시간 단위 발생 빈도를 보관하여 전체 리소스 사용률 및 인프라 추이를 한눈에 모니터링할 수 있도록 돕습니다.
2. **의심 IP 후보 목록 (`threat_candidates`)**
   - `DROP`, `PORT_SCAN`, `DDOS_DETECTED`, `LOGIN_FAIL` 등 비정상/위협 성격의 이벤트가 1회 이상 유입된 출발지 IP들을 격리하여 분석용 목록을 구축합니다.
3. **인프라 제품군별 시간당 트래픽 추이 (`hourly_device_type_trend`)**
   - 개별 장비명을 제품군 단위(`firewall`, `switch`, `server`)로 그루핑하고 트래픽 분포의 이상 여부를 감시합니다.
4. **시간별 Top Talkers (`hourly_top_talkers`)**
   - 시간대별로 가장 많은 트래픽/이벤트를 전송한 출발지 IP 상위 10개를 윈도우 함수(`ROW_NUMBER()`)를 이용해 정확하게 도출하여 이상 징후를 추적합니다.

---

## 6. 모니터링 및 복구 전략 (Monitoring & Troubleshooting)

* **실시간 수집 로그 유량 확인**:
  로그 생성기가 실행되면서 생성되는 원천 스트림은 NiFi의 `ListenTCP` 데이터 큐를 타고 유입되며 각 파싱 단계별 수집 유량을 직관적으로 모니터링할 수 있습니다.
* **장애 격리 (DLQ 및 예외 아카이브)**:
  데이터베이스 장애 혹은 적재 부하 발생 시, `PutDatabaseRecord`의 `failure` 관계를 통해 흘러간 데이터 흐름은 오프라인 스토리지 폴더인 `troubleshooting/error-log/` 하위 영역에 영구 보존됩니다. 원인 조치 후 이 파일들을 통해 수동 데이터 복구 및 Replay 작업이 가능합니다.
* **Airflow 재실행 및 백필 (Backfill)**:
  DAG 정의 시 `catchup=False` 설정과 Airflow 내장 매크로 `{{ data_interval_start }}` 및 `{{ data_interval_end }}`를 활용하여 과거 시점 실패 배치를 명확하고 일관되게 수동 트리거(Clear / Backfill)하여 정상 복구할 수 있습니다.
