# Network Log ETL Platform (네트워크 로그 실시간 수집 및 배치 분석 플랫폼)

본 플랫폼은 분산 환경에서 발생하는 대용량 네트워크 장비 로그를 실시간으로 수집, 파싱, 변환하여 관계형 데이터베이스(MySQL)에 실시간으로 적재하고, 주기적으로 배치 분석 파이프라인(Airflow DAG)을 수행하여 위협 IP 탐지 및 장비별 통계를 도출하는 End-to-End 데이터 엔지니어링 플랫폼입니다.

---

## 1. 시스템 전체 아키텍처 및 흐름

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
                                | Batch ETL Pipeline
                                v
               +----------------------------------+
               |         [Apache Airflow]         |
               |  - Scheduled DAGs                |
               |  - Statistics & Threat Detection |
               |  - Load into Data Mart Tables    |
               +----------------------------------+
```

### 데이터 처리 수명 주기 (Data Lifecycle)
1. **로그 생성 (Log Generation)**: `generator` 컨테이너 내 파이썬 스크립트가 무작위 네트워크 장비(Switch, Firewall, Server)의 원천 로그를 지속적으로 발생시킨 뒤 TCP 소켓(7070 포트)을 통해 실시간 전송합니다.
2. **실시간 수집 및 정제 (Ingestion & ETL)**: `NiFi`가 TCP 연결을 통해 유입되는 Raw 로그 본문을 FlowFile로 생성하고, 정규표현식(`ExtractText`)으로 항목별 속성값을 추출한 뒤, `AttributesToJSON`을 통해 정형 JSON 포맷으로 실시간 직렬화하여 MySQL 데이터베이스의 `network_logs` 테이블에 대량 고성능 적재합니다.
3. **배치 분석 (Batch Analytics)**: `Airflow` 배치 파이프라인이 주기적으로 트리거되어 최근 1시간/1일 동안 수집된 원본 로그 데이터를 분석하여 위협 대상 IP 식별 및 포트 트래픽 트렌드 통계 등의 Data Mart 적재 작업을 처리합니다.

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
│
├── docker/                     # 추가 Docker 및 관련 설정 보관 디렉토리
│   └── docker-compose.yml      # 보조/이전용 Docker Compose 파일
│
├── docs/                       # 기술 설계 및 상세 가이드 문서 아카이브
│   ├── architecture/
│   │   └── nifi-flow-design.md # NiFi 파싱 규칙 및 흐름도 상세 기술서
│   ├── images/
│   ├── performance/
│   └── troubleshooting/
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
    └── 02_seed_data.sql        # 초기 세정 및 시드 데이터 적재용 스크립트
```

---

## 3. 설치 및 구성 (Environment Setup)

### 3.1. 사전 요구사항 (Prerequisites)
* Docker Desktop 및 Docker Compose 설치 완료 상태 권장
* 호스트 7070(TCP 수집), 8443(NiFi Web), 3306(MySQL) 등의 포트 점유 여부 확인

### 3.2. 환경 변수 설정 (`.env`)
프로젝트 루트 디렉토리에 `.env` 파일을 구성하여 필요한 기밀 정보 및 인프라 포트를 관리합니다. 
*(예시 템플릿)*
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

### 4.1. 플랫폼 전체 기동 (Docker Compose Up)
통합 서비스들을 한 번에 실행하기 위해 프로젝트 루트에서 백그라운드 모드로 컴포즈 명령을 실행합니다.

```bash
docker compose up -d --build
```
이후 아래 명령어로 모든 컴포넌트가 안정적으로 구동되었는지 검증합니다:
```bash
docker compose ps
```

### 4.2. Apache NiFi 파이프라인 활성화 및 연동
1. **Web UI 접속**: 브라우저에서 `https://localhost:8443/nifi`에 접속합니다.
   * `.env` 파일에 기재된 `NIFI_USERNAME` 및 `NIFI_PASSWORD`를 활용하여 안전하게 로그인합니다.
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

---

## 5. 파이프라인 모니터링 및 복구 (Monitoring & Failure Handling)

* **실시간 수집 로그 유량 확인**:
  로그 생성기가 실행되면서 생성되는 원천 스트림은 NiFi의 `ListenTCP` 데이터 큐를 타고 유입되며 각 파싱 단계별 수집 유량을 직관적으로 모니터링할 수 있습니다.
* **장애 격리 (DLQ 및 예외 아카이브)**:
  데이터베이스 장애 혹은 적재 부하 발생 시, `PutDatabaseRecord`의 `failure` 관계를 통해 흘러간 데이터 흐름은 오프라인 스토리지 폴더인 `troubleshooting/error-log/` 하위 영역에 영구 보존됩니다. 원인 조치 후 이 파일들을 통해 수동 데이터 복구 및 Replay 작업이 가능합니다.
