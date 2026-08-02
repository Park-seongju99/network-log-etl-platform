-- 사용하실 데이터베이스 지정 (기존 DB명에 맞게 유지)
USE etl_db;

-- 1. 시간별 장비 이벤트 통계
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

-- 2. 의심 IP 후보 목록 (위협 탐지)
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

-- 3. 인프라 제품군별 시간당 트래픽 추이
CREATE TABLE IF NOT EXISTS hourly_device_type_trend (
    event_hour DATETIME NOT NULL,
    device_type VARCHAR(50) NOT NULL, -- 'firewall', 'switch', 'server'
    total_events BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (event_hour, device_type),
    INDEX idx_event_hour (event_hour)
);

-- 4. 시간별 Top Talkers (이벤트 발생량 상위 10개 IP)
CREATE TABLE IF NOT EXISTS hourly_top_talkers (
    event_hour DATETIME NOT NULL,
    src_ip VARCHAR(50) NOT NULL,
    event_count BIGINT DEFAULT 0,
    rank_no INT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (event_hour, src_ip),
    INDEX idx_event_hour (event_hour)
);