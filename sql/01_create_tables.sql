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

    INDEX idx_event_time(event_time),
    INDEX idx_device_name(device_name),
    INDEX idx_event_type(event_type),
    INDEX idx_src_ip(src_ip)
);