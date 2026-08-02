from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# 운영형 ETL 포트폴리오 기준에 맞춘 강화된 default_args
default_args = {
    'owner': 'data_platform',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30)
}

# v2 설계 문서 및 Airflow 2.7+ 표준을 완벽히 맞춘 DAG 정의
with DAG(
    dag_id='network_log_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 8, 1),
    schedule='@hourly',
    catchup=False,
    tags=['network', 'etl', 'mart'],
) as dag:

    # 파이프라인 시작 및 종료를 알리는 더미 태스크
    start_task = EmptyOperator(task_id='start_task')
    end_task = EmptyOperator(task_id='end_task')

    # 1. 시간별 장비 이벤트 통계 마트 적재
    mart_1_device_stats = SQLExecuteQueryOperator(
        task_id='insert_hourly_device_stats',
        conn_id='mysql_conn',
        sql="""
            INSERT INTO hourly_device_stats (event_hour, device_name, event_type, event_count)
            SELECT 
                DATE_FORMAT(event_time, '%Y-%m-%d %H:00:00') AS event_hour,
                device_name,
                event_type,
                COUNT(*) AS event_count
            FROM network_logs
            WHERE event_time >= '{{ data_interval_start }}'
              AND event_time < '{{ data_interval_end }}'
            GROUP BY event_hour, device_name, event_type
            ON DUPLICATE KEY UPDATE 
                event_count = VALUES(event_count);
        """
    )

    # 2. 의심 IP 후보 목록 (위협 탐지 조건: 특정 이벤트 및 1회 이상 발생) 마트 적재
    # 로그 생성기에서 특정 ip로 중복 건이 안 들어옴 (수정 필요)
    mart_2_threat_candidates = SQLExecuteQueryOperator(
        task_id='insert_threat_candidates',
        conn_id='mysql_conn',
        sql="""
            INSERT INTO threat_candidates (event_hour, src_ip, event_type, occurrence_count)
            SELECT 
                DATE_FORMAT(event_time, '%Y-%m-%d %H:00:00') AS event_hour,
                src_ip,
                event_type,
                COUNT(*) AS occurrence_count
            FROM network_logs
            WHERE event_time >= '{{ data_interval_start }}'
              AND event_time < '{{ data_interval_end }}'
              AND event_type IN ('DROP', 'PORT_SCAN', 'DDOS_DETECTED', 'LOGIN_FAIL')
            GROUP BY event_hour, src_ip, event_type
            HAVING COUNT(*) >= 1
            ON DUPLICATE KEY UPDATE 
                occurrence_count = VALUES(occurrence_count);
        """
    )

    # 3. 인프라 제품군별 시간당 트래픽 추이 마트 적재 (개선된 접두사 패턴 매칭 적용)
    mart_3_device_type_trend = SQLExecuteQueryOperator(
        task_id='insert_hourly_device_type_trend',
        conn_id='mysql_conn',
        sql="""
            INSERT INTO hourly_device_type_trend 
            (
                event_hour,
                device_type,
                total_events
            )
            SELECT 
                DATE_FORMAT(event_time, '%Y-%m-%d %H:00:00') AS event_hour,
                CASE 
                    WHEN device_name LIKE 'firewall-%' THEN 'firewall'
                    WHEN device_name LIKE 'switch-%' THEN 'switch'
                    WHEN device_name LIKE 'server-%' THEN 'server'
                    ELSE 'other'
                END AS device_type,
                COUNT(*) AS total_events
            FROM network_logs
            WHERE event_time >= '{{ data_interval_start }}'
              AND event_time < '{{ data_interval_end }}'
            GROUP BY 
                event_hour,
                device_type
            ON DUPLICATE KEY UPDATE 
                total_events = VALUES(total_events);
        """
    )

    # 4. 시간별 Top Talkers (시간대별 상위 10개 IP 정확히 추출) 마트 적재
    mart_4_top_talkers = SQLExecuteQueryOperator(
        task_id='insert_hourly_top_talkers',
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
                    event_hour,
                    src_ip,
                    event_count,
                    ROW_NUMBER() OVER(
                        PARTITION BY event_hour 
                        ORDER BY event_count DESC, src_ip ASC
                    ) AS rank_no
                FROM (
                    SELECT 
                        DATE_FORMAT(event_time, '%Y-%m-%d %H:00:00') AS event_hour,
                        src_ip,
                        COUNT(*) AS event_count
                    FROM network_logs
                    WHERE event_time >= '{{ data_interval_start }}'
                      AND event_time < '{{ data_interval_end }}'
                    GROUP BY event_hour, src_ip
                ) aggregated
            ) ranked
            WHERE rank_no <= 10
            ON DUPLICATE KEY UPDATE 
                event_count = VALUES(event_count),
                rank_no = VALUES(rank_no);
        """
    )

    # 설계 문서의 의존성 구조 반영: start -> [4개 마트 병렬 실행] -> end
    start_task >> [
        mart_1_device_stats,
        mart_2_threat_candidates,
        mart_3_device_type_trend,
        mart_4_top_talkers
    ] >> end_task