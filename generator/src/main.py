import os
import time
import random
import socket
import yaml

from datetime import datetime
from faker import Faker


# Faker 객체 생성
fake = Faker()



# 1. config.yaml 읽기
BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

CONFIG_PATH = os.path.join(
    BASE_DIR,
    "config.yaml"
)

with open(CONFIG_PATH, "r") as file:
    config = yaml.safe_load(file)


# 2. 환경 변수 가져오기
NIFI_HOST = os.getenv(
    "NIFI_HOST",
    "nifi"
)

NIFI_PORT = int(
    os.getenv(
        "NIFI_TCP_PORT",
        7070
    )
)

EVENTS_PER_SECOND = config["generator"]["events_per_second"]
INTERVAL = 1 / EVENTS_PER_SECOND


# 3. config 데이터 변수 저장
DEVICE_RATIO = config["devices"]

DEVICE_NAMES = config["device_names"]

EVENTS = config["events"]

PROTOCOLS = config["network"]["protocols"]

COMMON_PORTS = config["network"]["common_ports"]

# 4. 장비 종류 선택
def generate_device_type():

    devices = list(
        DEVICE_RATIO.keys()
    )


    weights = list(
        DEVICE_RATIO.values()
    )


    return random.choices(
        devices,
        weights=weights,
        k=1
    )[0]


# 5. 장비 이름 생성
def generate_device_name(device_type):

    return random.choice(
        DEVICE_NAMES[device_type]
    )


# 6. 네트워크 로그 생성
def generate_log():


    # firewall / switch / server 선택
    device_type = generate_device_type()


    # 이벤트 선택
    event = random.choice(
        EVENTS[device_type]
    )


    # 장비 이름 생성
    device_name = generate_device_name(
        device_type
    )


    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


    src_ip = fake.ipv4()

    dst_ip = fake.ipv4()


    src_port = random.randint(
        1024,
        65535
    )


    dst_port = random.choice(
        COMMON_PORTS
    )


    protocol = random.choice(
        PROTOCOLS
    )


    # Raw Network Log
    log = (
        f"{timestamp} "
        f"{device_name} "
        f"{event} "
        f"src={src_ip} "
        f"dst={dst_ip} "
        f"sport={src_port} "
        f"dport={dst_port} "
        f"protocol={protocol}\n"
    )


    return log


# 7. Nifi 전송
def send_logs():


    print(
        "Network Log Generator Start"
    )


    print(
        f"Target : {NIFI_HOST}:{NIFI_PORT}"
    )


    while True:


        try:

            # TCP Socket 생성
            with socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            ) as sock:


                # NiFi 연결
                sock.connect(
                    (
                        NIFI_HOST,
                        NIFI_PORT
                    )
                )


                print(
                    "NiFi Connected"
                )


                while True:


                    log = generate_log()


                    # 문자열 → bytes 변환 후 전송
                    sock.sendall(
                        log.encode("utf-8")
                    )


                    print(
                        log.strip()
                    )


                    time.sleep(
                        INTERVAL
                    )



        except ConnectionRefusedError:


            print(
                "NiFi 연결 실패"
            )


            time.sleep(3)



        except KeyboardInterrupt:


            print(
                "Generator 종료"
            )


            break



        except Exception as e:


            print(
                f"Error : {e}"
            )


            time.sleep(3)


# 8. 프로그램 시작
if __name__ == "__main__":

    send_logs()