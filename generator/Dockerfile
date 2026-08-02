FROM python:3.12-slim


WORKDIR /app


COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt


COPY config.yaml .


COPY src ./src


CMD ["python", "src/main.py"]