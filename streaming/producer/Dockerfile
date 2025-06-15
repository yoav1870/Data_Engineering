# ./producer/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY producer.py .
RUN pip install --no-cache-dir kafka-python

CMD ["python", "producer.py"]
