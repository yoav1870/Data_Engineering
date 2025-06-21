# ./producer/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer files
COPY producer/ratings_producer.py .

CMD ["python", "ratings_producer.py"]
