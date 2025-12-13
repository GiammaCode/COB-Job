FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py .

ENV PYTHONUNBUFFERED=1
ENV OUTPUT_DIR=/mnt/results

CMD ["python", "worker.py"]