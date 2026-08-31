FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN apt update &&  apt install pkg-config g++ -y 
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
RUN mkdir -p /var/lib/faust/state
ENV PYTHONUNBUFFERED=1
ENV FAUST_STORE=rocksdb://
CMD ["faust", "-A", "app", "worker", "-l", "info"]