FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libffi-dev gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

RUN mkdir -p /app/downloads

CMD ["python", "main.py"]
