FROM python:3.12-slim
LABEL authors="gabriel.cerioni@redis.com"

WORKDIR /app
RUN pip install --no-cache-dir boto3==1.35.64 pyarrow==18.1.0 redis==5.2.0

COPY make_offline_store.py hydrate.py ./

CMD ["python", "hydrate.py", "--help"]
