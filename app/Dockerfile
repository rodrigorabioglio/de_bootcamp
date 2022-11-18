FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app

COPY . .

ENTRYPOINT ["python", "ingest_taxi_data.py"]


