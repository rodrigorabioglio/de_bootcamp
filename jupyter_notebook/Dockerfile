FROM python:3.9

RUN pip install pandas sqlalchemy jupyter psycopg2 pyarrow datetime

WORKDIR /notebook

COPY . .

ENTRYPOINT /bin/sh -c 'jupyter notebook --no-browser --allow-root --ip 0.0.0.0'