FROM python:3.9

WORKDIR /notebook

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

ENTRYPOINT /bin/sh -c 'jupyter notebook --no-browser --allow-root --ip 0.0.0.0'