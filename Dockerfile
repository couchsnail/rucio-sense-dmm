FROM python:3.7-slim-bullseye

RUN pip3 install sense-o-api==1.23 sqlalchemy psycopg2-binary networkx
EXPOSE 5000

WORKDIR /opt/dmm
COPY ./ /opt/dmm

ENV PYTHONPATH=/opt/dmm/

ENV DMM_CONFIG /opt/dmm/dmm.cfg

ENTRYPOINT ["bin/dmm", "--loglevel", "debug"]
