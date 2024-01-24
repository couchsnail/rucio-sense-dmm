FROM python:3.7-slim-bullseye

RUN apt update && apt install sqlite3
RUN apt-get clean autoclean && \
    apt-get autoremove --yes && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/

RUN pip3 install sense-o-api==1.31 sqlalchemy psycopg2-binary networkx rucio-clients flask

WORKDIR /opt/dmm
COPY ./ /opt/dmm

ENV PYTHONPATH=/opt/dmm/
ENV DMM_CONFIG /opt/dmm/dmm.cfg

EXPOSE 80

ENTRYPOINT ["bin/dmm", "--loglevel", "debug"]
