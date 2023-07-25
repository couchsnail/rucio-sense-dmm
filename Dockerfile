FROM python:3.7-slim-bullseye

RUN pip3 install sense-o-api==1.23 sqlalchemy psycopg2-binary
EXPOSE 5000

WORKDIR /opt/dmm

ENTRYPOINT ["/bin/bash"]