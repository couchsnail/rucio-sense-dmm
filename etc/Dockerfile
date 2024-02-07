FROM python:3.7-slim-bullseye

RUN apt update && apt install sqlite3
RUN apt-get clean autoclean && \
    apt-get autoremove --yes && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/

RUN pip3 install sense-dmm==0.2.3

EXPOSE 80

ENTRYPOINT ["dmm", "--loglevel", "debug"]