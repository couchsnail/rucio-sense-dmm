FROM alpine:latest

WORKDIR /app

RUN apk add --update --no-cache python3 && ln -sf python3 /usr/bin/python
RUN python3 -m ensurepip
RUN pip3 install --no-cache --upgrade pip setuptools

COPY etc/requirements.txt /requirements.txt
RUN pip3 install --no-cache -r /requirements.txt

COPY ./monit /app

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "80"]
