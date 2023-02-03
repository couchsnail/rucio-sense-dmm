FROM alpine:latest

WORKDIR /app

RUN apk add --update --no-cache python3 && ln -sf python3 /usr/bin/python
RUN python3 -m ensurepip
RUN pip3 install --no-cache --upgrade pip setuptools
RUN pip3 install --no-cache pyyaml fastapi "uvicorn[standard]" python-multipart

COPY ./app /app

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "80"]