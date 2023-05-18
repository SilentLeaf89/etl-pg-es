FROM python:3.10-alpine

ENV PYTHONDONTWRITEBYTECODE 1 \
    PYTHONUNBUFFERED 1 \
    DB_NAME=DB_NAME \
    DB_USER=$DB_USER \
    DB_PASSWORD=$DB_PASSWORD \
    DB_HOST=$DB_HOST \
    DB_PORT=$DB_PORT

COPY ./requirements.txt /requirements.txt

RUN apk update \
    && apk add --virtual .tmp gcc musl-dev \
    && apk add postgresql-dev \
    && apk add netcat-openbsd \
    && mkdir -p /opt/etl \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && apk del .tmp

RUN addgroup -g 1000 app \
    && adduser -u 1000 -G app -s /bin/sh -D app\
    && chown -R app /opt/etl

USER app

WORKDIR /opt/etl

COPY --chown=app:app . .

CMD ["python", "src/load_data.py"]
