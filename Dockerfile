FROM python:3.7-alpine

RUN apk update && apk add postgresql-dev gcc musl-dev

COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY . /app
WORKDIR /app

ENV PYTHONUNBUFFERED=1
CMD [ "python", "./main.py" ]
