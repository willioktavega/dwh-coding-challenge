FROM python:3.9.15-slim-buster

RUN mkdir /app
RUN mkdir /data

COPY ./solution/ /app
COPY ./data/ /data
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV WORKDIR=/
WORKDIR /app

CMD [ "python3", "main.py"]