FROM python:3.10

RUN mkdir /src
WORKDIR /src

COPY . /src
RUN mkdir /code
RUN pip install -r requirements.txt
