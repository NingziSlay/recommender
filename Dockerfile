FROM python:3.7-alpine

COPY requirements.txt ./

RUN pip install -U -r /requirements.txt -i http://pypi.doubanio.com/simple/ --trusted-host pypi.doubanio.com

WORKDIR /usr/src/app

COPY . .

ENV ENVIRONMENT=test
