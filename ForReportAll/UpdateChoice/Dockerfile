FROM python:3.12-slim

WORKDIR /update_choice_cronjob

ADD . /update_choice_cronjob/

RUN apt-get update
RUN apt-get install -y cron

ENV TZ=Asia/Bangkok

COPY crontab /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["cron","-f"]