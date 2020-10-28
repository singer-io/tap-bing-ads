FROM ubuntu:18.04

ENV APP_PATH=/app \
    DEBIAN_FRONTEND=noninteractive \
    PYTHONIOENCODING=utf-8 \
    LANG=C.UTF-8

RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install -y --no-install-recommends \
        build-essential \
        python3-pip python3.7 python3.7-dev python3.7-venv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /app

ENV PATH=/home/venv/tap-bing-ads/bin:$PATH

COPY . .

RUN pip install -e .[tests]
