FROM ubuntu:22.04
LABEL authors="zoylendt"

RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY app/ /app

VOLUME /app/config
VOLUME /app/data
VOLUME /app/db
VOLUME /app/import

CMD ["python3", "main.py"]

#ENTRYPOINT ["top", "-b"]