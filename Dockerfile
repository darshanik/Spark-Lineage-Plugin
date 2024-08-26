FROM python:3.8.16-slim-buster

RUN apt-get update && \
    apt-get install -y vim && \
    rm -rf /var/lib/apt/lists/*

# change directory
WORKDIR /spark-lineage

# copy only requirements for installation
COPY requirements.txt .

# install packages
RUN pip install -r requirements.txt

RUN mkdir -pv /logs

# copy spark lineages to data directory 
COPY spark_lineage_transform .
