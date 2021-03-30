FROM continuumio/miniconda3

WORKDIR /app

COPY ./app/ .

RUN apt-get -y update && apt-get install -y --no-install-recommends \
        gcc \
        linux-libc-dev \
        libc6-dev

RUN conda env create -f conda.yaml

# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "myenv", "/bin/bash", "-c"]

