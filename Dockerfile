FROM continuumio/miniconda3

WORKDIR /app

RUN apt-get -y update && apt-get install -y --no-install-recommends \
        gcc \
        linux-libc-dev \
        libc6-dev \
        nano

# Copy conda.yaml for installation
COPY ./app/conda.yaml .

RUN conda env create -f conda.yaml

# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "aisarch_pipeline", "/bin/bash", "-c"]

# Activate conda on bash login
RUN echo "conda activate aisarch_pipeline" >> ~/.bashrc

# Copy application
COPY ./app/ .

# Copy data
COPY ./test_data /home/data/femiot/

