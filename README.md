# Fem-IoT
FemIoT repo for WP5


## Emission application

It uses MLFlow to manage the pipeline. It has a `conda.yaml` which defines the
packages requiered by the code. MLFlow will use this to run if `--no-conda` is
not specified.

Run using app/test_mlflow.sh.

## Running the application inside the testing network

```
docker run --name fem-iot -it --network=docker-hadoop_FemIoT --ip 172.15.1.05
fem-iot /bin/bash
```
