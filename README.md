# Fem-IoT
FemIoT repo for WP5


## Emission application

It uses MLFlow to manage the pipeline. It has a `conda.yaml` which defines the
packages requiered by the code. MLFlow will use this to run if `--no-conda` is
not specified.

Run using app/runpipe.sh.

## Building and logging in

```
docker build -t fem-iot .
docker run --name fem-iot -it fem-iot /bin/bash
```

## Running the application inside the testing network

```
docker run --name fem-iot -it --network=femiot --ip 172.15.1.05 fem-iot /bin/bash
```

## Initial setup for the testing framework

```
docker exec datanode mkdir /data
docker cp ../test_data/anon_ihs_test.csv datanode:/data
docker cp ../test_data/anon_2016-01.csv datanode:/data
```

```
docker exec -ti datanode /bin/bash
hdfs dfs -mkdir /data
hdfs dfs -put /data/*.csv /data/
```


