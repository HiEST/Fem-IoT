# Fem-IoT
FemIoT repo for WP5


## Emission application

This application computes the emissions produced by the ships given as input.
The defaulta parameters are defined in the code and in the `runpipe.sh` file.

The sample gaps are interpolated if there are two samples in, at least, 15
minutes. If there are gaps that are greater, the samples are not interpolated in
between. We interpolate a sample each 10 seconds.

The input data and the result output path is stored in the CSV path defined in `runpipe.sh`.

Run using app/runpipe.sh inside the docker machine.

At the end of the execution, a folder named **output** is created, containing
the emission summaries in csv and lineplots.

## Building and logging in

```
docker build -t fem-iot .
docker run --name fem-iot -it fem-iot /bin/bash
```

## Running without network

```
docker run --name fem-iot -it fem-iot /bin/bash
```

### Initial setup 

Copy the `../test_data/` CSVs into the desired HDFS path. By default:
```
IHS="hdfs:///user/ubuntu/emis_femiot/data/anon_ihs_test.csv"
AIS="hdfs:///user/ubuntu/emis_femiot/data/anon_2016-01.csv"
```

Make sure that this and the output folders exists before running.

In the `runpipe.sh` script you will find where to specify the HDFS URL along
with the files.


## Running with testing network

```
docker run --name fem-iot -it --network=femiot --ip 172.15.1.05 fem-iot /bin/bash
```

### Initial setup for the testing framework

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


