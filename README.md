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
./runpipe.sh # With the proper configuration
```

### Initial setup 

Copy the `../test_data/` CSVs into the desired HDFS path. For example:
```
IHS="hdfs:///user/ubuntu/emis_femiot/data/anon_ihs_test.csv"
AIS="hdfs:///user/ubuntu/emis_femiot/data/anon_2016-01.csv"
```

**Make sure that this and the output folders exists before running.**

    
    
## Running with testing network

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

### Running the testing framework
Inside of `docker_hadoop` folder:
```
docker-compose up
```

Notice that it will take a while to boot.

### Execution

`runpipe.sh` script has the following parameters:

- `HDFS_SERVER`: HDFS server endpoint. Example: "hdfs://172.15.1.10:9000"
- `IHS`: Path to the IHS dataset inside of HDFS. Example: "hdfs:///data/anon_ihs_test.csv"
- `AIS`: Path to the IHS dataset inside of HDFS. Example: "hdfs:///data/anon_2016-01.csv"
- `TMP`: Path for the intermediate calculations. Example: "hdfs:///data/calc/"
- `OUT`: Path where the output is stored. Example: "hdfs:///data/calc_emis"

You should input the parameters in the following way:
```
docker run --name fem-iot -it --network=femiot --ip 172.15.1.05 fem-iot /bin/bash
./runpipe.sh HDFS_SERVER="ServerURL" IHS="IHSFile" AIS="AISFile" TMP="TMPFolder" OUT="OUTFolder
```

With the example infrastructure:
```
./runpipe.sh HDFS_SERVER="hdfs://172.15.1.10:9000" IHS="hdfs:///data/anon_ihs_test.csv" AIS="hdfs:///data/anon_2016-01.csv" TMP="hdfs:///data/calc/" OUT="hdfs:///data/calc_emis"
```

## Output

After executing `runpipe.sh` the resulting emissions will be available in the `output` folder. The plots will be available in `output/plot`. 

The results show aggregations of the emissions by day, day of the week, week and month.
