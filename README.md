# Fem-IoT
FemIoT repo for WP5


## Emission application

### Background

TODO: Add technical background

### Technical details

This application computes the emissions produced by the ships given as input.
The defaulta parameters are defined in the code and in the `runpipe.sh` file.

The sample gaps are interpolated if there are two samples in, at least, 15
minutes. If there are gaps that are greater, the samples are not interpolated in
between. We interpolate a sample each 10 seconds.

The input data and the result output path is stored in the CSV path defined in `runpipe.sh`.

Run using app/runpipe.sh inside the docker machine.

At the end of the execution, a folder named **output** is created, containing
the emission summaries in csv and lineplots.

## Container infrastructure and data

TODO: Describe the two ways of working with the application.

### Building the container

```
docker build -t fem-iot .
```

### Build and run the testing framework
Inside of `docker_hadoop` folder:
```
docker-compose up
```

Notice that it will take a while to boot.


### Data setup 

TODO: Some intro

#### Data format

TODO: Define input CSVs

##### Input format


| nombre     | imo | mmsi | size_a | size_b | size_c | size_d | eslora | manga | draught | sog  | cog | rot | heading | navstatus | typeofshipandcargo | latitude         | longitude | fechahora           |
| ------     | --- | ---- | ------ | ------ | ------ | ------ | ------ | ----- | ------- | ---  | --- | --- | ------- | --------- | ------------------ | --------         | --------- | ---------           |
| Magic Ship | 0   | 33   | 20     | 170    | 5      | 20     | 190    | 25    | 5.5     | 19.8 | 358 | 0   | 356     | 0         | 60                 | 40.7606616666667 | 2.21904   | 2016-01-16 16:37:11 |

##### Output format

|imo|nombre|sog|latitude|longitude|time|type|hermes_type|me_rpm|ae_rpm|inst_pow_me|inst_pow_ae|design_speed|sfoc_me|sfoc_ae|last_move|d_lat|d_lon|amp_v|trans_p_me|trans_p_ae|sox_fact_me|sox_fact_ae|co2_fact_me|co2_fact_ae|nox_fact_me|nox_fact_ae|sox_me|sox_ae|co2_me|co2_ae|nox_me|nox_ae|
|---|------|---|--------|---------|----|----|-----------|------|------|-----------|-----------|------------|-------|-------|---------|-----|-----|-----|----------|----------|-----------|-----------|-----------|-----------|-----------|-----------|------|------|------|------|------|------|
|0|Magic Ship|19.700000762939453|40.76514434814453|2.2187983989715576|1452962280|Passenger/Ro-Ro Cargo Ship|FE|500|514|18006.0|3420.0|21.399999618530273|166|166|0|0.0|0.0|0.0|10485.126|3420.0|0.33165503|0.33165503|517.04|517.04|12.984299|12.912785|0.05795741|0.018904336|90.35382|29.47128|2.2690334|0.73602873|



#### Standalone

Copy the `../test_data/` CSVs into the desired HDFS path. For example:
```
IHS="hdfs:///user/ubuntu/emis_femiot/data/anon_ihs_test.csv"
AIS="hdfs:///user/ubuntu/emis_femiot/data/anon_2016-01.csv"
```

**Make sure that this and the output folders exists before running.**


#### With the testing framework

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



## Execution

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

### Running without network

If you want to use the standalone mode, the only thing that changes is the
docker run itself. The files should point to somewhere in the container's
filesystem.

```
docker run --name fem-iot -it fem-iot /bin/bash
./runpipe.sh # With the proper configuration
```

## Output

After executing `runpipe.sh` the resulting emissions will be available in the
folder configured as output (`OUT`) folder. The plots will be available in
`output/plot`. 

The results show aggregations of the emissions by day, day of the week, week and month.

### Output data format
TODO: Define the output format
