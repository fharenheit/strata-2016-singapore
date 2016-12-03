# Strata-2016-SINGAPORE

## Pre-requisites for Installation

Java installed on the laptop

## Download Spark 2.0

Download Spark 2.0 from here : http://spark.apache.org/downloads.html

Direct Download link : http://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz

## Install Spark 2.0 on Mac

tar -zxvf spark-2.0.2-bin-hadoop2.7.tgz

export PATH=$PATH:/Users/path_to_downloaded_spark/spark-2.0.2-bin-hadoop2.7/bin

## Install Spark 2.0 on Windows

tar -zxvf spark-2.0.2-bin-hadoop2.7

Add the spark bin directory to Path : ...\spark-2.0.2-bin-hadoop2.7\bin

## Set up winutils.exe on Windows

- download winutils.exe from https://github.com/steveloughran/winutils/tree/master/hadoop-2.6.0/bin
- move it to c:\hadoop\bin
- set HADOOP_HOME in your environment variables
    - HADOOP_HOME = C:\hadoop
- run from command prompt:
    - C:\hadoop\bin\winutils.exe chmod 777 /tmp/hive
- run spark-shell from command prompt with extra conf parameter
    - spark-shell --driver-memory 2G --executor-memory 3G --executor-cores 2 -conf spark.sql.warehouse.dir=file:///c:/tmp/spark-warehouse

## Git

Nice to have

[IMPORTANT]: Downloads
Have the following downloaded before the session
* Spark binaries
* JDK installed (> 1.7.x)
* https://github.com/WhiteFangBuck/strata-2016-ny

## Running spark-shell

- export PATH=$PATH:/Users/path_to_downloaded_spark/spark-2.0.2-bin-hadoop2.7/bin
- spark-shell

### Paste in spark-shell

scala> :paste
// Entering paste mode (ctrl-D to finish)

## IntelliJ

The code can be run either in spark-shell or IntelliJ

- Install IntelliJ from https://www.jetbrains.com/idea/download/
- Add the scala plugin
- Import the code as a maven project





