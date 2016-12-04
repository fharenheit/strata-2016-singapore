# Strata-2016-SINGAPORE

This tutorial can either be run in spark-shell or in an IDE (IntelliJ or Scala IDE for Eclipse)

Below are the steps for running in spark-shell or in IntelliJ/Scala IDE for Eclipse

## Pre-requisites for Installation

Java/JDK 1.7+ has to be installed on the laptop before proceeding with the steps below.

## Running in spark-shell

### Download Spark 2.0.2

Download Spark 2.0.2 from here : http://spark.apache.org/downloads.html

Direct Download link : http://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz

### Install Spark 2.0.2 on Mac

tar -zxvf spark-2.0.2-bin-hadoop2.7.tgz

export PATH=$PATH:/Users/path_to_downloaded_spark/spark-2.0.2-bin-hadoop2.7/bin

### Install Spark 2.0.2 on Windows

Unzip spark-2.0.2-bin-hadoop2.7.tgz

Add the spark bin directory to Path : ...\spark-2.0.2-bin-hadoop2.7\bin

### Set up winutils.exe on Windows (not needed on mac)

- download winutils.exe from https://github.com/steveloughran/winutils/tree/master/hadoop-2.6.0/bin
- move it to c:\hadoop\bin
- set HADOOP_HOME in your environment variables
    - HADOOP_HOME = C:\hadoop
- run from command prompt:
    - C:\hadoop\bin\winutils.exe chmod 777 /tmp/hive
- run spark-shell from command prompt with extra conf parameter
    - spark-shell --driver-memory 2G --executor-memory 3G --executor-cores 2 -conf spark.sql.warehouse.dir=file:///c:/tmp/spark-warehouse


### Running spark-shell

- export PATH=$PATH:/Users/path_to_downloaded_spark/spark-2.0.2-bin-hadoop2.7/bin
- spark-shell

### Paste in spark-shell

scala> :paste

## IntelliJ

The code can be run either in spark-shell or IntelliJ

- Install IntelliJ from https://www.jetbrains.com/idea/download/
- Add the scala language plugin
- Import the code as a maven project

## Scala IDE for Eclipse

- If using Eclipse, do use Scala IDE for Eclipse available at : http://scala-ide.org/download/sdk.html

## Summary of Downloads needed

Have the following downloaded before the session
- Spark binaries
- JDK installed (> 1.7.x)
- https://github.com/WhiteFangBuck/strata-2016-singapore


## Git

Nice to have





