# Spark Examples

Experiments with [Apache Spark](http://spark.apache.org).

## Prerequisites

### Download StackOverflow's users file

- Download `stackoverflow.com-Users.7z` from [Stack Exchange Data Dump](https://archive.org/details/stackexchange);
- Download `stackoverflow.com-Badges.7z` from [Stack Exchange Data Dump](https://archive.org/details/stackexchange);
- uncompress them to `/tmp/spark`.

### Clone this project and package it with sbt-assembly

  git clone https://github.com/fedragon/spark-examples
  cd spark-examples
  sbt assembly

### Run it

  <your_spark_home>/bin/spark-submit --master spark://<master>:<port> --class sparking.GetUsers <spark_examples_folder>/target/scala-2.10/spark-examples-assembly-1.0.0.jar
