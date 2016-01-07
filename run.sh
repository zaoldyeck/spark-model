#!/usr/bin/env bash

git pull
#sbt clean package
sbt package
spark-submit --driver-java-options "-Dcom.sun.management.jmxremote.port=12345 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote" --queue longrun --class Main target/scala-2.11/spark-model_2.11-1.0.jar