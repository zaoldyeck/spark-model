#!/usr/bin/env bash

git pull
#sbt clean package
sbt package
spark-submit \
   --driver-memory 15G \
  --num-executors 8 --executor-cores 2 --executor-memory 10G \
 --class Main target/scala-2.10/spark-model_2.10-1.0.jar
#spark-submit --driver-java-options "-Dcom.sun.management.jmxremote.port=12345 -Dcom.sun.management.jmxremotrmi.port=12344 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=ec2-54-65-230-104.ap-northeast-1.compute.amazonaws.com" --class Main target/scala-2.11/spark-model_2.11-1.0.jar