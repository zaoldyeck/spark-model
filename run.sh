#!/usr/bin/env bash

git pull
#sbt clean package
sbt package
spark-submit --driver-memory 20G \
             --num-executors 5 \
             --executor-cores 4 \
             --executor-memory 20G \
             --driver-java-options "-Djava.net.preferIPv4Stack=true
             -Dcom.sun.management.jmxremote.port=9999
             -Dcom.sun.management.jmxremotrmi.port=9999
             -Dcom.sun.management.jmxremote.ssl=false
             -Dcom.sun.management.jmxremote.authenticate=false
             -Djava.rmi.server.hostname=ec2-52-193-123-80.ap-northeast-1.compute.amazonaws.com" \
             --class Main target/scala-2.10/spark-model_2.10-1.0.jar
#spark-submit --driver-java-options "-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremotrmi.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=ec2-54-65-230-104.ap-northeast-1.compute.amazonaws.com" --class Main target/scala-2.11/spark-model_2.11-1.0.jar