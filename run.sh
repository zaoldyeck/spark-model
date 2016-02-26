#!/usr/bin/env bash

#git pull
#sbt clean package
#sbt package
#spark-submit --driver-memory 20G \
#             --num-executors 5 \
#             --executor-cores 4 \
#             --executor-memory 20G \
#             --class Main target/scala-2.10/spark-model_2.10-1.0.jar \
#             --driver-java-options "-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremotrmi.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=ec2-52-193-123-80.ap-northeast-1.compute.amazonaws.com"
#spark-submit --driver-java-options "-Dcom.sun.management.jmxremote.port=12345 -Dcom.sun.management.jmxremotrmi.port=12344 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=ec2-54-65-230-104.ap-northeast-1.compute.amazonaws.com" --class Main target/scala-2.11/spark-model_2.11-1.0.jar

domain=hadoop@ec2-52-192-213-179.ap-northeast-1.compute.amazonaws.com #pubgame
ssh -i ~/pubgame.pem ${domain} \
'
screen
spark-submit  --driver-memory 2G \
              --total-executor-cores 120 \
              --num-executors 15 \
              --executor-cores 8 \
              --executor-memory 10G \
              --class Main ./spark-model_2.10-1.0.jar \
              --driver-java-options "-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremotrmi.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=ec2-52-193-124-210.ap-northeast-1.compute.amazonaws.com"

'
exit

domain=vincent@ec2-54-65-230-104.ap-northeast-1.compute.amazonaws.com
ssh ${domain}
nohup spark-submit --class Main ./spark-model_2.11-1.0.jar 2>log &
spark-submit z --class Main ./spark-model_2.11-1.0.jar
nohup spark-submit --queue longrun --class Main ./spark-model_2.11-1.0.jar 2>log &