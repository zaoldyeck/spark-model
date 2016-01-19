#domain=hadoop@ec2-52-192-81-204.ap-northeast-1.compute.amazonaws.com
domain=hadoop@ec2-52-193-126-70.ap-northeast-1.compute.amazonaws.com
sbt package
scp -i ~/pubgame.pem -P 22 -pr ./target/scala-2.10/spark-model_2.10-1.0.jar ${domain}:
#ssh -i ~/pubgame.pem ${domain} \
#'
#screen
#spark-submit --driver-memory 2G \
#              --total-executor-cores 20 \
#              --num-executors 5 \
#              --executor-cores 4 \
#              --executor-memory 5G \
#              --class Main ./spark-model_2.10-1.0.jar \
#              --driver-java-options "-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremotrmi.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=ec2-52-193-124-210.ap-northeast-1.compute.amazonaws.com"
#
#'
#exit
