domain=hadoop@ec2-52-192-213-179.ap-northeast-1.compute.amazonaws.com
sbt +package
scp -i ~/pubgame.pem -P 22 -pr ./target/scala-2.10/spark-model_2.10-1.0.jar ${domain}: