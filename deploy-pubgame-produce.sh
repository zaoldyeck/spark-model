domain=vincent@ec2-54-65-230-104.ap-northeast-1.compute.amazonaws.com #pubgame
sbt package
scp -P 22 -pr ./target/scala-2.11/spark-model_2.11-1.0.jar ${domain}:produce