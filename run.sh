#!/usr/bin/env bash

git pull
#sbt clean package
sbt package
spark-submit --class Main target/scala-2.11/spark-model_2.11-1.0.jar