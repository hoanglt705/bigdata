sbt clean
sbt package
docker cp target/scala-2.11/bootstraping_2.11-0.1.jar docker-spark_master_1:/tmp