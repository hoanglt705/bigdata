sbt clean
sbt package
docker cp target/scala-2.11/bootstraping_2.11-0.1.jar docker-spark_master_1:/usr/spark-2.4.1
winpty docker exec -it docker-spark_master_1 sh -c "spark-submit --class 'Unemployment'  --master local[*]  bootstraping_2.11-0.1.jar"