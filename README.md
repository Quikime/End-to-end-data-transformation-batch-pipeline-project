# End to end data transformation batch pipeline project

![pipeline](https://user-images.githubusercontent.com/88790752/146460695-4d0e28f5-03af-4148-a195-9572668ea8be.jpg)

Efficient data transformation from json, csv and other file formats to parquet for further data analytics and cloud based storage.

-- Data inputs into S3 bucket trigger Lambda function spinning EC2 machine with preconfigured Airflow framework for data flow orchestration running in Docker container. 

-- Airflow DAG starts EMR cluster

-- Spark SBT project submit call starts/schedules all incoming batch data for transformation in EMR(Spark) cluster.

-- Transformed data parquet files stored in S3 bucket

-- Additional Lambda function activates Glue crawler inferring parquet data into Athena for querying and further analytics

-- Data analysis is done in Apache Superset spinned in Docker container

-- All active machines to be stopped as soon as transformation jobs are complete to minimize final cloud cost

_________________________________________________________________________________________________________________________________________________________________

Example DAG steps

![1 DAGs List](https://user-images.githubusercontent.com/88790752/160277820-147a3203-7d17-412c-87e6-4554a709df9e.jpg)

Example Superset Chart

![2 Charts](https://user-images.githubusercontent.com/88790752/160277827-6074fc10-e1b0-4fa2-bca1-9b2864f5fd13.jpg)

_________________________________________________________________________________________________________________________________________________________________

Following bootstrap settings required to start Docker service at Linux OS boot

1. SSH into EC2 Apache Airflow instance. Create .system file under home/etc/systemd/system/
2. Use vi to open .system file and copy/paste:

[Unit]
Description=docker boot
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/ec2-user/docker-airflow
ExecStart=/usr/bin/docker-compose -f /home/ec2-user/docker-airflow -f docker-compose-CeleryExecutor.yml up -d --remove-orphans

[Install]
WantedBy=multi-user.target
