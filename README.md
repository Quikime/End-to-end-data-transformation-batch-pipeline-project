# End to end data transformation batch pipeline project

![pipeline](https://user-images.githubusercontent.com/88790752/146460695-4d0e28f5-03af-4148-a195-9572668ea8be.jpg)

Additional settings are required to start Docker service at Linux OS boot. 

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
