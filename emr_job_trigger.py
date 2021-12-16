import airflow
import boto3
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults

DEFAULT_ARGS = {    
    'owner': 'name',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['email@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': None
}       

class TimeSleepSensor(BaseSensorOperator):
    """
    Waits for specified time interval relative to task instance start
    :param sleep_duration: time after which the job succeeds
    :type sleep_duration: datetime.timedelta
    """

    @apply_defaults
    def __init__(self, sleep_duration, *args, **kwargs):
        super(TimeSleepSensor, self).__init__(*args, **kwargs)
        self.sleep_duration = sleep_duration
        self.poke_interval = kwargs.get('poke_interval',int(sleep_duration.total_seconds()))
        self.timeout = kwargs.get('timeout',int(sleep_duration.total_seconds()) + 30)

    def poke(self, context):
        ti = context["ti"]

        sensor_task_start_date = ti.start_date
        target_time = sensor_task_start_date + self.sleep_duration

        self.log.info("Checking if the target time ({} - check:{} has come - time to go: {}, start {}, initial sleep_duration: {}"
                    .format(target_time, (timezone.utcnow() > target_time), (target_time-timezone.utcnow()), sensor_task_start_date, self.sleep_duration)
        )

        return timezone.utcnow() > target_time

SPARK_STEPS = [
    {
        'Name': 'pdo_midterm_project',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                '--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                '--num-executors', '2',
                '--driver-memory', '512m',
                '--executor-memory', '3g',
                '--executor-cores', '2',
                's3://{% INPUT BUCKET DIR CONTAINING JAR},
                '-p', 'wcd-parser',
                '-i', "{{task_instance.xcom_pull('source_file_type',key='fileType')}}",
                '-o', 'parquet',
                '-s', "{{ task_instance.xcom_pull('parse_request', key='s3location') }}",
                '-d',"{{task_instance.xcom_pull('source_file_type',key='outputBucket')}}",
                '-c',"{{task_instance.xcom_pull('source_file_type',key='partitionColumn')}}",
                '-m', 'append',
                '--input-options', 'header=true'
            ]
        }
    }
]

dag = DAG(
    'emr_job_flow_manual_steps_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=2),
    schedule_interval=None,
    is_paused_upon_creation=False
)

def get_file_type(**kwargs):
    
    s3_location=kwargs['dag_run'].conf['s3_location'] # Specification of file type Xcom level
    if s3_location.endswith('.csv'):
        kwargs['ti'].xcom_push(key='fileType',value='Csv')
        kwargs['ti'].xcom_push(key='partitionColumn',value='job')
        kwargs['ti'].xcom_push(key='outputBucket',value='s3://{%INPUT BUCKET NAME}')
    elif s3_location.endswith('.json'):
        kwargs['ti'].xcom_push(key='fileType',value='Json')
        kwargs['ti'].xcom_push(key='partitionColumn',value='name')
        kwargs['ti'].xcom_push(key='outputBucket',value='s3://{%INPUT BUCKET NAME}') 
    else:
        print("File type is not supported")
    

source_file_type=PythonOperator(task_id="source_file_type",
                            provide_context=True,
                            python_callable=get_file_type,
                            dag=dag
                            )


JOB_FLOW_OVERRIDES = {
    "Name": "mid_term_emr_cluster",
    "ReleaseLabel": "emr-6.4.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"},{"Name": "Hive"}], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                 }
        },
        {
            "Classification": "spark-hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                 }
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "ON_DEMAND", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "Ec2KeyName":"pdo-midterm-project",
        "Ec2SubnetId":"subnet-2aa55866",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # This lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

create_emr_cluster = EmrCreateJobFlowOperator( # Create an EMR cluster
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

sleep_task = TimeSleepSensor(
                    task_id="sleep_task",
                    sleep_duration=timedelta(minutes=8),  
                    mode='reschedule',
                    dag=dag
)

def retrieve_s3_files(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location']
    kwargs['ti'].xcom_push(key='s3location', value = s3_location
)

def stop_airflow_ec2():     #Stop Airflow EC2 instance if there is no other EMR clusters/SPARK jobs running
    emr = boto3.client("emr","us-east-1")
    status_check = emr.list_clusters(
    ClusterStates=[
        'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'
        ]
    )
    clusters = status_check['Clusters']
    emr_list = []
    for status in clusters:
	    emr_list.append(status['Status']['State'])


    if len(emr_list) == 0:
        ec2 = boto3.client('ec2', 'us-east-1')
        ec2.stop_instances(InstanceIds=['i-{% INPUT EC2 INTERFACE ID(AIRFLOW)'])
        
    else:
        print("Add retry timer in future")

parse_request = PythonOperator(task_id = 'parse_request',
                              provide_context = True,
                              python_callable = retrieve_s3_files,
                              dag = dag
)

step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)

step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id = "{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id = "aws_default",
    dag = dag
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag
)

terminate_ec2 = PythonOperator(
    task_id='stop_airflow_ec2',
    provide_context=False,
    python_callable=stop_airflow_ec2,
    dag=dag
)

create_emr_cluster.set_upstream(parse_request)
sleep_task.set_upstream(create_emr_cluster)
step_adder.set_upstream(sleep_task)
step_checker.set_upstream(step_adder)
terminate_emr_cluster.set_upstream(step_checker)
terminate_ec2.set_upstream(terminate_emr_cluster)