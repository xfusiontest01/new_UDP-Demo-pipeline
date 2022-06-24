from datetime import datetime
from airflow import DAG
from udp_base_operator import UDPBaseOperator
from udp_datablend_operator import UDPDatablendOperator

with DAG(dag_id="UDP-Demo-pipeline",schedule_interval="0 * * * *",start_date=datetime(2020,11,1),concurrency=16,max_active_runs=16,dagrun_timeout=None,orientation="LR",catchup=True,is_paused_upon_creation=False) as dag:

    udp1 = UDPBaseOperator(conn_id="spark_conn",total_executor_cores=None,executor_cores=None,name="arrow-spark",num_executors=None,status_poll_interval=1,verbose=False,task_id="udp1",email_on_retry=False,email_on_failure=False,retries=0,retry_exponential_backoff=False,depends_on_past=False,wait_for_downstream=False,priority_weight=1,pool_slots=1,task_concurrency=None,do_xcom_push=True)

    udp2 = UDPDatablendOperator(task_id="udp2",email_on_retry=False,email_on_failure=False,retries=0,retry_exponential_backoff=False,depends_on_past=False,wait_for_downstream=False,priority_weight=1,pool_slots=1,task_concurrency=None,do_xcom_push=True,conn_id="spark_conn",total_executor_cores=None,executor_cores=None,name="arrow-spark",num_executors=None,status_poll_interval=1,verbose=False)

    udp1 >> udp2
