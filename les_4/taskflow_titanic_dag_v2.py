import os
import datetime as dt
import pandas as pd
from airflow.models import DAG
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='titanic_taskflow_dag_v2',
        schedule_interval=None,
        default_args=args,
) as dag:
    @task
    def download_dataset():
        url = "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"
        file_path = os.path.join(os.path.expanduser('~'), 'titanic.csv')
        df = pd.read_csv(url)
        df.to_csv(file_path, encoding='utf-8')
        return file_path


    copy_titanic_dataset = BashOperator(
        task_id='copy_titanic_dataset_to_hdfs',
        bash_command='''hdfs dfs -mkdir -p /datasets/ && \
        hdfs dfs -put /home/hduser/titanic.csv /datasets/ '''
    )

    with TaskGroup("prepare_table") as prepare_table:
        @task
        def drop_table_hive():
            hive_hook = HiveCliHook()
            hive_hook.run_cli(hql='DROP TABLE titanic;')


        @task
        def create_hive_table():
            hive_hook = HiveCliHook()
            hive_hook.run_cli(
                hql='''
                CREATE TABLE IF NOT EXISTS titanic (
                    ID  INT,
                    Survived INT,
                    Pclass INT,
                    Name STRING,
                    Sex STRING,
                    Age INT,
                    Sibsp INT,
                    Parch INT,
                    Fare DOUBLE)
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                LINES TERMINATED BY '\n'
                STORED AS TEXTFILE
                TBLPROPERTIES('skip.header.line.count'='1');'''
            )


        drop_table_hive() >> create_hive_table()


    @task
    def load_dataset_in_hive():
        hive_hook = HiveCliHook()
        hive_hook.run_cli(
            hql='''LOAD DATA INPATH "/datasets/titanic.csv" INTO TABLE titanic;'''
        )


    @task
    def titanic_dataset_avg_fare():
        hive_hook = HiveCliHook()
        hive_hook.run_cli(
            hql='''SELECT Pclass, avg(Fare) FROM titanic GROUP BY Pclass;'''
        )


    @task
    def pivot_dataset(file_path):
        titanic_df = pd.read_csv(file_path)
        df = titanic_df.pivot_table(index=['Pclass'],
                                    values='Fare',
                                    aggfunc='mean').reset_index()
        return df.to_json(os.path.join(os.path.expanduser('~'), 'titanic_avg_fare.json'))


    send_result_telegram = TelegramOperator(
        task_id='send_success_message_telegram',
        telegram_conn_id='telegram_conn',
        chat_id='921496014',
        text='''Pipeline {{ execution_date.int_timestamp }} is done''',
    )
    pivot_dataset(download_dataset()) >> copy_titanic_dataset >> prepare_table >> load_dataset_in_hive() >> titanic_dataset_avg_fare() >> send_result_telegram
