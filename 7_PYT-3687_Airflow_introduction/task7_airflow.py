import os
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.datasets import Dataset
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo import MongoClient


file_path = f'{os.getcwd()}/airflow/dags/files'
in_data_path = f'{file_path}/tiktok_google_play_reviews.csv'
out_data_path = f'{file_path}/out3.csv'


def file_check(**kwargs):
    file_size = os.path.getsize(in_data_path)
    file_exists = os.path.exists(in_data_path)
    return 'file_empty_task' if (file_size == 0 or not file_exists) else 'data_process.replace_null_task'


def replace_null_values(**kwargs):
    df = pd.read_csv(in_data_path)
    df.fillna('-')
    df.to_csv(f'{file_path}/out1.csv')
    return df


def sort_by_at_date(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='data_process.replace_null_task')
    df.sort_values(by='at', inplace=True)
    df.to_csv(f'{file_path}/out2.csv')
    return df


def clean_content_column(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='data_process.sort_data_task')
    df['content'] = df['content'].str.replace('[^a-zA-Z0-9\s]', '', regex=True)
    df.to_csv(f'{file_path}/out3.csv')
    return df


# @dag(dag_id='dag7', start_date=datetime.datetime(2021, 1, 1), schedule='@daily')
with DAG(
    dag_id='dag7_etl',
    description='Airflow DAG for file import and transformation and export to MongoDB',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 2, 5),
        'schedule': '@daily',
    },
) as dag:

    start = EmptyOperator(
        task_id='start_task',
        dag=dag,
    )

    file_sensor = FileSensor(
        task_id='file_sensor_task',
        filepath=in_data_path,
        timeout=10,
        poke_interval=5,
    )

    file_check_branch = BranchPythonOperator(
        task_id='file_check_task',
        python_callable=file_check,
        trigger_rule='all_done',
        dag=dag,
    )

    file_empty = BashOperator(
        task_id='file_empty_task',
        bash_command='echo "Empty file found!" >> /home/user/airflow/files/file.log',
        trigger_rule='all_done',
    )

    with TaskGroup(group_id='data_process') as data_process:
        replace_null = PythonOperator(
            task_id='replace_null_task',
            python_callable=replace_null_values,
            dag=dag,
        )
        sort_data = PythonOperator(
            task_id='sort_data_task',
            python_callable=sort_by_at_date,
            dag=dag,
        )
        clean_content = PythonOperator(
            task_id='clean_content_task',
            python_callable=clean_content_column,
            dag=dag,
            outlets=[Dataset(out_data_path)],
        )
        replace_null >> sort_data >> clean_content

    trigger_mongodb = TriggerDagRunOperator(
        task_id='trigger_mongodb_task',
        trigger_dag_id='dag7_mongodb',
        dag=dag,
    )

    start >> file_sensor >> file_check_branch >> [file_empty, data_process]
    data_process >> trigger_mongodb


with DAG(
    'dag7_mongodb',
    description='Airflow DAG for export to MongoDB',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 2, 5),
    },
    schedule=[Dataset(out_data_path)],
) as dag:

    def load_to_mongodb(**kwargs):
        # ti = kwargs['ti']
        # df = ti.xcom_pull(task_ids='data_process.sort_data_task')
        df2 = pd.read_csv(out_data_path)
        hook = MongoHook(mongo_conn_id='mongo_default')
        client = hook.get_conn()
        db = client.test
        collection = db.task7
        collection.insert_many(df2.to_dict(orient='records'))

    load_to_mongodb = PythonOperator(
        task_id='load_to_mongodb_task',
        python_callable=load_to_mongodb,
        dag=dag,
    )

    load_to_mongodb


# MONGODB QUERIES
#
# 1. Топ-5 часто встречаемых комментариев
# db.task7.aggregate([
#     {"$group" : {_id : "$content",
#                  count : {$sum : 1}}},
#     {$sort : {"count" : -1}},
#     {$limit : 5}
# ])
# 2. Все записи, где длина поля “content” составляет менее 5 символов
# db.task7.find({
#     $expr: {$lt : [{ $strLenCP : "$content"}, 5]}
# })
# 3. Средний рейтинг по каждому дню (результат должен быть в виде timestamp type)
# db.task7.aggregate([
#     {$group : {_id : {$dateToString : {format: "%Y-%m-%d",
#                                        date: {$dateFromString:
#                                              {dateString: "$at", format: "%Y-%m-%d %H:%M:%S" }}}},
#                averageRating : {$avg : "$score"}
#               }
#     }
# ])