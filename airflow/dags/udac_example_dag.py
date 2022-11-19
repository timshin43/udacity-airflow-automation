from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# /opt/airflow/start.sh
default_args = {
    'owner': 'udacity',
#     'start_date': datetime.now(),
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 12),
    'catchup': False
}

dag = DAG(
    'airflow_project_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/",
    table="public.staging_events"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/B/B/",
    table="public.staging_songs"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmnt=SqlQueries.songplay_table_insert
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    insert_flag="append",
    table="users",
    sql_stmnt=SqlQueries.user_table_insert
)


load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    insert_flag="append",
    table="songs",
    redshift_conn_id="redshift",
    sql_stmnt=SqlQueries.song_table_insert
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    insert_flag="truncate_insert",
    table="artists",
    redshift_conn_id="redshift",
    sql_stmnt=SqlQueries.artist_table_insert   
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    insert_flag="append",
    table="time",
    redshift_conn_id="redshift",
    sql_stmnt=SqlQueries.time_table_insert   
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'test_sql': "SELECT COUNT(*) FROM songs", 'expected_result': 1},
        {'test_sql': "SELECT COUNT(*) FROM artists", 'expected_result': 1},
        {'test_sql': "SELECT COUNT(*) FROM users", 'expected_result': 1},
        {'test_sql': "SELECT COUNT(*) FROM time", 'expected_result': 1},
        {'test_sql': "SELECT COUNT(*) FROM songplays", 'expected_result': 1}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_songs_to_redshift,stage_events_to_redshift]
[stage_songs_to_redshift,stage_events_to_redshift] >> load_songplays_table
load_songplays_table >> [load_users_dimension_table, load_artists_dimension_table, load_songs_dimension_table, load_time_dimension_table]
#checks
[load_users_dimension_table,load_artists_dimension_table,load_songs_dimension_table,load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator







