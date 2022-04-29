import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from pathlib import Path

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'owner_name',
    'start_date': datetime(2022, 4, 27),
    "depends_on_past": False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
#           schedule_interval='0 * * * *',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_param="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_param="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="songplays",
    select_sql=SqlQueries.songplay_table_insert, 
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="users",
    select_sql=SqlQueries.user_table_insert, 
    append_row=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="songs",
    select_sql=SqlQueries.song_table_insert,
    append_row=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="artists",
    select_sql=SqlQueries.artist_table_insert,
    append_row=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="time",
    select_sql=SqlQueries.time_table_insert,
    append_row=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table="users",
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift 
start_operator >> stage_songs_to_redshift


stage_events_to_redshift  >> load_songplays_table 
stage_songs_to_redshift  >> load_songplays_table 


load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table 
load_songplays_table >> load_artist_dimension_table 
load_songplays_table >> load_time_dimension_table


load_song_dimension_table >> run_quality_checks 
load_user_dimension_table >> run_quality_checks 
load_artist_dimension_table >> run_quality_checks 
load_time_dimension_table >> run_quality_checks 


run_quality_checks >> end_operator


