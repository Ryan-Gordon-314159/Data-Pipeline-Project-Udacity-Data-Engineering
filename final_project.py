from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'start_date': pendulum.now(),
    'query_checks': [
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE song_id is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artist_id is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE userid is null", 'expected_result':0}
    ]
}

@dag(
    dag_id='dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_path='s3://data-pipelines-project-bucket/log_data/',
        json_path='s3://data-pipelines-project-bucket/log_json_path.json/',
        region=REGION,
        truncate=False,
        date_format=f"JSON '{LOG_JSON_PATH}'",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag,
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_path='s3://data-pipelines-project-bucket/song_data/A/A/A/',
        json_path='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='songplays',
        sql=SqlQueries.songplay_table_insert,
        append_only=False
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='users',
        sql=SqlQueries.user_table_insert,
        append_only=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='songs',
        sql=SqlQueries.song_table_insert,
        append_only=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='artists',
        sql=SqlQueries.artist_table_insert,
        append_only=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='time',
        sql=SqlQueries.time_table_insert,
        append_only=False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dag=dag,
        redshift_conn_id='redshift',
        tables=['songplays','users','songs','artists','time'],
        dq_checks = query_checks
        #tables = (songplays, songs, users, artists, time)
    )

    end_operator = DummyOperator(task_id='End_execution', dag=dag)

    # Task dependencies
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()