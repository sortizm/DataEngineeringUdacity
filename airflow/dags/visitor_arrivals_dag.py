from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator, StageCSVToRedshiftOperator)
from helpers import SqlQueries

SOURCE_S3_BUCKET = Variable.get('source_s3_bucket')

default_args = {
    'owner': 'Steve Ortiz',
    'start_date': datetime(2020, 6, 6),
    'email_on_retry': False,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

dag = DAG('visitor_arrivals_dag',
          default_args=default_args,
          description='Load and transform Visitor Arrivals data into dimensional model',
          schedule_interval=None,
          catchup=False,
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_world_happiness_to_redshift = StageCSVToRedshiftOperator(
    task_id='Stage_world_happiness',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_world_happiness',
    s3_bucket=SOURCE_S3_BUCKET,
    s3_key='world_happiness.csv',
)

stage_us_cities_to_redshift = StageCSVToRedshiftOperator(
    task_id='Stage_us_cities',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_us_cities',
    s3_bucket=SOURCE_S3_BUCKET,
    s3_key='us-cities-demographics.csv',
    delimiter=';',
)

stage_airport_codes_to_redshift = StageCSVToRedshiftOperator(
    task_id='Stage_airport_codes',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_airport_codes',
    s3_bucket=SOURCE_S3_BUCKET,
    s3_key='airport_codes.csv',
    delimiter=';',
)

stage_us_states_mapping_to_redshift = StageCSVToRedshiftOperator(
    task_id='Stage_us_states_mapping',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_us_states_mapping',
    s3_bucket=SOURCE_S3_BUCKET,
    s3_key='visitor_arrivals_mappings/i94addrl.csv',
    ignore_headers=0,
    delimiter=';'
)

stage_countries_mapping_to_redshift = StageCSVToRedshiftOperator(
    task_id='Stage_countries_mapping',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_countries_mapping',
    s3_bucket=SOURCE_S3_BUCKET,
    s3_key='visitor_arrivals_mappings/i94cntyl.csv',
    ignore_headers=0,
    delimiter=';'
)

stage_travel_mode_mapping_to_redshift = StageCSVToRedshiftOperator(
    task_id='Stage_travel_mode_mapping',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_travel_mode_mapping',
    s3_bucket=SOURCE_S3_BUCKET,
    s3_key='visitor_arrivals_mappings/i94model.csv',
    ignore_headers=0,
    delimiter=';'
)

stage_us_ports_mapping_to_redshift = StageCSVToRedshiftOperator(
    task_id='Stage_us_ports_mapping',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_us_ports_mapping',
    s3_bucket=SOURCE_S3_BUCKET,
    s3_key='visitor_arrivals_mappings/i94prtl.csv',
    ignore_headers=0,
    delimiter=';'
)

stage_visa_mapping_to_redshift = StageCSVToRedshiftOperator(
    task_id='Stage_visa_mapping',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_visa_mapping',
    s3_bucket=SOURCE_S3_BUCKET,
    s3_key='visitor_arrivals_mappings/i94visa.csv',
    ignore_headers=0,
    delimiter=';'
)

stage_visitor_arrivals_to_redshift = StageToRedshiftOperator(
    task_id='Stage_visa_mapping',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_visa_mapping',
    s3_bucket=SOURCE_S3_BUCKET,
    s3_key='visitor_arrivals'
)

staged_operator = DummyOperator(task_id='All_staged', dag=dag)

load_port_dimension_table = LoadDimensionOperator(
    task_id='Load_dim_port_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_port',
    select_query=SqlQueries.port_table_insert,
    truncate_insert=True
)

load_us_city_dimension_table = LoadDimensionOperator(
    task_id='Load_dim_us_city_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_us_city',
    select_query=SqlQueries.us_city_table_insert,
    truncate_insert=True
)

load_us_state_dimension_table = LoadDimensionOperator(
    task_id='Load_dim_us_state_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_us_state',
    select_query=SqlQueries.us_state_table_insert,
    truncate_insert=True
)

load_country_dimension_table = LoadDimensionOperator(
    task_id='Load_dim_country_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_country',
    select_query=SqlQueries.country_table_insert,
    truncate_insert=True
)

load_date_dimension_table = LoadDimensionOperator(
    task_id='Load_dim_date_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_date',
    select_query=SqlQueries.date_table_insert,
    truncate_insert=True
)

load_visitor_arrival_fact_table = LoadFactOperator(
    task_id='Load_fact_visitor_arrival_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='fact_visitor_arrival',
    select_query=SqlQueries.visitor_arrival_table_insert,
    truncate_insert=True
)

run_has_rows_quality_checks = DataQualityOperator(
    task_id='Run_has_rows_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    checks=[
        {"test": "SELECT COUNT(*) FROM dim_port", "not_equals": "0"},
        {"test": "SELECT COUNT(*) FROM dim_us_city", "not_equals": "0"},
        {"test": "SELECT COUNT(*) FROM dim_us_state", "not_equals": "0"},
        {"test": "SELECT COUNT(*) FROM dim_country", "not_equals": "0"},
        {"test": "SELECT COUNT(*) FROM dim_date", "not_equals": "0"},
        {"test": "SELECT COUNT(*) FROM fact_visitor_arrival", "not_equals": "0"}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Execution order
start_operator >> [
    stage_airport_codes_to_redshift,
    stage_countries_mapping_to_redshift,
    stage_travel_mode_mapping_to_redshift,
    stage_us_cities_to_redshift,
    stage_us_ports_mapping_to_redshift,
    stage_us_states_mapping_to_redshift,
    stage_visa_mapping_to_redshift,
    stage_world_happiness_to_redshift,
] >> staged_operator

staged_operator >> [
    load_country_dimension_table,
    load_date_dimension_table,
    load_us_city_dimension_table,
] >> load_visitor_arrival_fact_table << load_port_dimension_table

load_us_city_dimension_table >> load_us_state_dimension_table >> load_port_dimension_table

load_visitor_arrival_fact_table >> run_has_rows_quality_checks

run_has_rows_quality_checks >> end_operator

