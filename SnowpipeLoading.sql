use role sysadmin;
use database etl_pipeline_db;
use schema etl_pipeline_bronze_sch;

show stages like 'BRONZE_STG';
show tables like 'bronze_employee_data_parquet';

truncate table bronze_employee_data_parquet;

CREATE OR REPLACE PIPE etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_parquet_pipe
AUTO_INGEST = FALSE
AS 
COPY INTO bronze_employee_data_parquet
FROM @bronze_stg
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE='PARQUET')
PATTERN = '.*employee_data_part_.*[.]parquet';

show pipes;

select system$pipe_status('bronze_employee_parquet_pipe');

alter pipe bronze_employee_parquet_pipe refresh;

select system$pipe_status('bronze_employee_parquet_pipe');

select * from table(information_schema.copy_history(
table_name => 'bronze_employee_data_parquet',
start_time => dateadd(hour,-1,current_timestamp())
));

select * from bronze_employee_data_parquet;

alter pipe bronze_employee_parquet_pipe set pipe_execution_paused = TRUE;
