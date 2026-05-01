-- Monitoring ELT Activity with Streams and Tasks Integration
-- Demonstrates comprehensive logging integrated with the real streams/tasks pipeline from Module 3
-- Uses existing stored procedures from Module 2 with logging wrappers

use warehouse compute_wh;
use database etl_pipeline_db;

-- ================================================================
-- EVENT TABLE and LOGGING INFRASTRUCTURE
-- ================================================================
-- Create operations schema for monitoring
create schema if not exists etl_pipeline_ops_sch;

-- Create event table to capture custom pipeline logs
create or replace event table etl_pipeline_ops_sch.pipeline_events;

-- Associate event table with database for automatic telemetry collection
alter database etl_pipeline_db set event_table = etl_pipeline_db.etl_pipeline_ops_sch.pipeline_events;

-- Enable comprehensive logging and tracing
ALTER DATABASE etl_pipeline_db SET LOG_LEVEL = 'INFO';
ALTER DATABASE etl_pipeline_db SET TRACE_LEVEL = 'ALWAYS';

-- Step 1: Reset tables (Snowpipe will repopulate after files are re-uploaded)
TRUNCATE TABLE etl_pipeline_bronze_sch.bronze_employee_data_parquet;
TRUNCATE TABLE etl_pipeline_silver_sch.silver_employee_data_parquet;
TRUNCATE TABLE etl_pipeline_gold_sch.employee_demographics_by_department;
TRUNCATE TABLE etl_pipeline_gold_sch.survey_results_by_department;

-- ================================================================
-- STREAM: Track new rows arriving into bronze
-- ================================================================
create or replace stream etl_pipeline_bronze_sch.bronze_emp_parquet_stream
on table etl_pipeline_bronze_sch.bronze_employee_data_parquet;

-- Inspect stream status
SELECT SYSTEM$STREAM_HAS_DATA('etl_pipeline_bronze_sch.bronze_emp_parquet_stream') AS stream_has_data;

-- ================================================================
-- LOGGED WRAPPER PROCEDURES: wrappers around existing procedures
-- ================================================================
CREATE OR REPLACE PROCEDURE etl_pipeline_silver_sch.load_employee_staging_logged(etl_run_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  result_msg STRING;
  start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
  -- Log pipeline stage start
  SYSTEM$LOG('INFO', OBJECT_CONSTRUCT(
    'component', 'ELT_Pipeline',
    'run_id', :etl_run_id,
    'pipeline', 'streams_tasks_elt',
    'stage', 'bronze_stg',
    'status', 'STARTED',
    'source', 'etl_pipeline_bronze_sch.bronze_employee_data_parquet',
    'target', 'etl_pipeline_silver_sch.silver_employee_data_parquet',
    'procedure', 'etl_pipeline_silver_sch.silver_employee_data_parquet_sp'
  ));

  CALL etl_pipeline_silver_sch.silver_employee_data_parquet_sp(:etl_run_id) INTO :result_msg;

  SYSTEM$LOG('INFO', OBJECT_CONSTRUCT(
    'component', 'ELT_Pipeline',
    'run_id', :etl_run_id,
    'pipeline', 'streams_tasks_elt',
    'stage', 'bronze_stg',
    'status', 'SUCCESS',
    'duration_seconds', DATEDIFF(SECOND, :start_time, CURRENT_TIMESTAMP()),
    'result', :result_msg,
    'procedure', 'etl_pipeline_silver_sch.silver_employee_data_parquet_sp'
  ));

  RETURN :result_msg;

  EXCEPTION
  WHEN OTHER THEN

    SYSTEM$LOG('ERROR', OBJECT_CONSTRUCT(
      'component', 'ELT_Pipeline',
      'run_id', :etl_run_id,
      'pipeline', 'streams_tasks_elt',
      'stage', 'bronze_stg',
      'status', 'FAILED',
      'error_code', :SQLCODE,
      'error_message', :SQLERRM,
      'procedure', 'etl_pipeline_silver_sch.silver_employee_data_parquet_sp'
    ));
    
    RETURN 'ERROR in silver load: ' || :SQLERRM;
END;
$$;

-- Logged wrapper for the existing gold materialization procedure
CREATE OR REPLACE PROCEDURE etl_pipeline_gold_sch.materialize_core_models_logged(etl_run_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  result_msg STRING;
  start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
  SYSTEM$LOG('INFO', OBJECT_CONSTRUCT(
    'component', 'ELT_Pipeline',
    'run_id', :etl_run_id,
    'pipeline', 'streams_tasks_elt',
    'stage', 'gold_materialize',
    'status', 'STARTED',
    'source', 'etl_pipeline_silver_sch.silver_employee_data_parquet',
    'target', 'etl_pipeline_gold_sch.employee_demographics_by_department,etl_pipeline_gold_sch.survey_results_by_department',
    'procedure', 'etl_pipeline_gold_sch.materialize_core_models'
  ));

  CALL etl_pipeline_gold_sch.materialize_core_models() INTO :result_msg;

  SYSTEM$LOG('INFO', OBJECT_CONSTRUCT(
    'component', 'ELT_Pipeline',
    'run_id', :etl_run_id,
    'pipeline', 'streams_tasks_elt',
    'stage', 'gold_materialize',
    'status', 'SUCCESS',
    'duration_seconds', DATEDIFF(SECOND, :start_time, CURRENT_TIMESTAMP()),
    'result', :result_msg,
    'procedure', 'etl_pipeline_gold_sch.materialize_core_models'
  ));

  RETURN :result_msg;

EXCEPTION
  WHEN OTHER THEN
    SYSTEM$LOG('ERROR', OBJECT_CONSTRUCT(
      'component', 'ELT_Pipeline',
      'run_id', :etl_run_id,
      'pipeline', 'streams_tasks_elt',
      'stage', 'gold_materialize',
      'status', 'FAILED',
      'error_code', :SQLCODE,
      'error_message', :SQLERRM,
      'procedure', 'etl_pipeline_gold_sch.materialize_core_models'
    ));
    
    RETURN 'ERROR in gold materialization: ' || :SQLERRM;
END;
$$;

-- ================================================================
-- ENHANCED TASKS: Modify existing tasks from Module 3 to use logged procedures
-- ================================================================

use schema etl_pipeline_tasks_sch;

alter task if exists etl_pipeline_tasks_sch.t_silver_from_bronze_stream suspend;
alter task etl_pipeline_tasks_sch.t_silver_from_bronze_stream set log_level = INFO;

create or replace task etl_pipeline_tasks_sch.t_silver_from_bronze_stream 
    warehouse = compute_wh
    schedule = 'USING CRON 0 6 * * * UTC'
    when system$stream_has_data('ETL_PIPELINE_DB.ETL_PIPELINE_BRONZE_SCH.BRONZE_EMPLOYEE_DATA_PARQUET')
as
declare
     run_id STRING DEFAULT UUID_STRING();
     task_result STRING;
BEGIN
  -- Call the logged wrapper procedure
  CALL etl_pipeline_silver_sch.load_employee_staging_logged(:run_id) into :task_result;
END;

-- Update the existing gold task to use logged procedure and add task-level logging
ALTER TASK IF EXISTS etl_pipeline_tasks_sch.t_gold_refresh_after_silver SUSPEND;
ALTER TASK etl_pipeline_tasks_sch.t_gold_refresh_after_silver SET LOG_LEVEL = INFO;

CREATE OR REPLACE TASK etl_pipeline_tasks_sch.t_gold_refresh_after_silver
  WAREHOUSE = COMPUTE_WH
  AFTER etl_pipeline_tasks_sch.t_silver_from_bronze_stream
AS
DECLARE
  run_id STRING DEFAULT UUID_STRING();
  task_result STRING;
BEGIN
  -- Call the logged wrapper procedure
  CALL etl_pipeline_gold_sch.materialize_core_models_logged(:run_id) INTO :task_result;
END;

-- ================================================================
-- DEMO EXECUTION: Run the enhanced pipeline with logging
-- ================================================================

-- Modify the pipe
CREATE OR REPLACE PIPE ETL_PIPELINE_DB.ETL_PIPELINE_BRONZE_SCH.BRONZE_EMPLOYEE_PARQUET_PIPE
  AUTO_INGEST = FALSE
  AS
  COPY INTO ETL_PIPELINE_DB.ETL_PIPELINE_BRONZE_SCH.BRONZE_EMPLOYEE_DATA_PARQUET
  FROM @ETL_PIPELINE_DB.ETL_PIPELINE_BRONZE_SCH.BRONZE_STG
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FILE_FORMAT = (TYPE = 'PARQUET')
  PATTERN = '.*employee_data_part_.*[.]parquet';

ALTER PIPE ETL_PIPELINE_DB.ETL_PIPELINE_BRONZE_SCH.BRONZE_EMPLOYEE_PARQUET_PIPE REFRESH;

SELECT * FROM ETL_PIPELINE_DB.ETL_PIPELINE_BRONZE_SCH.BRONZE_EMPLOYEE_DATA_PARQUET;

SELECT SYSTEM$STREAM_HAS_DATA('ETL_PIPELINE_DB.ETL_PIPELINE_BRONZE_SCH.BRONZE_EMP_PARQUET_STREAM') AS stream_has_data;

-- Step 2: Resume and execute tasks
ALTER TASK ETL_PIPELINE_DB.ETL_PIPELINE_TASKS_SCH.T_GOLD_REFRESH_AFTER_SILVER RESUME;
ALTER TASK ETL_PIPELINE_DB.ETL_PIPELINE_TASKS_SCH.T_SILVER_FROM_BRONZE_STREAM RESUME;

-- Manually execute for demo purposes
EXECUTE TASK ETL_PIPELINE_DB.ETL_PIPELINE_TASKS_SCH.T_SILVER_FROM_BRONZE_STREAM;

-- ================================================================
-- EVENT TABLE MONITORING: Query the built-in event table
-- ================================================================

-- View all recent pipeline events
select * from etl_pipeline_db.etl_pipeline_ops_sch.pipeline_events
where record_type in ('EVENT','LOG','INFO')
order by timestamp desc limit 50;

-- View structured pipeline events with extracted JSON data
select 
  timestamp,
  scope:name::string as scope_name,
  parse_json(value):run_id::string as run_id,
  parse_json(value):pipeline::string as pipeline,
  parse_json(value):stage::string as stage,
  parse_json(value):event::string as event,
  parse_json(value):status::string as status,
  parse_json(value):task_name::string as task_name,
  parse_json(value):duration_seconds::number as duration_seconds,
  parse_json(value):source::string as source_object,
  parse_json(value):target::string as target_object,
  parse_json(value):error_message::string as error_message
from etl_pipeline_db.etl_pipeline_ops_sch.pipeline_events
where record_type = 'LOG' 
  and parse_json(value):pipeline::string = 'streams_tasks_elt'
order by timestamp desc 
limit 20;

-- Pipeline execution summary from event table (last 7 days)
select 
  parse_json(value):stage::string as pipeline_stage,
  parse_json(value):status::string as status,
  count(*) as execution_count,
  avg(parse_json(value):duration_seconds::number) as avg_duration_seconds
from etl_pipeline_db.etl_pipeline_ops_sch.pipeline_events
where record_type = 'LOG' 
  and parse_json(value):pipeline::string = 'streams_tasks_elt'
  and parse_json(value):status::string is not null
  and timestamp >= dateadd(day, -7, current_timestamp())
group by parse_json(value):stage::string, parse_json(value):status::string
order by pipeline_stage, status;

-- ================================================================
-- SYSTEM VIEWS: Task and Load History
-- ================================================================

-- Recent task executions (last 7 days)
select * from table(etl_pipeline_db.information_schema.task_history(
  scheduled_time_range_start => dateadd(day, -7, current_timestamp()),
  result_limit => 1000
)) order by scheduled_time desc;

-- Recent loads into bronze (COPY or Snowpipe history - last 7 days)
select * from table(etl_pipeline_db.information_schema.copy_history(
  table_name => 'ETL_PIPELINE_DB.ETL_PIPELINE_BRONZE_SCH.BRONZE_EMPLOYEE_DATA_PARQUET',
  start_time => dateadd(day, -7, current_timestamp())
)) order by last_load_time desc;

-- ================================================================
-- DYNAMIC TABLES MONITORING: Comprehensive monitoring for Dynamic Tables pipeline
-- Based on Snowflake documentation: https://docs.snowflake.com/en/user-guide/dynamic-tables-monitor
-- ================================================================

-- Monitor: Comprehensive status and lag metrics for our dynamic tables
select
  name,
  database_name,
  schema_name,
  scheduling_state,
  target_lag_type,
  target_lag_sec,
  last_completed_refresh_state,
  last_completed_refresh_state_code,
  last_completed_refresh_state_message,
  latest_data_timestamp,
  time_within_target_lag_ratio,
  maximum_lag_sec,
  executing_refresh_query_id
from table(etl_pipeline_db.information_schema.dynamic_tables())
where database_name = 'ETL_PIPELINE_DB'
  and name like '%_DT'
order by schema_name, name;

-- Monitor: View refresh history for our dynamic tables (last 7 days)
select
  name,
  data_timestamp,
  state,
  state_code,
  state_message,
  refresh_action,
  query_id,
  statistics
from table(etl_pipeline_db.information_schema.dynamic_table_refresh_history(
  data_timestamp_start => dateadd(day, -6, current_timestamp())
))
where name like '%_DT'
order by data_timestamp desc, name;

-- Monitor: Performance summary - credits and efficiency
select
  name,
  count(*) as total_refreshes,
  sum(case when state = 'SUCCEEDED' then 1 else 0 end) as successful_refreshes,
  sum(case when state = 'FAILED' then 1 else 0 end) as failed_refreshes,
  sum(statistics:numInsertedRows::number) as total_rows_inserted,
  sum(statistics:numDeletedRows::number) as total_rows_deleted,
  sum(statistics:numCopiedRows::number) as total_rows_copied
from table(etl_pipeline_db.information_schema.dynamic_table_refresh_history(
  data_timestamp_start => dateadd(day, -1, current_timestamp())
))
where name like '%_DT'
group by name
order by name;

-- ================================================================
-- CLEANUP: Pause tasks after demo
-- ================================================================

-- Pause the enhanced tasks to prevent unwanted executions
alter task etl_pipeline_db.etl_pipeline_tasks_sch.t_silver_from_bronze_stream suspend;
alter task etl_pipeline_db.etl_pipeline_tasks_sch.t_gold_refresh_after_silver suspend;

