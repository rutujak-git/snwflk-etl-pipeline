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

