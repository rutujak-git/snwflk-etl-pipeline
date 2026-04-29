use role accountadmin;
grant execute task on account to role sysadmin;

use database etl_pipeline_db;
use warehouse compute_wh;

show schemas;

create schema if not exists etl_pipeline_tasks_sch;
use schema etl_pipeline_tasks_sch;

-- Step 1: Reset tables (Snowpipe will repopulate after files are re-uploaded)
truncate table etl_pipeline_bronze_sch.bronze_employee_data_parquet;
truncate table etl_pipeline_silver_sch.silver_employee_data_parquet;
truncate table etl_pipeline_gold_sch.employee_demographics_by_department;
truncate table etl_pipeline_gold_sch.survey_results_by_department;

-- ================================================================
-- STREAM: Track new rows arriving into bronze
-- ================================================================

create or replace stream etl_pipeline_bronze_sch.bronze_emp_parquet_stream
on table etl_pipeline_bronze_sch.bronze_employee_data_parquet;

-- Inspect stream status
show streams in schema etl_pipeline_bronze_sch;
select system$stream_has_data('etl_pipeline_bronze_sch.bronze_emp_parquet_stream') as stream_has_data;

-- ================================================================
-- STREAM-AWARE STORED PROCEDURE: Incremental silver load from stream
-- ================================================================

-- Create a stream-aware version of the silver staging procedure using MERGE
