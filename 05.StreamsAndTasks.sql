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
create or replace procedure etl_pipeline_bronze_sch.bronze_emp_parquet_stream(etl_run_id string)
returns string
language sql
as
$$
begin
    merge into etl_pipeline_silver_sch.silver_employee_data_parquet as target
    using (
        select 
        EMPLOYEE_NUMBER, 
        EMPLOYEE_NAME, 
        GENDER, 
        CITY, 
        JOB_TITLE, 
        DEPARTMENT, 
        STORE_LOCATION, 
        BUSINESS_UNIT, 
        DIVISION, 
        AGE, 
        LENGTH_OF_SERVICE, 
        HOURS_ABSENT, 
        ENGAGEMENT_SURVEY,
        try_to_number(to_varchar(parse_json(ENGAGEMENT_SURVEY):satisfaction_score))::int as satisfaction_score,
        try_to_number(to_varchar(parse_json(ENGAGEMENT_SURVEY):work_life_balance_score))::int as work_life_balance_score,
        try_to_number(to_varchar(parse_json(ENGAGEMENT_SURVEY):career_growth_score))::int as career_growth_score,
        try_to_number(to_varchar(parse_json(ENGAGEMENT_SURVEY):communication_score))::int as communication_score,
        try_to_number(to_varchar(parse_json(ENGAGEMENT_SURVEY):teamwork_score))::int as teamwork_score,
        metadata$action,
        metadata$isupdate
    from etl_pipeline_bronze_sch.bronze_emp_parquet_stream
    ) as source
    on target.employee_number = source.employee_number

    when matched and source.metadata$action = 'DELETE' then DELETE
    when matched and source.metadata$action = 'INSERT' and source.metadata$isupdate = TRUE then
        update set       
          employee_name = source.employee_name,
          gender = source.gender,
          city = source.city,
          job_title = source.job_title,
          department = source.department,
          store_location = source.store_location,
          business_unit = source.business_unit,
          division = source.division,
          age = source.age,
          length_of_service = source.length_of_service,
          hours_absent = source.hours_absent,
          engagement_survey = source.engagement_survey,
          satisfaction_score = source.satisfaction_score,
          work_life_balance_score = source.work_life_balance_score,
          career_growth_score = source.career_growth_score,
          communication_score = source.communication_score,
          teamwork_score = source.teamwork_score,
          staged_at = CURRENT_TIMESTAMP(),
          source_object = 'bronze.employee_data_parquet_stream',
          etl_run_id = :etl_run_id

    when not matched and source.metadata$action = 'INSERT' then
        insert (
          employee_number,
          employee_name,
          gender,
          city,
          job_title,
          department,
          store_location,
          business_unit,
          division,
          age,
          length_of_service,
          hours_absent,
          engagement_survey,
          satisfaction_score,
          work_life_balance_score,
          career_growth_score,
          communication_score,
          teamwork_score,
          staged_at,
          source_object,
          etl_run_id
        )
        values (
          source.employee_number,
          source.employee_name,
          source.gender,
          source.city,
          source.job_title,
          source.department,
          source.store_location,
          source.business_unit,
          source.division,
          source.age,
          source.length_of_service,
          source.hours_absent,
          source.engagement_survey,
          source.satisfaction_score,
          source.work_life_balance_score,
          source.career_growth_score,
          source.communication_score,
          source.teamwork_score,
          CURRENT_TIMESTAMP(),
          'bronze.employee_data_parquet_stream',
          :etl_run_id
        );

        RETURN 'Processed stream records (INSERT/UPDATE/DELETE) into etl_pipeline_silver_sch.silver_employee_data_parquet with etl_run_id=' || :etl_run_id || '. Rows affected: ' || SQLROWCOUNT;
end;
$$;

-- ================================================================
-- TASKS: Incremental load into silver, then materialize gold
-- ================================================================

alter task if exists etl_pipeline_tasks_sch.t_gold_refresh_after_silver suspend;
alter task if exists etl_pipeline_tasks_sch.t_silver_from_bronze_stream suspend;

-- Create/replace the silver load task (calls stored procedure for clean encapsulation)
create or replace task etl_pipeline_tasks_sch.t_silver_from_bronze_stream
    warehouse = compute_wh
    schedule = 'USING CRON 0 6 * * * UTC'
    when system$stream_has_data('etl_pipeline_bronze_sch.bronze_emp_parquet_stream')
as 
call etl_pipeline_bronze_sch.bronze_emp_parquet_stream(uuid_string());

-- Create/replace the gold refresh task (runs after silver task)
create or replace task etl_pipeline_tasks_sch.t_gold_refresh_after_silver
    warehouse = compute_wh
    after etl_pipeline_tasks_sch.t_silver_from_bronze_stream
as 
call etl_pipeline_gold_sch.materialize_core_models();

-- Verify tasks
SHOW TASKS LIKE 'T_SILVER_FROM_BRONZE_STREAM' IN SCHEMA etl_pipeline_tasks_sch;
SHOW TASKS LIKE 'T_GOLD_REFRESH_AFTER_SILVER' IN SCHEMA etl_pipeline_tasks_sch;

-- ================================================================
-- DEMO RUN SEQUENCE
-- 1) Reset bronze to simulate a fresh load, then re-upload parquet files to @raw_zone/parquet/ and refresh the pipe
-- 2) Resume tasks and optionally execute on-demand
-- 3) Inspect silver and gold results and task history
-- ===============================================================

-- re-upload the files
/* remove them if rerunning the demo
REMOVE @bronze.raw_zone/parquet/employee_data_part_01.parquet;
REMOVE @bronze.raw_zone/parquet/employee_data_part_02.parquet;
REMOVE @bronze.raw_zone/parquet/employee_data_part_03.parquet;
REMOVE @bronze.raw_zone/parquet/employee_data_part_04.parquet;
REMOVE @bronze.raw_zone/parquet/employee_data_part_05.parquet;
*/

list @etl_pipeline_bronze_sch.bronze_stg;

-- Modify the pipe to refer to the schema
CREATE OR REPLACE PIPE etl_pipeline_bronze_sch.bronze_employee_parquet_pipe
  AUTO_INGEST = FALSE  -- Set to FALSE for manual demo control
  AS
  COPY INTO etl_pipeline_bronze_sch.bronze_employee_data_parquet
  FROM @etl_pipeline_bronze_sch.bronze_stg
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FILE_FORMAT = (TYPE = 'PARQUET')
  PATTERN = '.*employee_data_part_.*[.]parquet';

-- Get pipe details 
SELECT SYSTEM$PIPE_STATUS('etl_pipeline_bronze_sch.bronze_employee_parquet_pipe');

alter pipe etl_pipeline_bronze_sch.bronze_employee_parquet_pipe refresh;

-- Monitor Snowpipe execution history
-- Check what files have been loaded
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'etl_pipeline_bronze_sch.bronze_employee_data_parquet',
    START_TIME => DATEADD(MINUTE, -10, CURRENT_TIMESTAMP())
));

select * from etl_pipeline_bronze_sch.bronze_employee_data_parquet;

SELECT SYSTEM$STREAM_HAS_DATA('etl_pipeline_bronze_sch.bronze_emp_parquet_stream') AS stream_has_data;

-- Step 2: Resume tasks and optionally trigger an immediate run
ALTER TASK etl_pipeline_tasks_sch.t_gold_refresh_after_silver RESUME;
ALTER TASK etl_pipeline_tasks_sch.t_silver_from_bronze_stream RESUME;

-- Manually execute (for demo control); tasks also run on their schedule
EXECUTE TASK etl_pipeline_tasks_sch.t_silver_from_bronze_stream;

-- Step 3: Inspect results
select * from etl_pipeline_silver_sch.silver_employee_data_parquet;

select * from etl_pipeline_gold_sch.employee_demographics_by_department;

select * from etl_pipeline_gold_sch.survey_results_by_department;

ALTER TASK etl_pipeline_tasks_sch.t_silver_from_bronze_stream suspend;
ALTER TASK etl_pipeline_tasks_sch.t_gold_refresh_after_silver suspend;



