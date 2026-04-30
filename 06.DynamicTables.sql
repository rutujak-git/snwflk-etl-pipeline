-- Incremental MERGE with Dynamic Tables

-- Enviromental Setup
use role accountadmin;
use database etl_pipeline_db;
use warehouse compute_wh;

-- Step 1: Reset tables for demo
truncate table etl_pipeline_bronze_sch.bronze_employee_data_parquet;

-- ================================================================
-- DYNAMIC TABLES: Define refreshable pipeline objects
-- ================================================================

-- Silver-equivalent dynamic table parsing engagement_survey
create or replace dynamic table etl_pipeline_silver_sch.silver_emp_parquet_dt
    target_lag = '5 minutes'
    warehouse = compute_wh
    refresh_mode = auto
as
select EMPLOYEE_NUMBER, 
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
        current_timestamp() as staged_at,
        'etl_pipeline_bronze_sch.bronze_employee_data_parquet' as source_object,
        'dt_refresh' as etl_process_type
from etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_parquet;

-- Gold-equivalent dynamic table: demographics by department (CTE inlined)
create or replace dynamic table etl_pipeline_gold_sch.employee_demographics_by_department_dt
target_lag = '5 minutes'
warehouse = compute_wh
refresh_mode = auto
as
select department,
       count(*) as num_employees,
       avg(age::number) as avg_age,
       avg(length_of_service::number) as avg_length_of_service,
       sum(iff(upper(gender)='MALE',1,0)) as num_male,
       sum(iff(upper(gender)='FEMALE',1,0)) as num_female,
       sum(iff(upper(gender) not in ('MALE','FEMALE'),1,0)) as num_other_gender,
       -- Metadata columns for data lineage and auditing
       current_timestamp() as materialized_at,
       'etl_pipeline_silver_sch.silver_emp_parquet_dt' as source_object,
       'dt_refresh' as et_process_type 
from etl_pipeline_silver_sch.silver_emp_parquet_dt
group by department;

select * from etl_pipeline_silver_sch.silver_emp_parquet_dt;

-- Gold-equivalent dynamic table: survey results by department
create or replace dynamic table etl_pipeline_gold_sch.survey_results_by_department_dt
target_lag = '5 minutes'
warehouse = compute_wh
refresh_mode = auto
as
select
    department,
    avg(nullifzero(satisfaction_score)) as avg_satisfaction_score,
    avg(nullifzero(work_life_balance_score)) as avg_work_life_balance_score,
    avg(nullifzero(career_growth_score)) as avg_career_growth_score,
    avg(nullifzero(communication_score)) as avg_communication_score,
    avg(nullifzero(teamwork_score)) as avg_teamwork_score,
    count(*) as num_responses,
    -- Metadata columns for data lineage and auditing
    current_timestamp() as materialized_at,
    'etl_pipeline_silver_sch.silver_emp_parquet_dt' as source_object,
    'dt_refresh' as et_process_type
from etl_pipeline_silver_sch.silver_employee_data_parquet
group by department;

-- ================================================================
-- DEMO RUN SEQUENCE
-- 1) Truncate bronze, re-upload files and refresh the pipe
-- 2) Manually REFRESH the dynamic tables to show propagation
-- 3) Inspect results
-- ================================================================

-- Recreate the pipe to clear the COPY history
-- Modify the pipe to refer to the schema

CREATE OR REPLACE PIPE etl_pipeline_bronze_sch.bronze_employee_parquet_pipe
AUTO_INGEST = FALSE
AS 
COPY INTO etl_pipeline_bronze_sch.bronze_employee_data_parquet
FROM @etl_pipeline_bronze_sch.bronze_stg
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE='PARQUET')
PATTERN = '.*employee_data_part_.*[.]parquet';

-- Re-upload the files and refresh the pipe (following same pattern as streams demo)
-- LIST @bronze.raw_zone/parquet;
ALTER PIPE etl_pipeline_bronze_sch.bronze_employee_parquet_pipe REFRESH;

-- Wait for data to load
SELECT * FROM etl_pipeline_bronze_sch.bronze_employee_data_parquet;

-- Step 2: Force refresh for demo
ALTER DYNAMIC TABLE etl_pipeline_silver_sch.silver_emp_parquet_dt REFRESH;
ALTER DYNAMIC TABLE etl_pipeline_gold_sch.employee_demographics_by_department_dt REFRESH;
ALTER DYNAMIC TABLE etl_pipeline_gold_sch.survey_results_by_department_dt REFRESH;

select * from etl_pipeline_gold_sch.employee_demographics_by_department_dt;

-- ================================================================
-- DYNAMIC TABLE MONITORING: Track performance and status
-- Based on Snowflake documentation: https://docs.snowflake.com/en/user-guide/dynamic-tables-monitor
-- ================================================================

-- Monitor 1: List all dynamic tables in our demo with basic info
SHOW DYNAMIC TABLES IN DATABASE etl_pipeline_db;

-- Suspend all demo dynamic tables to stop automatic refreshes
ALTER DYNAMIC TABLE etl_pipeline_silver_sch.silver_emp_parquet_dt SUSPEND;
ALTER DYNAMIC TABLE etl_pipeline_gold_sch.employee_demographics_by_department_dt SUSPEND;
ALTER DYNAMIC TABLE etl_pipeline_gold_sch.survey_results_by_department_dt SUSPEND;