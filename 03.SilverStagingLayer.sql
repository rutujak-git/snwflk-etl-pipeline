use role accountadmin;
use database etl_pipeline_db;

create schema if not exists etl_pipeline_db.etl_pipeline_silver_sch;
use schema etl_pipeline_db.etl_pipeline_silver_sch;

show tables in schema etl_pipeline_bronze_sch;


-- ================================================================
-- STAGING TABLE CREATION
-- Create a staging table using the bronze table structure as a base
-- Then add parsed engagement_survey fields and metadata columns
-- ================================================================

create or replace table etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet 
    like etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_parquet;

alter table etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet add column satisfaction_score integer;
alter table etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet add column work_life_balance_score integer;
alter table etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet add column career_growth_score integer;
alter table etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet add column communication_score integer;
alter table etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet add column teamwork_score integer;

alter table etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet add column staged_at timestamp_ntz;
alter table etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet add column source_object string default 'etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_parquet';
alter table etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet add column etl_run_id string;

select * from  etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet;

-- ================================================================
-- STAND-ALONE INSERT INTO ... SELECT
-- Populate staging table by parsing the VARIANT engagement_survey field
-- ================================================================

select * from ETL_PIPELINE_DB.ETL_PIPELINE_BRONZE_SCH.BRONZE_EMPLOYEE_DATA_PARQUET;

insert into etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet (
EMPLOYEE_NUMBER, EMPLOYEE_NAME, GENDER, CITY, JOB_TITLE, DEPARTMENT, STORE_LOCATION, BUSINESS_UNIT, DIVISION, AGE, LENGTH_OF_SERVICE, HOURS_ABSENT, ENGAGEMENT_SURVEY, SATISFACTION_SCORE, WORK_LIFE_BALANCE_SCORE, CAREER_GROWTH_SCORE, COMMUNICATION_SCORE, TEAMWORK_SCORE, STAGED_AT, SOURCE_OBJECT, ETL_RUN_ID
)
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
    current_timestamp(),
    'etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_parquet',
    uuid_string()
from etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_parquet;

select * from ETL_PIPELINE_DB.ETL_PIPELINE_BRONZE_SCH.BRONZE_EMPLOYEE_DATA_PARQUET;

-- ================================================================
-- STORED PROCEDURE: Encapsulate staging load logic
-- Allows re-running the staging step with a single call
-- ================================================================

truncate table etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet;

create or replace procedure etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet_sp(etl_run_id string)
returns string
language sql
as
$$
begin
    insert into etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet (
    EMPLOYEE_NUMBER, EMPLOYEE_NAME, GENDER, CITY, JOB_TITLE, DEPARTMENT, STORE_LOCATION, BUSINESS_UNIT, DIVISION, AGE, LENGTH_OF_SERVICE,   HOURS_ABSENT, ENGAGEMENT_SURVEY, SATISFACTION_SCORE, WORK_LIFE_BALANCE_SCORE, CAREER_GROWTH_SCORE, COMMUNICATION_SCORE, TEAMWORK_SCORE, STAGED_AT, SOURCE_OBJECT, ETL_RUN_ID
)
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
    current_timestamp(),
    'etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_parquet',
    uuid_string()
from etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_parquet;

return 'Loaded' || sqlrowcount || 'rows into etl_pipeline_silver_sch.silver_employee_data_parquet with etl_run_id=' || :etl_run_id;
end;
$$;

-- Call the procedure
call etl_pipeline_silver_sch.silver_employee_data_parquet_sp(uuid_string());

-- Inspect a few rows to verify parsing/metadata
select * from etl_pipeline_silver_sch.silver_employee_data_parquet;