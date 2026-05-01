-- Environment Setup
use warehouse compute_wh;
use database etl_pipeline_db;
use schema etl_pipeline_ops_sch;

CREATE OR REPLACE TABLE etl_pipeline_ops_sch.data_quality_results (
  check_name STRING,
  layer STRING,
  table_name STRING,
  issue_count NUMBER,
  sample_details VARIANT,
  checked_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ================================================================
-- STORED PROCEDURE: Check for nulls in bronze.employee_number
-- ================================================================

create or replace procedure etl_pipeline_db.etl_pipeline_ops_sch.check_nulls_in_bronze_employee_number()
returns string
language sql
as
$$
declare
  v_issue_count number;
begin
  select count(*) into :v_issue_count
  from etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_parquet
  where employee_number is null;

  insert into etl_pipeline_db.etl_pipeline_ops_sch.data_quality_results (check_name, layer, table_name, issue_count, sample_details)
  select 'null_employee_number', 'bronze', 'etl_pipeline_bronze_sch.bronze_employee_data_parquet', :v_issue_count,
         object_construct('sample_rows', array_agg(object_construct('employee_name', employee_name)))
  from (
    select employee_name
    from etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_parquet
    where employee_number is null
    limit 10
  );

  return 'Null check complete. Issues=' || :v_issue_count;
end;
$$;

-- ================================================================
-- STORED PROCEDURE: Check engagement_survey score ranges in silver
-- Valid range assumed 1..5 for each score
-- ================================================================

create or replace procedure etl_pipeline_db.etl_pipeline_ops_sch.check_silver_survey_ranges()
returns string
language sql
as
$$
declare
  v_issue_count number;
begin
  select count(*) into :v_issue_count
  from etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet
  where coalesce(satisfaction_score, 0) not between 1 and 5
     or coalesce(work_life_balance_score, 0) not between 1 and 5
     or coalesce(career_growth_score, 0) not between 1 and 5
     or coalesce(communication_score, 0) not between 1 and 5
     or coalesce(teamwork_score, 0) not between 1 and 5;

  insert into etl_pipeline_db.etl_pipeline_ops_sch.data_quality_results (check_name, layer, table_name, issue_count, sample_details)
  select 'invalid_survey_range', 'silver', 'etl_pipeline_silver_sch.silver_employee_data_parquet', :v_issue_count,
         object_construct('bad_rows', array_agg(object_construct(
           'employee_number', employee_number,
           'department', department,
           'satisfaction', satisfaction_score,
           'work_life', work_life_balance_score,
           'career', career_growth_score,
           'communication', communication_score,
           'teamwork', teamwork_score,
           'staged_at', staged_at,
           'source_object', source_object,
           'etl_run_id', etl_run_id
         )))
  from (
    select employee_number, department, satisfaction_score, work_life_balance_score,
           career_growth_score, communication_score, teamwork_score,
           staged_at, source_object, etl_run_id
    from etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet
    where coalesce(satisfaction_score, 0) not between 1 and 5
       or coalesce(work_life_balance_score, 0) not between 1 and 5
       or coalesce(career_growth_score, 0) not between 1 and 5
       or coalesce(communication_score, 0) not between 1 and 5
       or coalesce(teamwork_score, 0) not between 1 and 5
    limit 10
  );

  return 'Survey range check complete. Issues=' || :v_issue_count;
end;
$$;

-- ================================================================
-- DEMO FLOW
-- 1) Ensure silver is populated (run staging + gold procs if needed)
-- 2) Intentionally modify some rows to create data quality issues (for demo)
-- 3) Run the procedures and inspect results
-- ================================================================

-- Run checks
call etl_pipeline_db.etl_pipeline_ops_sch.check_nulls_in_bronze_employee_number();
call etl_pipeline_db.etl_pipeline_ops_sch.check_silver_survey_ranges();

select * from etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet;

-- Inspect data quality results
select * from etl_pipeline_db.etl_pipeline_ops_sch.data_quality_results order by checked_at desc limit 50;

-- Intentionally inject a couple of bad values (for demo reset after)
update etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet set satisfaction_score = 99 where employee_number in (select employee_number from etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet limit 1);

update etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_parquet set employee_number = null where employee_number in (select employee_number from etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_parquet limit 1);

-- Run checks
call etl_pipeline_db.etl_pipeline_ops_sch.check_nulls_in_bronze_employee_number();
call etl_pipeline_db.etl_pipeline_ops_sch.check_silver_survey_ranges();

-- Inspect data quality results
select * from etl_pipeline_db.etl_pipeline_ops_sch.data_quality_results order by checked_at desc limit 50;
