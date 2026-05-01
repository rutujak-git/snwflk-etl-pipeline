-- Environment Setup
use database etl_pipeline_db;

create schema if not exists etl_pipeline_db.etl_pipeline_gold_sch;
use schema etl_pipeline_db.etl_pipeline_gold_sch;

-- Verify silver staging table exists and has data
show tables in schema etl_pipeline_db.etl_pipeline_silver_sch;
select count(*) as silver_row_count 
    from etl_pipeline_silver_sch.silver_employee_data_parquet;
-- ==================================================================
-- APPROACH 1: CTE-based transformation
-- Create demographics by department table using a CTE
-- ================================================================
create or replace table employee_demographics_by_department as
with employees as(
    select department,
           age::number as age,
           length_of_service::number as length_of_service,
           gender
    from etl_pipeline_silver_sch.silver_employee_data_parquet
)
select department,
       count(*) as num_employees,
       avg(age) as avg_age,
       avg(length_of_service) as avg_length_of_service,
       sum(iff(upper(gender)='MALE',1,0)) as num_male,
       sum(iff(upper(gender)='FEMALE',1,0)) as num_female,
       sum(iff(upper(gender) not in ('MALE','FEMALE'),1,0)) as num_other_gender,
       -- Metadata columns for data lineage and auditing
       current_timestamp() as materialized_at,
       'etl_pipeline_silver_sch.silver_employee_data_parquet' as source_object,
       uuid_string() as etl_run_id
from employees
group by department
order by department;

-- Inspect results
select * from employee_demographics_by_department;

-- ================================================================
-- APPROACH 2: Temp table + final table
-- Create survey results by department using a temp table step
-- ================================================================
create or replace temp table tmp_survey_results_by_department as
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
    'etl_pipeline_silver_sch.silver_employee_data_parquet' as source_object,
    uuid_string() as etl_run_id
from etl_pipeline_silver_sch.silver_employee_data_parquet
group by department;

create or replace table survey_results_by_department as
select * from tmp_survey_results_by_department;

-- Inspect results
select * from survey_results_by_department;

-- ================================================================
-- APPROACH 3: View instead of table
-- Provide a dynamic view over the same aggregation
-- ================================================================
create or replace view v_survey_results_by_department as
select department,
        avg(nullifzero(satisfaction_score)) as avg_satisfaction_score,
        avg(nullifzero(work_life_balance_score)) as avg_work_life_balance_score,
        avg(nullifzero(career_growth_score)) as avg_career_growth_score,
        avg(nullifzero(communication_score)) as avg_communication_score,
        avg(nullifzero(teamwork_score)) as avg_teamwork_score,
        count(*) as num_responses,
        -- Metadata columns for data lineage and auditing
        current_timestamp() as materialized_at,
        'etl_pipeline_silver_sch.silver_employee_data_parquet' as source_object,
        uuid_string() as etl_run_id
from etl_pipeline_silver_sch.silver_employee_data_parquet
group by department;

-- Inspect the view
select * from v_survey_results_by_department;

-- ================================================================
-- MODULAR STORED PROCEDURES: Each table gets its own procedure
-- This approach allows for better maintainability and scalability
-- ================================================================
-- Procedure 1: Build employee_demographics_by_department table
create or replace procedure etl_pipeline_gold_sch.build_employee_demographics_by_department()
returns string
language sql
as
$$
declare
    start_time timestamp := current_timestamp();
    row_count integer;
    result_message string;
begin
    begin
        create or replace table employee_demographics_by_department as
        with employees as(
            select department,
                   age::number as age,
                   length_of_service::number as length_of_service,
                   gender
            from etl_pipeline_silver_sch.silver_employee_data_parquet
        )
        select department,
               count(*) as num_employees,
               avg(age) as avg_age,
               avg(length_of_service) as avg_length_of_service,
               sum(iff(upper(gender)='MALE',1,0)) as num_male,
               sum(iff(upper(gender)='FEMALE',1,0)) as num_female,
               sum(iff(upper(gender) not in ('MALE','FEMALE'),1,0)) as num_other_gender,
               -- Metadata columns for data lineage and auditing
               current_timestamp() as materialized_at,
               'etl_pipeline_silver_sch.silver_employee_data_parquet' as source_object,
               uuid_string() as etl_run_id
        from employees
        group by department
        order by department;

        -- Get row count for logging
        select count(*) into row_count from etl_pipeline_gold_sch.employee_demographics_by_department;

        result_message := 'SUCCESS: Built employee_demographics_by_department with ' || row_count || ' rows in ' || DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) || ' seconds';

        RETURN result_message;

        exception
            when other then
            result_message := 'Failed to build employee_demographics_by_department - ' || SQLERRM;
            RETURN result_message;
    end;
end;
$$;

-- Procedure 2: Build survey_results_by_department table
create or replace procedure etl_pipeline_gold_sch.build_survey_results_by_department()
returns string
language sql
as
$$
declare
    start_time timestamp :=current_timestamp();
    row_count integer;
    result_message string;
begin
    begin
        create or replace temp table tmp_survey_results_by_department as
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
            'etl_pipeline_silver_sch.silver_employee_data_parquet' as source_object,
            uuid_string() as etl_run_id
        from etl_pipeline_silver_sch.silver_employee_data_parquet
        group by department;
 
        create or replace table etl_pipeline_gold_sch.survey_results_by_department as
        select * from tmp_survey_results_by_department;

        select count(*) into row_count from etl_pipeline_gold_sch.survey_results_by_department;

        result_message := 'SUCCESS: Built survey_results_by_department with ' || 
                     row_count || ' rows in ' || 
                     DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) || ' seconds';
    
        RETURN result_message;
        
    EXCEPTION
    WHEN OTHER THEN
      result_message := 'ERROR: Failed to build survey_results_by_department - ' || SQLERRM;
      RETURN result_message;
    end;
end;
$$;

-- ================================================================
-- MASTER ORCHESTRATION PROCEDURE: Calls individual table procedures
-- This provides a single entry point while maintaining modularity
-- ================================================================

create or replace procedure etl_pipeline_gold_sch.materialize_core_models()
returns string
language sql
as
$$
DECLARE
  overall_start_time TIMESTAMP := CURRENT_TIMESTAMP();
  demographics_result STRING;
  survey_result STRING;
  final_result STRING;
  error_count INTEGER := 0;

BEGIN

  CALL etl_pipeline_gold_sch.build_employee_demographics_by_department() INTO demographics_result;
  CALL etl_pipeline_gold_sch.build_survey_results_by_department() INTO survey_result;

  IF (demographics_result LIKE 'ERROR:%') THEN
    error_count := error_count + 1;
  END IF;
  
  IF (survey_result LIKE 'ERROR:%') THEN
    error_count := error_count + 1;
  END IF;

 final_result := '=== GOLD LAYER BUILD SUMMARY ===\n' ||
                 'Total execution time: ' || DATEDIFF('second', overall_start_time, CURRENT_TIMESTAMP()) || ' seconds\n' ||
                 'Errors encountered: ' || error_count || '\n\n' ||
                 'DEMOGRAPHICS TABLE: ' || demographics_result || '\n' ||
                 'SURVEY TABLE: ' || survey_result;
  
  RETURN final_result;
END;
$$;

-- ================================================================
-- USAGE EXAMPLES AND BENEFITS OF MODULAR APPROACH
-- ================================================================
-- Option 1: Run individual table builds (useful for debugging or selective rebuilds)
CALL etl_pipeline_gold_sch.build_employee_demographics_by_department();
CALL etl_pipeline_gold_sch.build_survey_results_by_department();

-- Option 2: Run all tables via master procedure (recommended for production)
CALL etl_pipeline_gold_sch.materialize_core_models();

-- Final checks - verify both tables were built successfully
SELECT COUNT(*) AS demographics_rows FROM employee_demographics_by_department;
SELECT COUNT(*) AS survey_rows FROM survey_results_by_department;