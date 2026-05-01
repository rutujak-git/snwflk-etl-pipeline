-- ================================================================
-- EXTEND SILVER/GOLD LAYERS: Process JSON & CSV data
-- Loads bronze_employee_data_json and bronze_employee_absense
-- into the unified silver_employee_data_parquet table
-- ================================================================

use role accountadmin;
use warehouse compute_wh;
use database etl_pipeline_db;

-- ================================================================
-- SILVER LAYER: Stored Procedure to load JSON source into Silver
-- JSON has engagement_survey VARIANT → parse scores like Parquet
-- ================================================================

create or replace procedure etl_pipeline_db.etl_pipeline_silver_sch.load_silver_from_json(etl_run_id string)
returns string
language sql
as
$$
begin
    insert into etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet (
        employee_number, employee_name, gender, city, job_title, department,
        store_location, business_unit, division, age, length_of_service, hours_absent,
        engagement_survey, satisfaction_score, work_life_balance_score,
        career_growth_score, communication_score, teamwork_score,
        staged_at, source_object, etl_run_id
    )
    select
        try_to_number(employee_number),
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
        try_to_number(to_varchar(parse_json(engagement_survey):satisfaction_score))::int,
        try_to_number(to_varchar(parse_json(engagement_survey):work_life_balance_score))::int,
        try_to_number(to_varchar(parse_json(engagement_survey):career_growth_score))::int,
        try_to_number(to_varchar(parse_json(engagement_survey):communication_score))::int,
        try_to_number(to_varchar(parse_json(engagement_survey):teamwork_score))::int,
        current_timestamp(),
        'etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_json',
        :etl_run_id
    from etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_json;

    return 'Loaded ' || sqlrowcount || ' rows from JSON into silver with etl_run_id=' || :etl_run_id;
end;
$$;

-- ================================================================
-- SILVER LAYER: Stored Procedure to load CSV source into Silver
-- CSV has NO engagement_survey → survey scores are NULL
-- ================================================================

create or replace procedure etl_pipeline_db.etl_pipeline_silver_sch.load_silver_from_csv(etl_run_id string)
returns string
language sql
as
$$
begin
    insert into etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet (
        employee_number, employee_name, gender, city, job_title, department,
        store_location, business_unit, division, age, length_of_service, hours_absent,
        engagement_survey, satisfaction_score, work_life_balance_score,
        career_growth_score, communication_score, teamwork_score,
        staged_at, source_object, etl_run_id
    )
    select
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
        null,
        null,
        null,
        null,
        null,
        null,
        current_timestamp(),
        'etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_absense',
        :etl_run_id
    from etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_absense;

    return 'Loaded ' || sqlrowcount || ' rows from CSV into silver with etl_run_id=' || :etl_run_id;
end;
$$;

-- ================================================================
-- MASTER SILVER LOAD: Load all sources into silver
-- ================================================================

create or replace procedure etl_pipeline_db.etl_pipeline_silver_sch.load_all_sources_to_silver()
returns string
language sql
as
$$
declare
    run_id string default uuid_string();
    parquet_result string;
    json_result string;
    csv_result string;
begin
    call etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet_sp(:run_id) into parquet_result;
    call etl_pipeline_db.etl_pipeline_silver_sch.load_silver_from_json(:run_id) into json_result;
    call etl_pipeline_db.etl_pipeline_silver_sch.load_silver_from_csv(:run_id) into csv_result;

    return '=== SILVER LOAD SUMMARY ===\n' ||
           'Run ID: ' || :run_id || '\n' ||
           'Parquet: ' || :parquet_result || '\n' ||
           'JSON: ' || :json_result || '\n' ||
           'CSV: ' || :csv_result;
end;
$$;

-- ================================================================
-- EXECUTE: Load all sources
-- ================================================================

truncate table etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet;

call etl_pipeline_db.etl_pipeline_silver_sch.load_all_sources_to_silver();

-- Verify: Check row counts by source
select
    source_object,
    count(*) as row_count
from etl_pipeline_db.etl_pipeline_silver_sch.silver_employee_data_parquet
group by source_object
order by source_object;

-- ================================================================
-- GOLD LAYER: Rebuild with all sources now in silver
-- Demographics will include CSV rows; Survey will include JSON rows
-- (CSV rows have NULL survey scores → excluded by NULLIFZERO in gold)
-- ================================================================

call etl_pipeline_db.etl_pipeline_gold_sch.materialize_core_models();

-- Verify gold results
select * from etl_pipeline_db.etl_pipeline_gold_sch.employee_demographics_by_department;
select * from etl_pipeline_db.etl_pipeline_gold_sch.survey_results_by_department;
