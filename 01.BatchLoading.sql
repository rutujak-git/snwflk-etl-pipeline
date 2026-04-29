---------------------------- Initial Settings --------------------------------------------
use role sysadmin;
use warehouse etl_pipeline_wh;
use database etl_pipeline_db;

---------------------------- Create Bronze Schema -----------------------------------------
create schema if not exists etl_pipeline_db.etl_pipeline_bronze_sch;
use schema etl_pipeline_db.etl_pipeline_bronze_sch;

---------------------------- Create Internal Stage to upload batch data from UI -----------
create or replace stage etl_pipeline_db.etl_pipeline_bronze_sch.bronze_stg;

list @etl_pipeline_db.etl_pipeline_bronze_sch.bronze_stg;

----------------------------- Bronze Tables Creation ------------------------------------
create or replace table etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_absense
(
    employee_number INT,
    employee_name VARCHAR(255),
    gender VARCHAR(10),
    city VARCHAR(100),
    job_title VARCHAR(100),
    department VARCHAR(100),
    store_location VARCHAR(100),
    business_unit VARCHAR(100),
    division VARCHAR(100),
    age INT,
    length_of_service INT,
    hours_absent INT
) comment= 'this is csv table structure for bronze layer to load raw batch data files';

create or replace table etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_json
(
    employee_number VARCHAR(50),
    employee_name VARCHAR(255),
    gender VARCHAR(50),
    city VARCHAR(255),
    job_title VARCHAR(255),
    department VARCHAR(255),
    store_location VARCHAR(255),
    business_unit VARCHAR(255),
    division VARCHAR(255),
    age INTEGER,
    length_of_service INTEGER,
    hours_absent INTEGER,
    engagement_survey VARIANT
) comment= 'this is json table structure for bronze layer to load raw batch data files';

create or replace table etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_data_parquet
(
    employee_number BIGINT,
    employee_name VARCHAR(255),
    gender VARCHAR(50),
    city VARCHAR(255),
    job_title VARCHAR(255),
    department VARCHAR(255),
    store_location VARCHAR(255),
    business_unit VARCHAR(255),
    division VARCHAR(255),
    age BIGINT,
    length_of_service BIGINT,
    hours_absent BIGINT,
    engagement_survey VARIANT
) comment= 'this is parquet table structure for bronze layer to load raw batch data files';


----------------------- Data Loading in the tables --------------------------------------
copy into etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_absense
from @etl_pipeline_db.etl_pipeline_bronze_sch.bronze_stg
pattern = '.*/employee_.*ence[.]csv'  ----employee***_absence.csv
file_format = (type='csv' field_delimiter=',' skip_header=1);

select * from etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_absense;
select count(*) from etl_pipeline_db.etl_pipeline_bronze_sch.bronze_employee_absense;