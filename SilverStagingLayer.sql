use role sysadmin;
use database etl_pipeline_db;

create schema if not exists etl_pipeline_db.etl_pipeline_silver_sch;
use schema etl_pipeline_db.etl_pipeline_silver_sch;

show tables in schema etl_pipeline_bronze_sch;

create or replace table etl_pipeline_db