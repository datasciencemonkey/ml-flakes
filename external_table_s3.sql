-- All of the objects used the account admin role
use role ACCOUNTADMIN;
use database demo_db;
use schema public;

-- Create an integration object for storage

create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::133045618979:role/snowflk_int_extstage'
  storage_allowed_locations = ('s3://sgdata01/');
  
desc integration s3_int;

-- Create a file format  
create or replace file format demo_db.public.csv_format
type = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true;

desc file format demo_db.public.csv_format;

-- Create a stage (external) using the storage integration and the file format
create or replace stage demo_db.public.s3_stage
storage_integration = s3_int
url = 's3://sgdata01/'
file_format = demo_db.public.csv_format;

list @s3_stage;

-- query directly from s3

select t.$1 as sex,t.$2 length,t.$3 diameter
from @demo_db.public.s3_stage as t limit 5;

-- external table def
-- https://docs.snowflake.com/en/user-guide/tables-external-intro.html#querying-external-tables
-- https://docs.snowflake.com/en/sql-reference/sql/create-external-table.html

create or replace external table ext_abalone_test
  with location = @demo_db.public.s3_stage
  auto_refresh = true
  file_format = demo_db.public.csv_format
  pattern='.*abalone.*[.]csv';

alter external table ext_abalone_test refresh;

select metadata$filename from ext_abalone_test limit 1;


desc table ext_abalone_test;

desc table sgdb.public.abalone_test;

select * from ext_abalone_test limit 2;


select 
value:c1::string as sex,
value:c2::NUMBER(5,4) as length,
value:c3::NUMBER(5,4) as diameter,
value:c4::NUMBER(5,4) as height,
value:c5::NUMBER(5,4) as whole_weight,
value:c6::NUMBER(5,4) as shucked_weight,
value:c7::NUMBER(5,4) as viscera_weight,
value:c8::NUMBER(5,4) as shell_weight,
value:c9::NUMBER(38,4) as rings
from ext_abalone_test limit 5;

-- create a materialized view
create or replace materialized view abalone_test_mat_view
    comment='abalone data on s3'
    as
    select 
value:c1::string as sex,
value:c2::NUMBER(5,4) as length,
value:c3::NUMBER(5,4) as diameter,
value:c4::NUMBER(5,4) as height,
value:c5::NUMBER(5,4) as whole_weight,
value:c6::NUMBER(5,4) as shucked_weight,
value:c7::NUMBER(5,4) as viscera_weight,
value:c8::NUMBER(5,4) as shell_weight,
value:c9::NUMBER(38,4) as rings
from ext_abalone_test;

select sex, length, diameter, height from ABALONE_TEST_MAT_VIEW limit 5;

--  score using external function
select sgdb.public.score_abalone_age_function(sex, length, diameter, height, whole_weight, shucked_weight, viscera_weight, shell_weight)  as age
from demo_db.public.ABALONE_TEST_MAT_VIEW limit 50;


