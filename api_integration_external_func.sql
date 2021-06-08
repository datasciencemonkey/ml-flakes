-- All of the objects used the account admin role
use role ACCOUNTADMIN;
use database sgdb;

-- Best Reference - https://docs.snowflake.com/en/sql-reference/external-functions-creating-aws.html

create or replace api integration aws_endpoint_integration_01
api_provider = aws_api_gateway
api_aws_role_arn = 'arn:aws:iam::133045618979:role/snowflake_role'
api_allowed_prefixes = ('https://b64nvsnp7j.execute-api.us-east-1.amazonaws.com/test/snowflake-proxy')
enabled = true;


describe api integration aws_endpoint_integration_01;

describe table abalone_test;

--   Creating the external function

create or replace external function score_abalone_age_function(sex varchar, length number,
diameter number, height number, whole_weight number, 
shucked_weight number, viscera_weight number, shell_weight number)
returns variant
api_integration = aws_endpoint_integration_01
as 'https://b64nvsnp7j.execute-api.us-east-1.amazonaws.com/test/snowflake-proxy';
    

select score_abalone_age_function(sex, length, diameter, height, 
whole_weight, shucked_weight, viscera_weight, shell_weight)  as age 
from abalone_test limit 5;