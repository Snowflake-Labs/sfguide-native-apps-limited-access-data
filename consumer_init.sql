/*************************************************************************************************************
Script:             Privacy Protected Data - Native App - Consumer Initialization
Create Date:        2023-05-16
Author:             M. Rainey
Description:        Sample Native App -- Consumer side object and data initialization
Copyright © 2023 Snowflake Inc. All rights reserved
***************************************************************************************************************************************************/

/* cleanup consumer */
/*
drop database if exists ppd_demo_app;
drop share if exists ppd_demo_requests;
drop share if exists ppd_demo_signal_inject_share;
drop database if exists ppd_demo_consumer;
*/

/* CONSUMER setup */

use role accountadmin;
call system$wait(3);

/* create warehouse for */
create or replace warehouse ppd_wh;

/* create role and add permissions required by role for installation of consumer side */
create role if not exists ppd_consumer_role;


set myusername = current_user();
grant role ppd_consumer_role to user identifier($myusername);
grant create share on account to role ppd_consumer_role;
grant import share on account to role ppd_consumer_role;
grant create application on account to role ppd_consumer_role;
--show grants to role ppd_consumer_role;
grant create database on account to role ppd_consumer_role;
grant role ppd_consumer_role to role sysadmin;
grant usage, operate on warehouse ppd_wh to role ppd_consumer_role;

use role ppd_consumer_role;
call system$wait(3);

use warehouse ppd_wh;

create or replace database ppd_demo_consumer;
create or replace schema ppd_demo_consumer.mydata;
create or replace schema ppd_demo_consumer.shared;
create or replace schema ppd_demo_consumer.local;

create or replace table ppd_demo_consumer.mydata.customers as
select 'user'||seq4()||'_'||uniform(1, 3, random())||'@email.com' as email,
 replace(to_varchar(seq4() % 999, '000') ||'-'||to_varchar(seq4() % 888, '000')||'-'||to_varchar(seq4() % 777, '000')||uniform(1, 10, random()),' ','') as phone,
  case when uniform(1,10,random())<2 then 'CAT'
       when uniform(1,10,random())>=2 then 'DOG'
       when uniform(1,10,random())>=1 then 'BIRD'
else 'NO_PETS' end as pets,
  round(20000+uniform(0,65000,random()),-2) as zip,
  uniform(1,1000,random())/1000 as ltv_score,
  (ltv_score > .7)::varchar as high_value
  from table(generator(rowcount => 1000000));

create or replace table ppd_demo_consumer.mydata.conversions as
    select email, 'product_'||uniform(1,5,random()) as product,
    ('2021-'||uniform(3,5,random())||'-'||uniform(1,30,random()))::date as sls_date,
    uniform(1,100,random())+uniform(1,100,random())/100 as sales_dlr
    from ppd_demo_consumer.mydata.customers sample (10);

/* request log table to be shared with provider, required for application */
create or replace table ppd_demo_consumer.shared.requests (request_id varchar(1000),request variant);
create or replace table ppd_demo_consumer.local.user_settings (setting_name varchar(1000), setting_value varchar(1000));

/* populate the local settings table - provider1 */

delete from ppd_demo_consumer.local.user_settings;
insert into ppd_demo_consumer.local.user_settings (setting_name, setting_value)
VALUES ('app_data','ppd_demo_app'),
        ('consumer_db','ppd_demo_consumer'),
        ('consumer_schema','mydata'),
        ('consumer_shared_data_schema','shared'),
        ('consumer_table','customers'),
        ('consumer_join_field','email'),
        ('consumer_email_field','email'),
        ('consumer_phone_field','phone'),
        ('consumer_customer_table','customers'),
        ('consumer_conversions_table','conversions'),
        ('consumer_internal_join_field','email')
        ;

create or replace secure view ppd_demo_consumer.shared.customers as select * from ppd_demo_consumer.mydata.customers;
create or replace secure view ppd_demo_consumer.shared.conversions as select * from ppd_demo_consumer.mydata.conversions;

select 'Install application from the private listing. Then run script file consumer_app_grants.sql.' as DO_THIS_NEXT;
