/*************************************************************************************************************
Script:             Limited Access Data - Native App - Provider Initialization
Create Date:        2023-05-16
Author:             M. Rainey
Description:        Sample Native App -- Provider object and data initialization
Copyright © 2023 Snowflake Inc. All rights reserved
***************************************************************************************************************************************************/

/* cleanup provider */
/*
drop database if exists ppd_demo_provider_db;
drop database if exists ppd_demo_dev;
drop database if exists ppd_demo_package;
drop database if exists ppd_demo_CONSUMER_ACCT;
*/

/* Setup roles */

use role accountadmin;
call system$wait(3);

// CREATE WAREHOUSE 
create warehouse if not exists ppd_wh;

/* create role and add permissions required by role for installation of framework */
create role if not exists provider_role;

grant create share on account to role provider_role;
grant import share on account to role provider_role;
grant create database on account to role provider_role;
grant execute task on account to role provider_role;
grant create application package on account to role provider_role;
grant create data exchange listing on account to role provider_role;
grant role provider_role to role sysadmin;
grant usage, operate on warehouse ppd_wh to role provider_role;

/* Setup provider side objects */
use role provider_role;
call system$wait(3);

/* cleanup */
drop database if exists ppd_demo_provider_db;
drop database if exists ppd_demo_package;

/* create provider database and schemas */
create or replace database ppd_demo_provider_db;
create or replace schema ppd_demo_provider_db.source;
create or replace schema ppd_demo_provider_db.admin;
create or replace schema ppd_demo_provider_db.templates;

use warehouse ppd_wh;

/* create sample provider data */
create or replace table ppd_demo_provider_db.source.customers  as
select 'user'||seq4()||'_'||uniform(1, 3, random())||'@email.com' as email,
 replace(to_varchar(seq4() % 999, '000') ||'-'||to_varchar(seq4() % 888, '000')||'-'||to_varchar(seq4() % 777, '000')||uniform(1, 10, random()),' ','') as phone,
  case when uniform(1,10,random())>3 then 'MEMBER'
       when uniform(1,10,random())>5 then 'SILVER'
       when uniform(1,10,random())>7 then 'GOLD'
else 'PLATINUM' end as status,
round(18+uniform(0,10,random())+uniform(0,50,random()),-1)+5*uniform(0,1,random()) as age_band,
'REGION_'||uniform(1,20,random()) as region_code,
uniform(1,720,random()) as days_active
  from table(generator(rowcount => 5000000));

create or replace table ppd_demo_provider_db.source.exposures  as
select email, 'campaign_'||uniform(1,3,random()) as campaign,
  case when uniform(1,10,random())>3 then 'STREAMING'
       when uniform(1,10,random())>5 then 'MOBILE'
       when uniform(1,10,random())>7 then 'LINEAR'
else 'DISPLAY' end as device_type,
('2021-'||uniform(3,5,random())||'-'||uniform(1,30,random()))::date as exp_date,
uniform(1,60,random()) as sec_view,
uniform(0,2,random())+uniform(0,99,random())/100 as exp_cost
from ppd_demo_provider_db.source.customers sample (20);

/* setup applictation supporting tables */

create or replace table ppd_demo_provider_db.admin.provider_account(account_name varchar(1000)) as select current_account();

select 'Run code in script file provider_app_pkg.sql on the provider account' as DO_THIS_NEXT;
