/*************************************************************************************************************
Script:             Limited Access Data - Native App - Provider Enable Consumer
Create Date:        2023-05-16
Author:             M. Rainey
Description:        Sample Native App -- Provider enabling the consumer for submitting requests. Depends 
                    on the execution of script consumer_init.sql.
Copyright © 2023 Snowflake Inc. All rights reserved
***************************************************************************************************************************************************/

/* run after the app is installed on the consumer account */

use role provider_role;
call system$wait(3);

/* create a new mounted request database for each consumer */

/* create database from share */
create or replace database ppd_demo_CONSUMER_ACCT from share CONSUMER_ACCT.ppd_demo_requests;

/* Grant permissions */
use role accountadmin;
call system$wait(3);
grant imported privileges on database ppd_demo_CONSUMER_ACCT to role provider_role;

select 'Run individual statements in script file consumer_request.sql on the consumer account' as DO_THIS_NEXT;
