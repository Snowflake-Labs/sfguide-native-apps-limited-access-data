/*************************************************************************************************************
Script:             Limited Access Data - Native App - Consumer Application Grants
Create Date:        2023-05-16
Author:             M. Rainey
Description:        Sample Native App -- Consumer side grant privileges on consumer objects to the app and
                    create shares back to the provider account.
Copyright © 2023 Snowflake Inc. All rights reserved
***************************************************************************************************************************************************/

/* CONSUMER grant app */

use role ppd_consumer_role;
call system$wait(3);

/**** install application from marketplace listing ****/
/* ensure your snowsight role context is set to ppd_consumer_role
  follow instructions here to install a private listing: 
  https://other-docs.snowflake.com/en/LIMITEDACCESS/nativeapps-consumer-listings.html#installing-an-application-from-a-privately-shared-listing
*/

grant application role ppd_demo_app.ppd_db_role to role ppd_consumer_role;

/* grant privileges on consumer side objects to application database */
grant usage on database ppd_demo_consumer to application ppd_demo_app;
grant usage on schema ppd_demo_consumer.local to application ppd_demo_app;
grant select on table ppd_demo_consumer.local.user_settings to application ppd_demo_app;
grant usage on schema ppd_demo_consumer.shared to application ppd_demo_app;
grant usage on schema ppd_demo_consumer.mydata to application ppd_demo_app;
grant select on table ppd_demo_consumer.shared.customers to application ppd_demo_app;
grant select, insert, update on ppd_demo_consumer.shared.requests to application ppd_demo_app;
grant select on table ppd_demo_consumer.shared.customers to application ppd_demo_app;
grant select on table ppd_demo_consumer.mydata.customers to application ppd_demo_app;
grant select on table ppd_demo_consumer.shared.conversions to application ppd_demo_app;

/* create share to send requests to provider account */
create or replace share ppd_demo_requests;
grant usage on database ppd_demo_consumer to share ppd_demo_requests;
grant usage on schema ppd_demo_consumer.shared to share ppd_demo_requests;
grant select on ppd_demo_consumer.shared.requests to share ppd_demo_requests;
alter share ppd_demo_requests add accounts = PROVIDER_ACCT;

select 'Run code in script file provider_enable_consumer.sql on the provider account' as DO_THIS_NEXT;
