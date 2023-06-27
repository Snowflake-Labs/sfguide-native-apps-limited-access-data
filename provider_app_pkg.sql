/*************************************************************************************************************
Script:             Limited Access Data - Native App - Provider Application Package
Create Date:        2023-05-16
Author:             M. Rainey
Description:        Sample Native App -- Provider creating the application package object. Depends on
                    the creation of setup_script.sql and manifest.yml, and follows provider-init.sql.
Copyright © 2023 Snowflake Inc. All rights reserved
***************************************************************************************************************************************************/

/* APPLICATION PACKAGE */

use role provider_role;
call system$wait(3);

USE WAREHOUSE ppd_WH;

drop database if exists ppd_demo_package;
create application package ppd_demo_package;

/* create the schema to share the protected provider data */
create or replace schema ppd_demo_package.shared;

/* create views to share provider application supporting data with the consumer */
create or replace secure view shared.provider_account as select * from ppd_demo_provider_db.admin.provider_account;

/* create secure views for sharing provider source data */
create secure view shared.customers_v as select * from ppd_demo_provider_db.source.customers;
create secure view shared.exposures_v as select * from ppd_demo_provider_db.source.exposures;

/* grant privileges on objects to share in application package */
GRANT REFERENCE_USAGE ON DATABASE ppd_demo_provider_db TO SHARE IN APPLICATION PACKAGE ppd_demo_package; 
GRANT USAGE ON SCHEMA ppd_demo_package.shared TO SHARE IN APPLICATION PACKAGE ppd_demo_package;
GRANT SELECT ON TABLE ppd_demo_package.shared.provider_account TO SHARE IN APPLICATION PACKAGE ppd_demo_package;
GRANT SELECT ON VIEW ppd_demo_package.shared.customers_v TO SHARE IN APPLICATION PACKAGE ppd_demo_package;
GRANT SELECT ON VIEW ppd_demo_package.shared.exposures_v TO SHARE IN APPLICATION PACKAGE ppd_demo_package;

 /* create provider application database code repository */
create database if not exists ppd_demo_dev;
create schema if not exists code;
create or replace STAGE files_v1;

/* add application files to stage via snowsql */
-- PUT file:///Users/mrainey/Documents/limited-access-data/manifest.yml @ppd_demo_dev.code.files_v1 auto_compress=false overwrite=true;
-- PUT file:///Users/mrainey/Documents/limited-access-data/setup_script.sql @ppd_demo_dev.code.files_v1 auto_compress=false overwrite=true;

/* test creating the app locally
CREATE APPLICATION ppd_demo_app FROM APPLICATION PACKAGE ppd_demo_package;
*/

select 'Add application setup_script.sql and manifest.yml files to stage @ppd_demo_dev.code.files_v1. Then run script file provider_app_version.sql.' as DO_THIS_NEXT;
