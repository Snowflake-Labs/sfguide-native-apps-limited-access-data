/*************************************************************************************************************
Script:             Limited Access Data - Native App - Provider Application Version
Create Date:        2023-04-20
Author:             M. Rainey
Description:        Sample Native App -- Provider altering the application package object to add a new 
                    version and release directive. Instructions for adding the application package to a 
                    listing are provided. Depends on provider_app_pkg.sql.
Copyright © 2023 Snowflake Inc. All rights reserved
***************************************************************************************************************************************************/

/* APPLICATION PACKAGE VERSION */

use role provider_role;
call system$wait(3);

USE WAREHOUSE ppd_WH;

/* ensure application files have been added to stage via snowsql */
-- PUT file:///Users/mrainey/Documents/limited-access-data/manifest.yml @ppd_demo_dev.code.files_v1 auto_compress=false overwrite=true;
-- PUT file:///Users/mrainey/Documents/limited-access-data/setup_script.sql @ppd_demo_dev.code.files_v1 auto_compress=false overwrite=true;

/* cleanup
ALTER APPLICATION PACKAGE ppd_demo_package
    DROP VERSION Version1;
*/

/* create application package version using code files */
ALTER APPLICATION PACKAGE ppd_demo_package
    ADD VERSION Version1 USING '@ppd_demo_dev.code.files_v1';

ALTER APPLICATION PACKAGE ppd_demo_package
  SET DEFAULT RELEASE DIRECTIVE
  VERSION = Version1
  PATCH = 0;    

/* add to listing */
/* follow instructions here to create a private listing: 
  https://docs.snowflake.com/en/nativeapps-provider-create-listing.html#label-nativeapps-provider-listings-create
*/

select 'Add application to a private listing.' as DO_THIS_NEXT;
