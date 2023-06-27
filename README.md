# Limited Access Data Sample Framework
Copyright (c) 2023 Snowflake Inc. All Rights Reserved.

**For use with Snowflake Native Applications Preview feature**

# Deploying this code

Update all references to CONSUMER_ACCT to the consumer account name and PROVIDER_ACCT to the provider account name.

Prior to creating a Python UDF, you must acknowledge the Snowflake Third Party Terms following steps here:
https://docs.snowflake.com/developer-guide/udf/python/udf-python-packages#getting-started

Run in this order:

To set up 1 consumer and 1 provider:

1. Provider account: run provider_init.sql<br/>
2. Provider account: run provider_app_pkg.sql<br/>
3. Provider account: in SnowSQL, upload the setup_script.sql and manifest.yml files to the stage location<br/>

```console
PUT file:///Users/mrainey/Documents/limited-access-data/v1/manifest.yml @ppd_demo_dev.code.files_v1 auto_compress=false overwrite=true;
PUT file:///Users/mrainey/Documents/limited-access-data/v1/setup_script.sql @ppd_demo_dev.code.files_v1 auto_compress=false overwrite=true;
```
4. Provider account: run provider_app_version.sql<br/>
5. Provider account: Add private listing using provider studio<br/>
     5.1  Select 'PROVIDER_ROLE'
<br/>
     5.2  Select Provider Studio from menu to launch the screen to create listings
<br/>
     5.3  Create Listing by choosing a name for the application and selecting the appropriate audience 
<br/>
     5.4	Name the application and add Consumer Account details for the private listing
<br/>
     5.5	Use script 'consumer_add_grants.sql' to add to Sample Queries in order for consumer to be able to execute after installation
<br/>
     5.6	Modify SQL to include the relevant provider account locator
<br/>
     5.7	After adding the script select the 'Publish' button and the listing is now live<br  />  
6. Consumer account: run consumer_init.sql<br  />
7. Consumer account: install application from listing<br  />
<br  />
     7.1	Select 'Apps' from menu to launch module
<br/>
     7.2	Select ppd_CONSUMER_ROLE as the role to implement application
<br/>
     7.3	'Get' the application that has been shared with you and select appropriate warehouse to run it on
<br/>
     7.4	Once application is installed, click Done and navigate to the application and select it. This will launch the remaining script to configure the app.
                                    
8. Consumer account: run consumer_app_grants.sql<br/>
9. Provider account: run provider_enable_consumer.sql<br/>
10. Consumer account: run individual statements in consumer_request.sql<br/>

This sample code is provided for reference purposes only.  Please note that this code is provided “AS IS” and without warranty.  Snowflake will not offer any support for use of the sample code.

