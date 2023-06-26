/*************************************************************************************************************
Script:             Privacy Protected Data - Native App - Consumer Request
Create Date:        2023-05-16
Author:             M. Rainey
Description:        Sample Native App -- Consumer side request process
Copyright © 2023 Snowflake Inc. All rights reserved
****************************************************************************************************************************************************/

use role ppd_consumer_role;
use database ppd_demo_app;
use warehouse ppd_wh;

/* JINJA BASED REQUESTS */
/* see the templates */
select * from ppd_demo_app.internal.templates;

/* see previous requests, if any */
select * from ppd_demo_consumer.shared.requests;

/* REQUEST 1, BASIC OVERLAP COUNT JOIN ON EMAIL WITH COUNT */
/* see count of customer overlap */
call ppd_demo_app.allowed_sprocs.run('customer_overlap', object_construct('', ''));

/* REQUEST 2, OVERLAP COUNT JOIN ON EMAIL WITH COUNT AND WHERE CLAUSE AND GROUP BYS */
/* try with combinations 'c.zip', 'c.pets', 'p.status', 'p.age_band' */
call ppd_demo_app.allowed_sprocs.run('customer_overlap',
        object_construct(
            'dimensions',array_construct( 'p.status','c.pets'),
            'where_clause','PETS <> $$BIRD$$'
            ));

/* REQUEST 3, CAMPAIGN CONVERSION */
/* see campaign conversion counts */
call ppd_demo_app.allowed_sprocs.run('campaign_conversion',
        object_construct(
            'dimensions',array_construct( 'c_conv.product', 'p_exp.campaign' ),
            'where_clause','c_conv.sls_date >= p_exp.exp_date'
            ));


/* REQUEST 4, FULL LOOP OVERLAP COUNT BOOLEAN OR JOIN ON EMAIL AND PHONE WITH COUNT AND WHERE CLAUSE */
call ppd_demo_app.allowed_sprocs.run('customer_overlap_waterfall',
        object_construct(
            'dimensions',array_construct('p.status', 'c.pets', 'p.age_band'),
            'where_clause',' c.PETS <> $$BIRD$$ '
            ));

