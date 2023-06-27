/*************************************************************************************************************
Script:             Limited Access Data - Native App - Setup Script v1
Create Date:        2023-05-16
Author:             M. Rainey
Description:        Sample Native App -- Provider application setup script contains the objects that
                    the application will use when implemented on the consumer account. 
Copyright © 2023 Snowflake Inc. All rights reserved
***************************************************************************************************************************************************/

/*** make sure this file is ready, but do not run in worksheet ***/

/* create versioned schema for application procedures */
create or alter versioned schema allowed_sprocs;

/* schema for internal app processing */    
create or replace schema app_internal_schema;

/* query request procedure */
create or replace procedure allowed_sprocs.request(in_template varchar(1000), in_params varchar(10000), request_id varchar(1000), at_timestamp VARCHAR(30))
    returns variant
    language javascript
    execute as owner 
    as
    $$

    /* get the provider account */
    var result = snowflake.execute({ sqlText:  `select account_name from internal.provider_account ` });
    result.next();
    var provider_account = result.getColumnValue(1);

    /* get the consumer account */
    var result = snowflake.execute({ sqlText:  `select current_account() ` });
    result.next();
    var requester_account = result.getColumnValue(1);

    /* bring procedure parameters into JS variables, and remove unneeded special characters (low grade SQL injection defense) */
    var in_template_name = IN_TEMPLATE.replace(/[^a-zA-Z0-9_]/g, "");
    var in_parameters = IN_PARAMS;
    if (in_parameters) {
        in_parameters = in_parameters.replace(/[^a-zA-Z0-9_{}:"$\\s.\\,<>=\\+\\%\\-\\[\\]]/g, "");
    }
    // set timestamp
    var at_timestamp = "CURRENT_TIMESTAMP()::string";

    // create the request JSON object with a SQL statements
        var request_sql = `
        with all_params as (
    with request_params as
    (
    select replace(uuid_string(),'-','_') as request_id,
    '`+in_template_name+`' as query_template,
    '`+requester_account+`' as requester_account,
    array_construct('`+provider_account+`') as provider_accounts,
    `+at_timestamp+` as request_ts
    ),
    query_params as
    (select parse_json('`+in_parameters+`') query_params
    ),
    settings as
    (
    select object_agg(setting_name, setting_value::variant) as settings from ppd_demo_consumer.local.user_settings
    ),
    query_params_full as
    (
    select request_params.*, parse_json(left(query_params::varchar,len(query_params::varchar)-1) ||
                                        ', "at_timestamp": "'||request_params.request_ts::varchar||'",' ||
                                        '"request_id": "'||request_params.request_id||'",' ||
                                        right(settings::varchar,len(settings::varchar::varchar)-1)) as request_params
    from query_params, settings, request_params
    ),
    proposed_query as (
            select internal.get_sql_jinja(
                (select template from internal.templates where template_name = rp.query_template), qpf.request_params) as proposed_query,
                sha2(proposed_query) as proposed_query_hash from query_params_full qpf, request_params rp)
    select rp.*, pq.*, f.request_params
    from query_params_full f, request_params rp, proposed_query pq )
    select object_construct(*) as request from all_params;   `;

  var result = snowflake.execute({ sqlText: request_sql });
  result.next();
  var request = result.getColumnValue(1);


  // put request JSON into a temporary place so if it is approved we can use it later directly from SQL to avoid JS altering it
   var result = snowflake.execute({ sqlText:
      `
     create or replace table app_internal_schema.request_temp as
      select REQUEST FROM table(result_scan(last_query_id()));
      ` });
  
    var request_string = escape(JSON.stringify(request));

    // extract items that are needed by the approval process (with additional low-grade sql injection defense)
    request_id = request.REQUEST_PARAMS.request_id.replace(/[^a-zA-Z0-9_]/g, "");
    proposed_query_hash = request.PROPOSED_QUERY_HASH.replace(/[^a-zA-Z0-9_]/g, "");
    request_ts =  request.REQUEST_TS.replace(/[^a-zA-Z0-9_:\.\-\s]/g, "");
    request_template = request.QUERY_TEMPLATE.replace(/[^a-zA-Z0-9_]/g, "");
    var proposed_query = request.PROPOSED_QUERY;

    // get template type
    var result = snowflake.execute({ sqlText:
      `
       select template_type from internal.templates where template_name = '`+request_template+`'
      ` });
    result.next();
    var template_type = result.getColumnValue(1);

    /* INSERT CUSTOM VALIDATION STEPS */

    /* FROM HERE BELOW, ASSUMES VALIDATION SUCCESS */

    /*insert the signed request */
    var insert_sql =  `insert into ppd_demo_consumer.shared.requests (request_id , request )
        select '`+request_id+`',(select any_value(request) from app_internal_schema.request_temp) ; `;

    var r_stmt = snowflake.createStatement( { sqlText: insert_sql } );
    var result = r_stmt.execute();

    return [ "Approved", request_id, request ];
    $$;
    
/* run procedure for immediate results */
create or replace procedure allowed_sprocs.run(template string, params variant)
returns table()
language sql
  
as
$$
declare
        res resultset;
        res1 resultset;
        res2 resultset;
        status varchar;
        req_sql varchar;
        request_id varchar;
        consumer_db varchar;
        c1 cursor for select request[0]::varchar, request[2]:PROPOSED_QUERY::varchar, request[1]::varchar, request[2]:REQUEST_PARAMS:consumer_db::varchar FROM table(result_scan(last_query_id()));
begin

        call allowed_sprocs.request(:TEMPLATE,:PARAMS::varchar, null, null);

        open c1;
        fetch c1 into status, req_sql, request_id, consumer_db;
        if (:status != 'Approved') then
          res2 := (select :status as "invalid_request");
          return table(res2);
        end if;

        res := (execute immediate :req_sql);
        return table(res);
end;
$$;

/* schema for sharing app objects with consumer */
create schema internal;

/* create template processing jinja sql function */
create or replace function internal.get_sql_jinja(template string, parameters variant)
  returns string
  language python
  runtime_version = 3.8
  handler='apply_sql_template'
  packages = ('six','jinja2==3.0.3','markupsafe')
  
as
$$
# Most of the following code is copied from the jinjasql package, which is not included in Snowflake's python packages
from __future__ import unicode_literals
import jinja2
from six import string_types
from copy import deepcopy
import os
import re
from jinja2 import Environment
from jinja2 import Template
from jinja2.ext import Extension
from jinja2.lexer import Token
from markupsafe import Markup

try:
    from collections import OrderedDict
except ImportError:
    # For Python 2.6 and less
    from ordereddict import OrderedDict

from threading import local
from random import Random

_thread_local = local()

# This is mocked in unit tests for deterministic behaviour
random = Random()


class JinjaSqlException(Exception):
    pass

class MissingInClauseException(JinjaSqlException):
    pass

class InvalidBindParameterException(JinjaSqlException):
    pass

class SqlExtension(Extension):

    def extract_param_name(self, tokens):
        name = ""
        for token in tokens:
            if token.test("variable_begin"):
                continue
            elif token.test("name"):
                name += token.value
            elif token.test("dot"):
                name += token.value
            else:
                break
        if not name:
            name = "bind#0"
        return name

    def filter_stream(self, stream):
        """
        We convert
        {{ some.variable | filter1 | filter 2}}
            to
        {{ ( some.variable | filter1 | filter 2 ) | bind}}

        ... for all variable declarations in the template

        Note the extra ( and ). We want the | bind to apply to the entire value, not just the last value.
        The parentheses are mostly redundant, except in expressions like {{ '%' ~ myval ~ '%' }}

        This function is called by jinja2 immediately
        after the lexing stage, but before the parser is called.
        """
        while not stream.eos:
            token = next(stream)
            if token.test("variable_begin"):
                var_expr = []
                while not token.test("variable_end"):
                    var_expr.append(token)
                    token = next(stream)
                variable_end = token

                last_token = var_expr[-1]
                lineno = last_token.lineno
                # don't bind twice
                if (not last_token.test("name")
                    or not last_token.value in ('bind', 'inclause', 'sqlsafe')):
                    param_name = self.extract_param_name(var_expr)

                    var_expr.insert(1, Token(lineno, 'lparen', u'('))
                    var_expr.append(Token(lineno, 'rparen', u')'))
                    var_expr.append(Token(lineno, 'pipe', u'|'))
                    var_expr.append(Token(lineno, 'name', u'bind'))
                    var_expr.append(Token(lineno, 'lparen', u'('))
                    var_expr.append(Token(lineno, 'string', param_name))
                    var_expr.append(Token(lineno, 'rparen', u')'))

                var_expr.append(variable_end)
                for token in var_expr:
                    yield token
            else:
                yield token

def sql_safe(value):
    """Filter to mark the value of an expression as safe for inserting
    in a SQL statement"""
    return Markup(value)

def bind(value, name):
    """A filter that prints %s, and stores the value
    in an array, so that it can be bound using a prepared statement

    This filter is automatically applied to every {{variable}}
    during the lexing stage, so developers can't forget to bind
    """
    if isinstance(value, Markup):
        return value
    elif requires_in_clause(value):
        raise MissingInClauseException("""Got a list or tuple.
            Did you forget to apply '|inclause' to your query?""")
    else:
        return _bind_param(_thread_local.bind_params, name, value)

def bind_in_clause(value):
    values = list(value)
    results = []
    for v in values:
        results.append(_bind_param(_thread_local.bind_params, "inclause", v))

    clause = ",".join(results)
    clause = "(" + clause + ")"
    return clause

def _bind_param(already_bound, key, value):
    _thread_local.param_index += 1
    new_key = "%s_%s" % (key, _thread_local.param_index)
    already_bound[new_key] = value

    param_style = _thread_local.param_style
    if param_style == 'qmark':
        return "?"
    elif param_style == 'format':
        return "%s"
    elif param_style == 'numeric':
        return ":%s" % _thread_local.param_index
    elif param_style == 'named':
        return ":%s" % new_key
    elif param_style == 'pyformat':
        return "%%(%s)s" % new_key
    elif param_style == 'asyncpg':
        return "$%s" % _thread_local.param_index
    else:
        raise AssertionError("Invalid param_style - %s" % param_style)

def requires_in_clause(obj):
    return isinstance(obj, (list, tuple))

def is_dictionary(obj):
    return isinstance(obj, dict)

class JinjaSql(object):
    # See PEP-249 for definition
    # qmark "where name = ?"
    # numeric "where name = :1"
    # named "where name = :name"
    # format "where name = %s"
    # pyformat "where name = %(name)s"
    VALID_PARAM_STYLES = ('qmark', 'numeric', 'named', 'format', 'pyformat', 'asyncpg')
    def __init__(self, env=None, param_style='format'):
        self.env = env or Environment()
        self._prepare_environment()
        self.param_style = param_style

    def _prepare_environment(self):
        self.env.autoescape=True
        self.env.add_extension(SqlExtension)
        self.env.filters["bind"] = bind
        self.env.filters["sqlsafe"] = sql_safe
        self.env.filters["inclause"] = bind_in_clause

    def prepare_query(self, source, data):
        if isinstance(source, Template):
            template = source
        else:
            template = self.env.from_string(source)

        return self._prepare_query(template, data)

    def _prepare_query(self, template, data):
        try:
            _thread_local.bind_params = OrderedDict()
            _thread_local.param_style = self.param_style
            _thread_local.param_index = 0
            query = template.render(data)
            bind_params = _thread_local.bind_params
            if self.param_style in ('named', 'pyformat'):
                bind_params = dict(bind_params)
            elif self.param_style in ('qmark', 'numeric', 'format', 'asyncpg'):
                bind_params = list(bind_params.values())
            return query, bind_params
        finally:
            del _thread_local.bind_params
            del _thread_local.param_style
            del _thread_local.param_index

# Non-JinjaSql package code starts here
def quote_sql_string(value):
    '''
    If `value` is a string type, escapes single quotes in the string
    and returns the string enclosed in single quotes.
    '''
    if isinstance(value, string_types):
        new_value = str(value)
        new_value = new_value.replace("'", "''")
        #baseline sql injection deterrance
        new_value2 = re.sub(r"[^a-zA-Z0-9_.-]","",new_value)
        return "'{}'".format(new_value2)
    return value

def get_sql_from_template(query, bind_params):
    if not bind_params:
        return query
    params = deepcopy(bind_params)
    for key, val in params.items():
        params[key] = quote_sql_string(val)
    return query % params

def strip_blank_lines(text):
    '''
    Removes blank lines from the text, including those containing only spaces.
    https://stackoverflow.com/questions/1140958/whats-a-quick-one-liner-to-remove-empty-lines-from-a-python-string
    '''
    return os.linesep.join([s for s in text.splitlines() if s.strip()])

def apply_sql_template(template, parameters):
    '''
    Apply a JinjaSql template (string) substituting parameters (dict) and return
    the final SQL.
    '''
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(template, parameters)
    return strip_blank_lines(get_sql_from_template(query, bind_params))

$$;



/* Populate Jinja templates */

create or replace table internal.ppd_templates (template_name string, template string, dp_sensitivity int, dimensions varchar(2000), template_type string, procedure_header string );

insert into internal.ppd_templates (template_name, template, dimensions, template_type) 
values ('customer_overlap',
$$
select
{% if dimensions %}
    identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    , {% endif %}  
    count(distinct p.email) as overlap 
from
    internal.customers_v p,
    {{ consumer_db | sqlsafe }}.{{ consumer_shared_data_schema | sqlsafe }}.{{ consumer_customer_table | sqlsafe }} at(timestamp => '{{ at_timestamp | sqlsafe }}'::timestamp_tz) c
where
    c.{{ consumer_join_field | sqlsafe }} = p.email
    {% if  where_clause  %} 
    and ( {{ where_clause | sqlsafe }} )
    {% endif %}    
{% if dimensions %}
    group by identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    {% endif %} 
having overlap  > 25
order by overlap desc;
$$,'c.pets|c.zip|c.high_value|p.status|p.age_band|p.region_code','SQL_immediate');

insert into internal.ppd_templates (template_name, template, dimensions, template_type) 
values ('campaign_conversion',
$$
with actual_result as 
(
select
    identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    , count(distinct p.email) as conversion_count
from
      internal.customers_v p,
      internal.exposures_v p_exp,
    {{ consumer_db | sqlsafe }}.{{ consumer_shared_data_schema | sqlsafe }}.{{ consumer_customer_table | sqlsafe }} c
join
    {{ consumer_db | sqlsafe }}.{{ consumer_shared_data_schema | sqlsafe }}.{{ consumer_conversions_table | sqlsafe }} c_conv
        on c.{{ consumer_internal_join_field | sqlsafe }} = c_conv.{{ consumer_internal_join_field | sqlsafe }}
where
    ( 
      c.{{ consumer_email_field | sqlsafe }} = p.email
    )
    and p.email = p_exp.email
    {% if  where_clause  %} 
    and ( {{ where_clause | sqlsafe }} )
    {% endif %}
group by
    identifier({{ dimensions[0]  }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim }})
    {% endfor %}
having count(distinct p.email)  > 25
order by count(distinct p.email) desc)
{% set d = dimensions[0].split('.') %}
select identifier({{ d[1] }})
    {% for dim in dimensions[1:] %}
    {% set d = dim.split('.') %} , identifier({{ d[1]  }})
    {% endfor %}
    , conversion_count as bought_after_exposure
    from actual_result
    having bought_after_exposure > 25
    order by bought_after_exposure desc;
$$, 'c.pets|c.zip|c.high_value|p.status|p.age_band|p.region_code|c_conv.product|p_exp.campaign|p_exp.device_type','SQL_immediate');

insert into internal.ppd_templates (template_name, template, dimensions, template_type) 
values ('customer_overlap_waterfall',
$$
select
    identifier({{ dimensions[0] }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim  }})
    {% endfor %}
    , count(distinct p.email) as overlap 
from
    {{ app_data | sqlsafe }}.internal.customers_v p
join    {{ consumer_db | sqlsafe }}.{{ consumer_schema | sqlsafe }}.{{ consumer_table | sqlsafe }} at(timestamp => '{{ at_timestamp | sqlsafe }}'::timestamp_tz) c
on  ( 
      c.{{ consumer_email_field | sqlsafe }} = p.email
      or c.{{ consumer_phone_field | sqlsafe }} = p.phone
    )
    {% if  where_clause  %} 
    and ( {{ where_clause | sqlsafe }} )
    {% endif %}
    and exists (select table_name from {{ consumer_db | sqlsafe }}.information_schema.tables where table_schema = upper('{{ consumer_schema | sqlsafe }}') and table_name = upper('{{ consumer_table| sqlsafe }}') and table_type = 'BASE TABLE')
group by
    identifier({{ dimensions[0]  }})
    {% for dim in dimensions[1:] %}
    , identifier({{ dim }})
    {% endfor %}
having overlap  > 25
order by overlap desc;
$$, 'c.pets|c.zip|c.high_value|p.status|p.age_band|p.region_code','SQL_immediate');




/* create views on provider data that will not be shared with consumer directly */
create secure view internal.customers_v as select * from shared.customers_v;
create secure view internal.exposures_v as select * from shared.exposures_v; 

/* create secure views to share application supporting data with the consumer */
create or replace secure view internal.provider_account as select * from shared.provider_account;
create or replace secure view internal.templates as select * from internal.ppd_templates;

/* create application role for consumer access to objects and apply grants */
create or replace application role ppd_db_role; 

grant usage on schema allowed_sprocs to application role ppd_db_role;
grant usage on schema internal to application role ppd_db_role;  
grant usage on procedure allowed_sprocs.request( varchar(1000),  varchar(10000),  varchar(1000),  VARCHAR(30)) TO application role ppd_db_role;
grant usage on procedure allowed_sprocs.run( string,  variant) TO application role ppd_db_role;
GRANT SELECT ON TABLE internal.provider_account to application role ppd_db_role;
GRANT SELECT ON VIEW internal.templates TO application role ppd_db_role;
grant usage on function internal.get_sql_jinja( string, variant) TO application role ppd_db_role;
