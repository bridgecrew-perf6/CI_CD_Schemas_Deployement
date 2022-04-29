create schema dev_demo_schema;

create  sequence IF NOT EXISTS ALERT_RULES_SEQ start with 1 increment by 1;

create  sequence  IF NOT EXISTS ALERT_TBL_SEQ start with 1 increment by 1;



create TABLE IF NOT EXISTS ALERT_RULES (
	ID NUMBER(38,0),
	CATEGORY VARCHAR(200),
	TRIGGER_TYPE VARCHAR(200),
	BEHAVIOUR_TYPE VARCHAR(200),
	SOURCE_NM VARCHAR(200),
	CONTEXT VARCHAR(100),
	CONDITION VARCHAR(16777216),
	RULE_JSON VARCHAR(16777216),
	FEATURE VARCHAR(200),
	METRIC VARCHAR(100),
	OPERATOR VARCHAR(100),
	THRESHOLD NUMBER(38,2),
	ARITHMETIC VARCHAR(100),
	MEASURE VARCHAR(100),
	MEASURE_LIMIT NUMBER(38,0),
	TIMEFRAME_VAL NUMBER(38,0),
	TIMEFRAME_UNIT VARCHAR(100),
	TRIGGER_FREQUENCY VARCHAR(100),
	CRON_EXPRESSION VARCHAR(200),
	STATUS VARCHAR(100),
	SEVERITY VARCHAR(200),
	DESCRIPTION VARCHAR(16777216),
	COMMENT VARCHAR(16777216),
	IS_DELETED BOOLEAN,
	USER_VISIBILITY BOOLEAN,
	ENABLE_NOTIFICATION BOOLEAN,
	EMAIL VARCHAR(16777216),
	ROLLUP_TIME_VAL NUMBER(38,0),
	ROLLUP_TIME_UNIT VARCHAR(100),
	CREATED_BY VARCHAR(200),
	COMMIT_TIMESTAMP TIMESTAMP_NTZ(9),
	REQUEST_ID NUMBER(38,0),
	CONDITION_TYPE VARCHAR(100),
	INFO_RPLC VARCHAR(16777216),
	INFO_JSON_RPLC VARCHAR(16777216),
	KQL_INPUT VARCHAR(16777216),
	FEATURE_DATATYPE VARCHAR(16777216)
);




create TABLE IF NOT EXISTS ALERT_RULES_LOOKUP (
	CATEGORY VARCHAR(200),
	NAME VARCHAR(200),
	VALUE VARCHAR(200),
	DATATYPE VARCHAR(200),
	SOURCE_NM VARCHAR(200),
	CONTEXT VARCHAR(200),
	CONDITION VARCHAR(200),
	SOURCE_NM_ALIAS VARCHAR(200)
);




create TABLE  IF NOT EXISTS ALERT_TBL (
	ALERT_ID NUMBER(38,0) DEFAULT ALERT_TBL_SEQ.NEXTVAL,
	EVENT_TIME TIMESTAMP_NTZ(9),
	COMMIT_TIME TIMESTAMP_NTZ(9),
	ALERT_RULE_ID VARCHAR(100),
	ALERT_TYPE VARCHAR(100),
	SOURCE_NM VARCHAR(200),
	CONTEXT VARCHAR(100),
	TRIGGER_TYPE VARCHAR(100),
	INFO VARIANT,
	INSERTION_TS TIMESTAMP_NTZ(9)
);




create TABLE IF NOT EXISTS ALERT_TBL_INGEST_TRACK (
	ODM_NM VARCHAR(16777216),
	COMMIT_TIME TIMESTAMP_NTZ(9)
);




create TABLE IF NOT EXISTS ALERT_APP_TBL (
	PARENT_ALERT_ID VARCHAR(16777216),
	ID VARCHAR(16777216),
	PARENT_KEY VARIANT,
	ALERT_NAME VARCHAR(16777216),
	EVENT_DATE TIMESTAMP_NTZ(9),
	COMMIT_TIME TIMESTAMP_NTZ(9),
	PRIORITY VARCHAR(16777216),
	ALERT_TITLE VARCHAR(16777216),
	ALERT_TYPE VARCHAR(16777216),
	DESCRIPTION VARIANT,
	ASSIGNED VARCHAR(16777216),
	STATUS VARCHAR(16777216),
	MODIFIED_BY VARCHAR(16777216),
	DELETED_FLAG BOOLEAN,
	MODIFIED_TS TIMESTAMP_NTZ(9),
	ENABLE_NOTIFICATION BOOLEAN
);




create TABLE IF NOT EXISTS ALERT_SEARCH (
	REQUEST_ID NUMBER(38,0) NOT NULL autoincrement,
	EVENT_TIME TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))),
	USER_ID VARCHAR(16777216),
	USER_INPUT VARCHAR(16777216),
	SNOWSQL VARCHAR(16777216),
	INDEX_NAME VARCHAR(16777216),
	SEARCH_TITLE VARCHAR(16777216),
	SOURCE_COLUMNS VARCHAR(16777216),
	SOURCE_COLUMNS_JSON VARCHAR(16777216)
);








CREATE  VIEW  IF NOT EXISTS VW_ALERT_RULES as
Select 
ALERT_RULES.ID::varchar as ID,
ALERT_RULES.CATEGORY,
ALERT_RULES.TRIGGER_TYPE,
ALERT_RULES.BEHAVIOUR_TYPE,
ALERT_RULES.SOURCE_NM,
ALERT_RULES.CONTEXT,
case when replace(ALERT_RULES.CONDITION,'"','''') = '()' then NULL else replace(ALERT_RULES.CONDITION,'"','''') end as CONDITION,
ALERT_RULES.FEATURE,
ALERT_RULES.METRIC,
ALERT_RULES.OPERATOR,
ALERT_RULES.THRESHOLD,
ALERT_RULES.ARITHMETIC,
ALERT_RULES.MEASURE,
ALERT_RULES.TIMEFRAME_VAL,
ALERT_RULES.TIMEFRAME_UNIT,
ALERT_RULES.TRIGGER_FREQUENCY,
ALERT_RULES.SEVERITY,
ALERT_RULES.DESCRIPTION,
ALERT_RULES.COMMENT,
ALERT_RULES.ENABLE_NOTIFICATION,
ALERT_RULES.EMAIL,
ALERT_RULES.ROLLUP_TIME_VAL,
ALERT_RULES.ROLLUP_TIME_UNIT,

COALESCE(ALERT_RULES.INFO_JSON_RPLC,'') as INFO_JSON_RPLC,
ALERT_RULES.SOURCE_NM as ODM_NM,
'GUID' as GUID_RPLC,
'EVENT_TIME' as EVENT_TIME_RPLC,
'COMMIT_TIME' as COMMIT_TIME_RPLC,
COALESCE(ALERT_RULES.INFO_RPLC,'') as INFO_RPLC,
ALERT_RULES.SOURCE_NM as ALIAS,

ALERT_RULES.REQUEST_ID,   ----------- CAN BE REMOVED

 CASE WHEN UPPER(ALERT_RULES.category) = 'RULE' THEN ('FOR ' || ALERT_RULES.SOURCE_NM ) -- POLUPLATE SOURCE NAME FOR RULE BASED
  ELSE  --- START OF GENERIC CODE FOR BEHAVIOUR ALERTS
                 (
                 'WHEN '
                 || CASE WHEN UPPER(BEHAVIOUR_type) = 'RELATIVE' THEN ''   ELSE  -- NO METIC FOR RELATIVE ALERTS
                                             (CASE WHEN UPPER(METRIC)  = 'COUNT' AND UPPER(ALERT_RULES.FEATURE_DATATYPE) = 'STRING' then ('Number of occurrences') else METRIC  END || ' OF ')
                              END
                 || FEATURE
                 ||' FOR '
                 || CASE WHEN UPPER(ALERT_RULES.CONTEXT) = 'GLOBAL' THEN 'ALL RECORDS ' ELSE 'EACH ' 
                 || ALERT_RULES.CONTEXT || ' ' END
                 || OPERATOR || ' ' 
                 || CASE WHEN UPPER(BEHAVIOUR_type) = 'RELATIVE' --CUSTOM CODE FOR RELATIVE ALERTS FOR THRESHOLD
                              THEN ('IT''S ' || MEASURE || ' '|| ARITHMETIC || ' ' ||  (case when  RIGHT(to_varchar(THRESHOLD),2) = '00' THEN  to_varchar(CAST(THRESHOLD AS INTEGER)) ELSE to_varchar(THRESHOLD) END )) 
                               ELSE (CASE WHEN  RIGHT(to_varchar(THRESHOLD),2) = '00' THEN  to_varchar(CAST(THRESHOLD AS INTEGER)) ELSE to_varchar(THRESHOLD) END ) -- REMOVING .00 IF A THRESHOLD IS A WHOLE NUMBER
                              END  
                 || ' IN LAST ' || TIMEFRAME_VAL || ' ' || TIMEFRAME_UNIT 
                 || CASE WHEN UPPER(trigger_type) = 'EVENT' and UPPER(ALERT_RULES.category) = 'BEHAVIOUR' THEN ' FOR EACH RESULT' ELSE '' END
                 ) 
  END --- END OF GENERIC CODE FOR BEHAVIOUR ALERTS
  || CASE WHEN replace(ALERT_RULES.CONDITION,'"','''') = '()' then ' ' else ' WHERE ' 
  ----CASE STATEMENT FOR KQL IN WHERE CLAUSE
  || (CASE WHEN ALERT_RULES.KQL_INPUT IS NOT NULL THEN 'KQL Query is ' || ALERT_RULES.KQL_INPUT ELSE 'Filter Query is ' || replace(ALERT_RULES.CONDITION,'"','''') END) 
     END
  as COND_PREVIEW
from ALERT_RULES
where IS_DELETED = FALSE AND STATUS = 'ACTIVE';




CREATE VIEW IF NOT EXISTS VW_ALERT_RULES_SQL as
Select id,category,trigger_type,trigger_frequency,'INSERT INTO ALERT_TBL(EVENT_TIME,COMMIT_TIME,ALERT_RULE_ID,ALERT_TYPE,SOURCE_NM,CONTEXT,TRIGGER_TYPE,INFO,INSERTION_TS)
With dist_time as 
(
Select distinct(dateadd(second, 1, date_trunc(''seconds'',' || EVENT_TIME_RPLC || '))) as difftime
from ' || ODM_NM || ' where ' || COMMIT_TIME_RPLC || ' > coalesce((Select max(commit_time) from ALERT_TBL_INGEST_TRACK where odm_nm = ''' || ODM_NM || '''), date_trunc(''day'',convert_timezone(''America/Los_Angeles'', ''UTC'', current_timestamp())))
' || case when CONDITION is not NULL then ' and ' || CONDITION else '' end || case when FEATURE = '*' then '' else ' and ' || FEATURE  || ' is not NULL' end || 
case when UPPER(CONTEXT) <> 'GLOBAL' then ' and ' || CONTEXT || ' is not NULL' else '' end || '
),
agg as
(
Select ' || case when UPPER(CONTEXT) <> 'GLOBAL' then context else '''' || context || '''' end || ' as context, difftime, ' 
|| case when case when UPPER(BEHAVIOUR_TYPE) = 'ABSOLUTE' then metric else measure end = 'P90' 
then 'percentile_cont(0.9) within group (order by ' || feature || ')' when case when UPPER(BEHAVIOUR_TYPE) = 'ABSOLUTE' then metric else measure end = 'P10'
then 'percentile_cont(0.1) within group (order by ' || feature || ')' when case when UPPER(BEHAVIOUR_TYPE) = 'ABSOLUTE' then metric else measure end = 'Rate'
then '(count(case when '|| coalesce(condition,'1=1') || ' then 1 end)/count(*))*100'
else case when UPPER(BEHAVIOUR_TYPE) = 'ABSOLUTE' then metric else measure end || '(' || feature || ')' end || ' as metric
from ' || ODM_NM || '
left join dist_time where ' || EVENT_TIME_RPLC || ' between dateadd(''' || TIMEFRAME_UNIT || ''', - ' || TIMEFRAME_VAL || ',difftime) and difftime'
|| case when CONDITION is not NULL and (case when UPPER(BEHAVIOUR_TYPE) = 'ABSOLUTE' then metric else measure end <> 'Rate')  then ' and ' || CONDITION else '' end 
|| case when FEATURE = '*' then '' else ' and ' || FEATURE  || ' is not NULL' end || 
case when UPPER(CONTEXT) <> 'GLOBAL' then ' and ' || CONTEXT || ' is not NULL' else '' end || ' 
group by 1,2
)
Select EVENT_TIME, COMMIT_TIME, ALERT_ID, ALERT_TYPE,''' ||  SOURCE_NM || ''',RULE_CONTEXT, TRIGGER_TYPE,' || 
'object_construct(''AGGREGATE VALUE'',agg.metric' || 
case when contains(UPPER(INFO_RPLC),UPPER(GUID_RPLC)) then '' else ',''' || GUID_RPLC || ''',' || GUID_RPLC end ||
case when contains(UPPER(INFO_RPLC),UPPER(FEATURE)) or UPPER(FEATURE) = UPPER(GUID_RPLC) or FEATURE = '*' then '' else ',''' || FEATURE || ''',' || FEATURE end || 
case when contains(UPPER(INFO_JSON_RPLC),UPPER('''' || CONTEXT || '''')) or UPPER(CONTEXT) = UPPER(GUID_RPLC) or UPPER(CONTEXT) = 'GLOBAL' then '' else ',''' || CONTEXT || ''',' || CONTEXT end ||  
case when INFO_JSON_RPLC = '' then '' else ',' || INFO_JSON_RPLC end || ') as info,
convert_timezone(''America/Los_Angeles'', ''UTC'', current_timestamp()) as INSERTION_TS from
(
Select ' || EVENT_TIME_RPLC || ' as EVENT_TIME,' || COMMIT_TIME_RPLC || ' as COMMIT_TIME,''' 
|| ID || ''' as ALERT_ID,''' || CATEGORY || ''' as ALERT_TYPE,''' || CONTEXT || ''' as RULE_CONTEXT,' || 
'''EVENT'' as TRIGGER_TYPE,' || case when CONTAINS(UPPER(INFO_RPLC),UPPER(GUID_RPLC)) then '' else GUID_RPLC  end ||
case when contains(UPPER(INFO_RPLC),UPPER(FEATURE)) or UPPER(FEATURE) = UPPER(GUID_RPLC) or FEATURE = '*' then '' else ',' || FEATURE end ||  
case when contains(UPPER(INFO_RPLC),UPPER(CONTEXT)) or UPPER(CONTEXT) = UPPER(GUID_RPLC) or UPPER(CONTEXT) = 'GLOBAL'  then '' else  
 ',' || CONTEXT  end ||  
case when INFO_RPLC = '' then '' else  ','|| INFO_RPLC end ||  ' from ' || ODM_NM || 
' where ' || COMMIT_TIME_RPLC || ' > coalesce((Select max(commit_time) from ALERT_TBL_INGEST_TRACK where odm_nm = ''' || ODM_NM || '''), date_trunc(''day'',convert_timezone(''America/Los_Angeles'', ''UTC'', current_timestamp())))' ||
case when FEATURE = '*' then '' else ' and ' || FEATURE  || ' is not NULL' end ||
case when CONDITION is not NULL then ' and ' || CONDITION else '' end || '
) base_tbl
left join agg on 
agg.difftime = dateadd(second, 1, date_trunc(''seconds'',' || EVENT_TIME_RPLC || '))' ||
case when UPPER(CONTEXT) <> 'GLOBAL' then ' and agg.context = base_tbl.' || context else '' end || '
where ' 
|| case when UPPER(BEHAVIOUR_TYPE) = 'ABSOLUTE' then 'metric ' || OPERATOR || ' ' || THRESHOLD 
else FEATURE || ' ' || OPERATOR || ' ' || THRESHOLD || ' ' || ARITHMETIC || ' METRIC' end || ';'
as sql_query 
from VW_ALERT_RULES 
where UPPER(category) = 'BEHAVIOUR' and  UPPER(trigger_type) = 'EVENT'
UNION ALL
Select id,category,trigger_type,trigger_frequency, 'INSERT INTO ALERT_TBL(EVENT_TIME,COMMIT_TIME,ALERT_RULE_ID,ALERT_TYPE,SOURCE_NM,CONTEXT,TRIGGER_TYPE,INFO,INSERTION_TS)   
With agg as
(
Select ' || case when UPPER(CONTEXT) <> 'GLOBAL' then context else '''' || context || '''' end || ' as context, 
dateadd(''' || TIMEFRAME_UNIT || ''', - ' || TIMEFRAME_VAL || ',convert_timezone(''America/Los_Angeles'', ''UTC'', date_trunc(''minute'',current_timestamp()))) as st_time,convert_timezone(''America/Los_Angeles'', ''UTC'', date_trunc(''minute'',current_timestamp())) as difftime, ' || 

case when case when UPPER(BEHAVIOUR_TYPE) = 'ABSOLUTE' then metric else measure end = 'P90' 
then 'percentile_cont(0.9) within group (order by ' || feature || ')' when case when UPPER(BEHAVIOUR_TYPE) = 'ABSOLUTE' then metric else measure end = 'P10'
then 'percentile_cont(0.1) within group (order by ' || feature || ')' when case when UPPER(BEHAVIOUR_TYPE) = 'ABSOLUTE' then metric else measure end = 'Rate'
then '(count(case when '|| coalesce(condition,'1=1') || ' then 1 end)/count(*))*100'
else case when UPPER(BEHAVIOUR_TYPE) = 'ABSOLUTE' then metric else measure end || '(' || feature || ')' end || ' as metric,
max(' || COMMIT_TIME_RPLC || ') as max_commit, count(*) as event_count
from ' || ODM_NM || '
where ' || EVENT_TIME_RPLC || ' between st_time and difftime'
|| case when CONDITION is not NULL  and (case when UPPER(BEHAVIOUR_TYPE) = 'ABSOLUTE' then metric else measure end <> 'Rate') then ' and ' || CONDITION else '' end || case when FEATURE = '*' then '' else ' and ' || FEATURE  || ' is not NULL' end || 
case when UPPER(CONTEXT) <> 'GLOBAL' then ' and ' || CONTEXT || ' is not NULL' else '' end || ' 
group by 1,2,3)
' 
|| case when UPPER(BEHAVIOUR_TYPE) = 'RELATIVE' then 
',overall_agg as (
Select difftime, ' || measure || '(' || feature || ') ' || ARITHMETIC || ' ' || THRESHOLD || ' as overall_metric from ' || ODM_NM || ' 
left join dist_time where ' || EVENT_TIME_RPLC || ' between dateadd(''' || TIMEFRAME_UNIT || ''', - ' || TIMEFRAME_VAL || ',difftime) and difftime' || 
case when CONDITION is not NULL then ' and ' || CONDITION else '' end || case when FEATURE = '*' then '' else ' and ' || FEATURE  || ' is not NULL' end || 
case when UPPER(CONTEXT) <> 'GLOBAL' then ' and ' || CONTEXT || ' is not NULL' else '' end || ' 
group by 1
)
Select agg.difftime, agg.max_commit, ''' || ID || ''' as ALERT_ID,''' || CATEGORY || ''' as ALERT_TYPE, ''' ||  SOURCE_NM || ''', ''' || CONTEXT || ''' as RULE_CONTEXT,''' || TRIGGER_TYPE ||
''' as TRIGGER_TYPE, object_construct(''start time'', agg.st_time, ''end time'', agg.difftime, ''no of events'', agg.event_count, ''AGGREGATE VALUE'' , agg.metric,''Global Aggregate Value'', overall_agg.overall_metric' || case when UPPER(CONTEXT) = 'GLOBAL' then '' else ',''' || CONTEXT || ''', agg.context' end || ') as info,convert_timezone(''America/Los_Angeles'', ''UTC'', current_timestamp()) as INSERTION_TS from agg 
join overall_agg on agg.difftime=overall_agg.difftime where agg.metric ' || OPERATOR || ' overall_agg.overall_metric' 
else
'Select difftime, max_commit, ''' || ID || ''' as ALERT_ID,''' || CATEGORY || ''' as ALERT_TYPE, ''' ||  SOURCE_NM || ''', ''' || CONTEXT || ''' as RULE_CONTEXT,''' || TRIGGER_TYPE || 
''' as TRIGGER_TYPE, object_construct(''start time'', st_time, ''end time'', difftime, ''no of events'', event_count, ''AGGREGATE VALUE'' , metric' || case when UPPER(CONTEXT) = 'GLOBAL' then '' else ',''' || CONTEXT || ''', agg.context' end || ') as info,
convert_timezone(''America/Los_Angeles'', ''UTC'', current_timestamp()) as INSERTION_TS 
from agg where metric ' || OPERATOR || ' ' || THRESHOLD end || ';' as sql_query 
from VW_ALERT_RULES 
where UPPER(category) = 'BEHAVIOUR' and  UPPER(trigger_type) = 'INDEPENDENT'
UNION ALL
Select id,category,trigger_type,trigger_frequency,'INSERT INTO ALERT_TBL(EVENT_TIME,COMMIT_TIME,ALERT_RULE_ID,ALERT_TYPE,SOURCE_NM,CONTEXT,TRIGGER_TYPE,INFO,INSERTION_TS) 
Select ' || EVENT_TIME_RPLC || ' as event_time, ' || COMMIT_TIME_RPLC || ' as commit_time, ''' || ID || ''' as ALERT_ID, ''' 
|| CATEGORY || ''' as ALERT_TYPE, ''' ||  SOURCE_NM || ''', ' || case when CONTEXT is NULL or CONTEXT = '' then '''GLOBAL''' else '''' || CONTEXT || '''' end
|| ' as CONTEXT, ' || case when TRIGGER_TYPE is not NULL then '''' || TRIGGER_TYPE || '''' else 'NULL' end || ' as TRIGGER_TYPE,' ||
'object_construct(' ||  INFO_JSON_RPLC || case when INFO_JSON_RPLC <> '' then ',' else '' end ||
case when contains(UPPER(INFO_RPLC),UPPER(GUID_RPLC)) then '' else '''' || GUID_RPLC || ''',' || GUID_RPLC end || 
case when contains(UPPER(INFO_JSON_RPLC),UPPER('''' || CONTEXT || '''')) or UPPER(CONTEXT) = UPPER(GUID_RPLC) or UPPER(CONTEXT) = 'GLOBAL' or CONTEXT is NULL or context = '' 
then '' else ',''' || CONTEXT || ''',' || CONTEXT end || ') as info, convert_timezone(''America/Los_Angeles'', ''UTC'',current_timestamp()) as INSERTION_TS
from ' ||  ODM_NM ||  ' where ' || COMMIT_TIME_RPLC || ' > coalesce((Select max(commit_time) from ALERT_TBL_INGEST_TRACK where odm_nm = ''' || ODM_NM || '''), date_trunc(''day'',convert_timezone(''America/Los_Angeles'', ''UTC'', current_timestamp()))) and '
|| CONDITION as sql_query 
from VW_ALERT_RULES
where UPPER(category) = 'RULE';




create view IF NOT EXISTS VW_ALERT_APP_TBL_L3 as 
select 
parent_alert_id as ID,
parent_key ,
ALT.parent_key:alert_type::STRING as alert_NAME,
ALT.ALERT_NAME AS ALERT_TYPE,
ALT.DELETED_FLAG,
ALT.PRIORITY,
ALT.ENABLE_NOTIFICATION,
ALT.DESCRIPTION,
ALT.MODIFIED_TS,
EVENT_DATE,
EMAIL1.EMAIL
from 
ALERT_APP_TBL ALT
join VW_ALERT_RULES EMAIL1 on ALT.parent_key:alert_type::STRING=EMAIL1.ID::varchar;








CREATE OR REPLACE FUNCTION FN_NP_GET_ALERT_GROUP_NOTIFY_DATA("INPUT_MODIFIED_TS" VARCHAR(16777216))
RETURNS TABLE ("PARENT_ALERT_ID" VARCHAR(16777216), "PARENT_KEY" VARIANT, "ID" VARCHAR(16777216), "CONTEXT" VARCHAR(16777216), "ALERT_RULE" VARCHAR(16777216), "EVENT_DATE" TIMESTAMP_NTZ(9), "COMMIT_TIME" TIMESTAMP_NTZ(9), "PRIORITY" VARCHAR(16777216), "ALERT_TITLE" VARCHAR(16777216), "ALERT_TYPE" VARCHAR(16777216), "DESCRIPTION" VARIANT, "DELETED_FLAG" BOOLEAN, "MODIFIED_TS" TIMESTAMP_NTZ(9), "ENABLE_NOTIFICATION" BOOLEAN, "GROUP_ID" NUMBER(38,0), "EMAIL" VARCHAR(16777216), "ROLLUP_TIME_VAL" NUMBER(38,0), "ROLLUP_TIME_UNIT" VARCHAR(16777216))
LANGUAGE SQL
AS '
with base as
(
select 
*,
PARENT_KEY:"parent_id" :: varchar as context,
PARENT_KEY:"alert_type" :: varchar as alert_rule,
lag(event_date) over(partition by alert_name,context order by event_date) prev_event_date,
coalesce(datediff(''secs'',prev_event_date,event_date),0) as time_diff
from alert_app_tbl
where modified_ts > INPUT_MODIFIED_TS and enable_notification = TRUE
),
running_sum as
(
select 
*, 
sum(time_diff) over (partition by alert_name,context order by event_date, prev_event_date) as running_sum
from base
),
view_def as
(
select id, email, rollup_time_val, rollup_time_unit, 
case when (rollup_time_unit is NULL or rollup_time_val is NULL) then NULL 
when rollup_time_unit = ''Hours'' then rollup_time_val * 60 * 60 
when rollup_time_unit = ''Minutes'' then rollup_time_val * 60 end as roll_window
from VW_ALERT_RULES
)
select r.parent_alert_id,r.parent_key, r.id,r.context, r.alert_rule, r.event_date,r.commit_time,r.priority,r.alert_title,r.alert_type,r.description,
r.deleted_flag,r.modified_ts,r.enable_notification, 
case when roll_window is NULL then row_number() over (order by event_date) 
  else floor(running_sum/roll_window) end as group_id, v.email, v.rollup_time_val, v.rollup_time_unit
from running_sum r join view_def v on r.alert_rule = v.id::varchar';




CREATE OR REPLACE FUNCTION FN_NP_GET_ALERT_GROUPS_NOTIFY("INPUT_MODIFIED_TS" VARCHAR(16777216))
RETURNS TABLE ("EVENT_DATE" TIMESTAMP_NTZ(9), "ID" VARCHAR(16777216), "PARENT_KEY" VARIANT, "ALERT_RULE" VARCHAR(16777216), "PARENT_ID" VARCHAR(16777216), "SRC" VARCHAR(16777216), "ALERT_TYPE" VARCHAR(16777216), "DESCRIPTION" VARIANT, "PRIORITY" VARCHAR(16777216), "GROUP_ID" NUMBER(38,0), "DELETED_FLAG" BOOLEAN, "ENABLE_NOTIFICATION" BOOLEAN, "EMAIL" VARCHAR(16777216), "ROLLUP_TIME_VAL" NUMBER(38,0), "ROLLUP_TIME_UNIT" VARCHAR(16777216), "MODIFIED_TS" TIMESTAMP_NTZ(9), "ROW_COUNT" NUMBER(38,0))
LANGUAGE SQL
AS '
SELECT 
min(EVENT_DATE) as min_event_date,
id11 as id,
parent_key,                    
parent_key:alert_type ::varchar as alert_type, 
parent_key:parent_id ::varchar as parent_id,
parent_key:src ::varchar as src,
alert_type as alert_type1,
description11,
---description, 
priority,
group_id,
deleted_flag,
enable_notification,
email,
rollup_time_val, 
rollup_time_unit,
max(modified_ts),
count(*) as row_count from
(Select *,first_value(description) over (partition by group_id order by event_date nulls last) as description11,
first_value(id) over (partition by group_id order by event_date nulls last) as id11
FROM table(FN_NP_GET_ALERT_GROUP_NOTIFY_DATA(INPUT_MODIFIED_TS)))
group by 2,3,4,5,6,7,8,9,10,11,12,13,14,15';




CREATE OR REPLACE FUNCTION FN_NP_GET_ALERT_GROUP_NOTIFY_UI_DATA("INPUT_EVENT_DATE" VARCHAR(16777216), "ROLL_UP_UNIT" VARCHAR(16777216), "ROLL_UP_VALUE" NUMBER(38,0), "INPUT_CONTEXT" VARCHAR(16777216), "INPUT_ALERT_RULE" VARCHAR(16777216))
RETURNS TABLE ("ALERT_TITLE" VARCHAR(16777216), "PARENT_ID" VARCHAR(16777216), "ALERT_KEY" VARCHAR(16777216), "ALERT_TYPE" VARCHAR(16777216), "PARENT_ALERT_ID" VARCHAR(16777216), "ROLL_UP_UNIT" VARCHAR(16777216), "ROLL_UP_VAL" NUMBER(38,0), "ID" VARCHAR(16777216), "DESCRIPTION" VARIANT, "PRIORITY" VARCHAR(16777216), "EVENT_DATE" TIMESTAMP_NTZ(9), "COMMIT_TIME" TIMESTAMP_NTZ(9), "MODIFIED_TS" TIMESTAMP_NTZ(9), "DELETED_FLAG" BOOLEAN, "COMMENT" VARCHAR(16777216), "ALERT_RULE_DESCRIPTION" VARCHAR(16777216))
LANGUAGE SQL
AS '
With rollup_time as
(Select INPUT_EVENT_DATE as min_rollup_time, 
case when (ROLL_UP_UNIT =''NA'') then NULL 
when ROLL_UP_UNIT = ''Hours'' then ROLL_UP_VALUE * 60 * 60 
when ROLL_UP_UNIT = ''Minutes'' then ROLL_UP_VALUE * 60 end as roll_window,
case when roll_window is NULL then INPUT_EVENT_DATE else dateadd(''seconds'',roll_window,INPUT_EVENT_DATE) end as max_rollup_time,description as alert_rule_description
,comment
from VW_ALERT_RULES where ID = INPUT_ALERT_RULE)
SELECT 
alert_title, 
parent_key:parent_id :: string as parent_id,
parent_key:alert_type :: string as alert_key,
alert_type,
parent_alert_id,
ROLL_UP_UNIT,
ROLL_UP_VALUE as roll_up_val,
id,
description,
priority,
event_date,
commit_time,
modified_ts,
deleted_flag,
rollup_time.comment,
rollup_time.alert_rule_description
from alert_app_tbl 
join rollup_time on 1=1 
where event_date between min_rollup_time and max_rollup_time
and parent_key:parent_id = INPUT_CONTEXT
and parent_key:alert_type = INPUT_ALERT_RULE';








CREATE OR REPLACE PROCEDURE SP_ALERT_TRIGGER()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try
{

	snowflake.execute( {sqlText:''BEGIN''});

	var alert_query_resultset = snowflake.execute( {sqlText: `Select SQL_QUERY from VW_ALERT_RULES_SQL where trigger_frequency = ''Real-time'' or category = ''rule'';`});
	// Loop through the results, processing one row at a time... 
	while (alert_query_resultset.next())  {
		var insert_sql_query = alert_query_resultset.getColumnValue(1);
	    snowflake.execute({sqlText: insert_sql_query});
	}

	var source_odm_resultset = snowflake.execute( {sqlText: `Select DISTINCT SOURCE_NM,''COMMIT_TIME'' AS COMMIT_TIME_RPLC from ALERT_RULES;`});
	// Loop through the results, processing one row at a time... 
	while (source_odm_resultset.next())  {

		var update_query = `merge into ALERT_TBL_INGEST_TRACK using (SELECT DISTINCT SOURCE_NM FROM ALERT_RULES WHERE SOURCE_NM = ''` + source_odm_resultset.getColumnValue(1) + `'') AS source_table
    on UPPER(ALERT_TBL_INGEST_TRACK.ODM_NM) = UPPER(source_table.SOURCE_NM)
    when matched then 
        UPDATE  set commit_time = (Select max(` + source_odm_resultset.getColumnValue(2) + `) from ` + source_odm_resultset.getColumnValue(1) + `)
    when not matched then 
        insert (ODM_NM, COMMIT_TIME) values (''` + source_odm_resultset.getColumnValue(1) + `'', (Select max(` + source_odm_resultset.getColumnValue(2) + `) from ` + source_odm_resultset.getColumnValue(1) + `)); `;
	    snowflake.execute({sqlText: update_query});
	}

	snowflake.execute( {sqlText:''COMMIT''});
}
catch(err)
{
	snowflake.execute({sqlText: "ROLLBACK"});
	return err;
}
return "Succeeded";
';




CREATE OR REPLACE PROCEDURE SP_ALERT_APP_INSERT()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '

var sql_command = `Insert into ALERT_APP_TBL(PARENT_ALERT_ID,ID,PARENT_KEY,ALERT_NAME,EVENT_DATE, COMMIT_TIME, PRIORITY, ENABLE_NOTIFICATION, 
ALERT_TITLE,ALERT_TYPE,DESCRIPTION,ASSIGNED,STATUS,MODIFIED_BY,DELETED_FLAG,MODIFIED_TS)
select
 CASE WHEN ALERT_TBL.CONTEXT = ''GLOBAL'' THEN ''GLOBAL''
WHEN ALERT_TBL.CONTEXT IS NOT NULL THEN
GET_PATH(ALERT_TBL.INFO,to_variant(ALERT_TBL.CONTEXT))
ELSE ALERT_TBL.ALERT_ID END||''|rule''||ALERT_TBL.ALERT_RULE_ID as PARENT_ALERT_ID,
ALERT_TBL.ALERT_ID AS ID,
object_construct(''src'',VW_ALERT_RULES.ALIAS,''parent_id'',
CASE WHEN ALERT_TBL.CONTEXT = ''GLOBAL'' THEN ''GLOBAL''
WHEN ALERT_TBL.CONTEXT IS NOT NULL THEN
GET_PATH(ALERT_TBL.INFO,to_variant(ALERT_TBL.CONTEXT))
ELSE ALERT_TBL.ALERT_ID END,
''alert_type'',ALERT_TBL.alert_rule_id, 
''alert_category'', ALERT_TYPE, ''context'', ALERT_TBL.context,''search_src'',lower(ODM_NM)) AS PARENT_KEY,
VW_ALERT_RULES.COND_PREVIEW  as ALERT_NAME,
EVENT_TIME,
COMMIT_TIME,
SEVERITY AS PRIORITY,
ENABLE_NOTIFICATION,
''Alert for : '' || VW_ALERT_RULES.ALIAS  as ALERT_TITLE,
VW_ALERT_RULES.COND_PREVIEW  as ALERT_TYPE,
Info as DESCRIPTION,
''Admin'' as ASSIGNED,
''Open'' as STATUS,
''NULL'' as MODIFIED_BY,
''0'' as DELETED_FLAG,
convert_timezone(''America/Los_Angeles'', ''UTC'',current_timestamp()) as MODIFIED_TS
FROM ALERT_TBL JOIN VW_ALERT_RULES on ALERT_TBL.ALERT_RULE_ID = VW_ALERT_RULES.ID 
where ALERT_TBL.INSERTION_TS > coalesce((Select max(modified_ts) from ALERT_APP_TBL),date_trunc(''day'',convert_timezone(''America/Los_Angeles'', ''UTC'',current_timestamp())));`	



var stmt = snowflake.createStatement({sqlText: sql_command});
stmt.execute();
return "Successful";
';




CREATE OR REPLACE PROCEDURE SP_ALERT_RULE_INDEP_TASK("ID_NUM" VARCHAR(16777216), "WAREHOUSE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '

var alert_status_stmt = snowflake.createStatement({sqlText: `Select ALERT_RULES.STATUS, ALERT_RULES.IS_DELETED, ALERT_RULES.CRON_EXPRESSION from ALERT_RULES 
where ID = ` + ID_NUM});

var result2 = alert_status_stmt.execute();
result2.next();
query_result2 = result2.getColumnValue(1);
query_result3 = result2.getColumnValue(2);
query_result4 = result2.getColumnValue(3);

snowflake.execute({sqlText: "BEGIN"});
if (query_result2 == ''ACTIVE'' && query_result3 === false) {
	
	var task_create_stmt = `CREATE OR REPLACE TASK ALERT_RULE_INDEP_TASK_` + ID_NUM + `\\nWAREHOUSE = STUBHUB_SYSADMIN_WH \\nSCHEDULE = ''USING CRON ` + query_result4 + ` UTC''\\nas call SP_INDEP_ALERT_TRIGGER(` + ID_NUM + `);`;

    snowflake.execute({sqlText: task_create_stmt});
    snowflake.execute({sqlText: `ALTER TASK ALERT_RULE_INDEP_TASK_` + ID_NUM + ` RESUME`});

} else if (query_result2 == ''INACTIVE'' && query_result3 === false) {
    snowflake.execute({sqlText: `ALTER TASK ALERT_RULE_INDEP_TASK_` + ID_NUM + ` SUSPEND`});
} else if (query_result3 === true) { 
    snowflake.execute({sqlText: `DROP TASK  ALERT_RULE_INDEP_TASK_` + ID_NUM});
}

snowflake.execute({sqlText: "COMMIT"});
return "Successful";
';




CREATE OR REPLACE PROCEDURE SP_INDEP_ALERT_TRIGGER("ID_NUM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try
{

    snowflake.execute( {sqlText:`BEGIN`});

	var alert_query_resultset = snowflake.execute({sqlText: `Select SQL_QUERY from VW_ALERT_RULES_SQL where id = ` + ID_NUM + `;`});
	alert_query_resultset.next();

	var insert_sql_query = alert_query_resultset.getColumnValue(1);
    snowflake.execute({sqlText: insert_sql_query});
    
    var procedure_query = `call SP_ALERT_APP_INDEP_INSERT(` + ID_NUM + `);`;
    snowflake.execute( {sqlText: procedure_query});

	snowflake.execute( {sqlText:`COMMIT`});
}
catch(err)
{
	snowflake.execute({sqlText: "ROLLBACK"});
	return err;
}
return "Succeeded";
';




CREATE OR REPLACE PROCEDURE SP_ALERT_APP_INDEP_INSERT("ID_NUM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '

var sql_command = `Insert into ALERT_APP_TBL(PARENT_ALERT_ID,ID,PARENT_KEY,ALERT_NAME,EVENT_DATE, COMMIT_TIME, PRIORITY, ENABLE_NOTIFICATION, 
ALERT_TITLE,ALERT_TYPE,DESCRIPTION,ASSIGNED,STATUS,MODIFIED_BY,DELETED_FLAG,MODIFIED_TS)
select
 CASE WHEN ALERT_TBL.CONTEXT = ''GLOBAL'' THEN ''GLOBAL''
WHEN ALERT_TBL.CONTEXT IS NOT NULL THEN
GET_PATH(ALERT_TBL.INFO,to_variant(ALERT_TBL.CONTEXT))
ELSE ALERT_TBL.ALERT_ID END||''|rule''||ALERT_TBL.ALERT_RULE_ID as PARENT_ALERT_ID,
ALERT_TBL.ALERT_ID AS ID,
object_construct(''src'',VW_ALERT_RULES.ALIAS,''parent_id'',
CASE WHEN ALERT_TBL.CONTEXT = ''GLOBAL'' THEN ''GLOBAL''
WHEN ALERT_TBL.CONTEXT IS NOT NULL THEN
GET_PATH(ALERT_TBL.INFO,to_variant(ALERT_TBL.CONTEXT))
ELSE ALERT_TBL.ALERT_ID END,
''alert_type'',ALERT_TBL.alert_rule_id, 
''alert_category'', ALERT_TYPE, ''context'', ALERT_TBL.context,''search_src'',lower(ODM_NM)) AS PARENT_KEY,
VW_ALERT_RULES.COND_PREVIEW  as ALERT_NAME,
EVENT_TIME,
COMMIT_TIME,
SEVERITY AS PRIORITY,
ENABLE_NOTIFICATION,
''Alert for : '' || VW_ALERT_RULES.ALIAS  as ALERT_TITLE,
VW_ALERT_RULES.COND_PREVIEW  as ALERT_TYPE,
Info as DESCRIPTION,
''Admin'' as ASSIGNED,
''Open'' as STATUS,
''NULL'' as MODIFIED_BY,
''0'' as DELETED_FLAG,
convert_timezone(''America/Los_Angeles'', ''UTC'',current_timestamp()) as MODIFIED_TS
FROM ALERT_TBL JOIN VW_ALERT_RULES on ALERT_TBL.ALERT_RULE_ID = VW_ALERT_RULES.ID 
and VW_ALERT_RULES.ID =` +  ID_NUM + `
where ALERT_TBL.INSERTION_TS > coalesce((Select max(modified_ts) from ALERT_APP_TBL where parent_key:alert_type::varchar = ''` +  ID_NUM + `''),date_trunc(''day'',convert_timezone(''America/Los_Angeles'', ''UTC'',current_timestamp())));`

var stmt = snowflake.createStatement({sqlText: sql_command});
stmt.execute();
return "Successful";
';

