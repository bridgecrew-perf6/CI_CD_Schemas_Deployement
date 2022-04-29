use schema dev_demo_schema;

create or replace TABLE MSEXCHANGE_ML_SCORE_BATCH (
	EVENT_DATE DATE,
	NAME VARCHAR(2000),
	TYPE VARCHAR(100),
	FEATURE_AGG VARIANT,
	ALERT_AGG VARIANT,
	TS_AGG VARIANT,
	ZSCORE VARIANT,
	PAS_KMEANS FLOAT,
	PAS_ISOLATION FLOAT,
	PAS_SVM FLOAT,
	PAS FLOAT,
	EXECUTION_TIMESTAMP NUMBER(38,0)
);

create or replace TABLE MSEXCHANGE_ML_SCORE_STREAM (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(5,0),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(10),
	TYPE VARCHAR(100),
	NAME VARCHAR(200),
	FEATURE_AGG VARIANT,
	ALERT_AGG VARIANT,
	ZSCORE VARIANT,
	PAS_KMEANS FLOAT,
	PAS_ISOLATION FLOAT,
	PAS_SVM FLOAT,
	PAS FLOAT,
	EXECUTION_TIMESTAMP NUMBER(38,0)
);


create or replace TABLE NP_MS_EXCH_MESSAGETRACKING_ML_SCORE_BATCH (
	EVENT_DATE DATE,
	NAME VARCHAR(2000),
	TYPE VARCHAR(100),
	FEATURE_AGG VARIANT,
	ALERT_AGG VARIANT,
	TS_AGG VARIANT,
	ZSCORE VARIANT,
	PAS_KMEANS FLOAT,
	PAS_ISOLATION FLOAT,
	PAS_SVM FLOAT,
	PAS FLOAT,
	EXECUTION_TIMESTAMP NUMBER(38,0)
);


create or replace TABLE NP_MS_WIN_SECURITYAUDITING_ML_SCORE_BATCH (
	EVENT_DATE DATE,
	NAME VARCHAR(2000),
	TYPE VARCHAR(100),
	FEATURE_AGG VARIANT,
	ALERT_AGG VARIANT,
	TS_AGG VARIANT,
	ZSCORE VARIANT,
	PAS_KMEANS FLOAT,
	PAS_ISOLATION FLOAT,
	PAS_SVM FLOAT,
	PAS FLOAT,
	EXECUTION_TIMESTAMP NUMBER(38,0)
);


create or replace TABLE NP_WG_FW_NETWORKTRAFFIC_ML_SCORE_BATCH (
	EVENT_DATE DATE,
	NAME VARCHAR(2000),
	TYPE VARCHAR(100),
	FEATURE_AGG VARIANT,
	ALERT_AGG VARIANT,
	TS_AGG VARIANT,
	ZSCORE VARIANT,
	PAS_KMEANS FLOAT,
	PAS_ISOLATION FLOAT,
	PAS_SVM FLOAT,
	PAS FLOAT,
	EXECUTION_TIMESTAMP NUMBER(38,0)
);


create or replace TABLE TEST_ML_SCORE_WGTRAFFIC_AGG (
	EVENT_TIME TIMESTAMP_NTZ(9),
	TYPE VARCHAR(30),
	ENTITY VARIANT,
	FEATURE_AGG VARIANT,
	ALERT_AGG VARIANT,
	CREATED_TS TIMESTAMP_NTZ(9) NOT NULL DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);


create or replace TABLE TEST_ML_SCORE_WINDOWSNXLOG_AGG (
	EVENT_TIME TIMESTAMP_NTZ(9),
	TYPE VARCHAR(30),
	ENTITY VARIANT,
	FEATURE_AGG VARIANT,
	ALERT_AGG VARIANT,
	CREATED_TS TIMESTAMP_NTZ(9) NOT NULL DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);


create or replace TABLE WGTRAFFIC_ML_SCORE_BATCH (
	EVENT_DATE DATE,
	NAME VARCHAR(2000),
	TYPE VARCHAR(100),
	FEATURE_AGG VARIANT,
	ALERT_AGG VARIANT,
	TS_AGG VARIANT,
	ZSCORE VARIANT,
	PAS_KMEANS FLOAT,
	PAS_ISOLATION FLOAT,
	PAS_SVM FLOAT,
	PAS FLOAT,
	EXECUTION_TIMESTAMP NUMBER(38,0)
);



create or replace TABLE WGTRAFFIC_ML_SCORE_STREAM (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(5,0),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(10),
	TYPE VARCHAR(100),
	NAME VARCHAR(200),
	FEATURE_AGG VARIANT,
	ALERT_AGG VARIANT,
	ZSCORE VARIANT,
	PAS_KMEANS FLOAT,
	PAS_ISOLATION FLOAT,
	PAS_SVM FLOAT,
	PAS FLOAT,
	EXECUTION_TIMESTAMP NUMBER(38,0)
);


create or replace TABLE WINDOWSNXLOG_ML_SCORE_BATCH (
	EVENT_DATE DATE,
	NAME VARCHAR(2000),
	TYPE VARCHAR(100),
	FEATURE_AGG VARIANT,
	ALERT_AGG VARIANT,
	TS_AGG VARIANT,
	ZSCORE VARIANT,
	PAS_KMEANS FLOAT,
	PAS_ISOLATION FLOAT,
	PAS_SVM FLOAT,
	PAS FLOAT,
	EXECUTION_TIMESTAMP NUMBER(38,0)
);

create or replace TABLE ANALYST_FEEDBACK (
	ANALYST_NAME VARCHAR(16777216),
	UE_NAME VARCHAR(16777216),
	UE_CATEGORY VARCHAR(16777216),
	UE_TYPE VARCHAR(16777216),
	TIME_FRAME VARCHAR(16777216),
	DURATION_FROM DATE,
	DURATION_TO DATE,
	SCORE_ADJ_TYPE VARCHAR(16777216),
	SCORE_ADJ_VALUE NUMBER(38,0),
	SCORE_ADJ_RANGE VARCHAR(16777216),
	SCORE_ADJ_PARAM VARCHAR(16777216),
	PROCESSING_DTTM TIMESTAMP_NTZ(9)
);


CREATE OR REPLACE FUNCTION "FN_ML_ANALYST_FB"("P_SOURCE_NM" VARCHAR(16777216), "P_ANALYST_NM" VARCHAR(16777216), "P_TYPE" VARCHAR(16777216))
RETURNS TABLE ("ANALYST_NAME" VARCHAR(16777216), "EVENT_DATE" DATE, "TYPE" VARCHAR(16777216), "USER_NAME" VARCHAR(16777216), "FEATURE_VAL" VARIANT, "ALERT_AGG" VARIANT, "TS_AGG" VARIANT, "ADJ_ZSCORE" VARIANT, "ADJ_ANOMALY_SCORE" FLOAT)
LANGUAGE SQL
AS '
with uore_type as
(
select value :: varchar as type from lateral flatten(input=>split((case when P_TYPE = ''both'' THEN ''user,entity'' else P_TYPE end), '',''))
),
ml_user_score as
(
  select analyst_name, event_date, type, user_name, anomaly_score, feature_val, alert_agg, ts_agg, feature_name, feature_zscore from
  (
    select 
    lower(P_ANALYST_NM) as analyst_name,
    event_date,
    type as type,
    name as user_name,
    pas as anomaly_score,
    feature_agg as feature_val,
    alert_agg as alert_agg,
	ts_agg as ts_agg,
    zsc.key as feature_name,
    zsc.value as feature_zscore,
    rank() over (partition by event_date, type, name order by execution_timestamp desc) as rnk 
    from
    windowsnxlog_ml_score_batch, table(flatten(zscore)) zsc
    where lower(P_SOURCE_NM) = ''windowsnxlog'' and type in (select type from uore_type) and zsc.value <> ''NaN''
  ) where rnk = 1
  UNION ALL
  select analyst_name, event_date, type, user_name, anomaly_score, feature_val, alert_agg, ts_agg, feature_name, feature_zscore from
  (
    select 
    lower(P_ANALYST_NM) as analyst_name,
    event_date,
    type as type,
    name as user_name,
    pas as anomaly_score,
    feature_agg as feature_val,
    alert_agg as alert_agg,
	ts_agg as ts_agg,
    zsc.key as feature_name,
    zsc.value as feature_zscore,
    rank() over (partition by event_date, type, name order by execution_timestamp desc) as rnk 
    from
    wgtraffic_ml_score_batch, table(flatten(zscore)) zsc
    where lower(P_SOURCE_NM) = ''wgtraffic'' AND type in (select type from uore_type) and zsc.value <> ''NaN''
  ) where rnk = 1
  UNION ALL
  select analyst_name, event_date, type, user_name, anomaly_score, feature_val, alert_agg, ts_agg, feature_name, feature_zscore from
  (
    select 
    lower(P_ANALYST_NM) as analyst_name,
    event_date,
    type as type,
    name as user_name,
    pas as anomaly_score,
    feature_agg as feature_val,
    alert_agg as alert_agg,
	ts_agg as ts_agg,
    zsc.key as feature_name,
    zsc.value as feature_zscore,
    rank() over (partition by event_date, type, name order by execution_timestamp desc) as rnk 
    from
    msexchange_ml_score_batch, table(flatten(zscore)) zsc
    where lower(P_SOURCE_NM) = ''msexchange'' AND type in (select type from uore_type) and zsc.value <> ''NaN''
  ) where rnk = 1
),
pas_fb as 
(
  select analyst_name, type, user_name, duration_from, duration_to, pas_adj_by
  from
  (
    select 
    analyst_name,
    ue_type as type,
    ue_name as user_name,
    score_adj_range,
    score_adj_param,
    case when time_frame = ''indefinite'' then ''1900-01-01'' else duration_from end as duration_from,
    case when time_frame = ''indefinite'' then ''2100-12-31'' else duration_to end as duration_to,
    case when score_adj_type = ''decrease'' then -1 * score_adj_value/100
       when score_adj_type = ''increase'' then score_adj_value/100
       when score_adj_type = ''ignore'' then -1
    end as pas_adj_by,
    rank() over (partition by analyst_name, type, ue_name order by processing_dttm desc) as rnk
    from ANALYST_FEEDBACK
    where analyst_name = lower(P_ANALYST_NM) and ue_category = ''individual'' and type in (select type from uore_type) and score_adj_range = ''overall''
  ) where rnk = 1
),
zsc_fb as 
(
  select analyst_name, type, user_name, duration_from, duration_to, score_adj_param as feature_name, zsc_adj_by
  from
  (
    select 
    analyst_name,
    ue_name as user_name,
    ue_type as type,
    score_adj_range,
    score_adj_param,
    case when time_frame = ''indefinite'' then ''1900-01-01'' else duration_from end as duration_from,
    case when time_frame = ''indefinite'' then ''2100-12-31'' else duration_to end as duration_to,
    case when score_adj_type = ''decrease'' then -1 * score_adj_value/100
         when score_adj_type = ''increase'' then score_adj_value/100
         when score_adj_type = ''ignore'' then -1
    end as zsc_adj_by,
    rank() over (partition by analyst_name, type, ue_name, score_adj_param order by processing_dttm desc) as rnk
    from ANALYST_FEEDBACK
    where analyst_name = lower(P_ANALYST_NM) and ue_category = ''individual'' and type in (select type from uore_type) and score_adj_range = ''feature''
  ) where rnk = 1
)
select 
mlz.analyst_name, 
mlz.event_date, 
mlz.type,
mlz.user_name,
mlz.feature_val,
mlz.alert_agg,
mlz.ts_agg,
mlz.adj_zscore,
mlz.anomaly_score + (mlz.anomaly_score * coalesce(pas.pas_adj_by,0)) as adj_anomaly_score
from(
  select 
  analyst_name,
  event_date,
  type,
  user_name,
  anomaly_score,
  feature_val,
  alert_agg,
  ts_agg,
  parse_json (''{''|| listagg(feature_name || '':'' || adj_feature_zscore,'', '') || ''}'') as adj_zscore
  from
  (
    select 
    ml.analyst_name,
    ml.event_date,
    ml.user_name,
    ml.type,
    ml.feature_name,
    ml.anomaly_score,
    ml.feature_val,
    ml.alert_agg,
    ml.ts_agg,
    ml.feature_zscore + (ml.feature_zscore * coalesce(zsc.zsc_adj_by,0)) as adj_feature_zscore
    from ml_user_score ml 
    left join zsc_fb zsc
      on  ml.analyst_name = zsc.analyst_name
      and ml.type = zsc.type
      and ml.user_name = zsc.user_name
      and ml.feature_name = zsc.feature_name
      and ml.event_date between zsc.duration_from and zsc.duration_to
  )group by 1,2,3,4,5,6,7,8
)mlz
left join pas_fb pas
  on  mlz.analyst_name = pas.analyst_name
  and mlz.type = pas.type
  and mlz.user_name  = pas.user_name
  and mlz.event_date between pas.duration_from and pas.duration_to
';


create or replace view NP_ITD_ML_SOURCES(
	ZSCORE,
	USER_NAME,
	ADJ_ANOMALY_SCORE,
	SOURCE,
	EVENT_DATE,
	TYPE
) as
  SELECT adj_zscore :: string AS zscore,
               user_name,
               adj_anomaly_score,
               'MS_WIN_SECURITYAUDITING'   AS source,
               event_date,
               TYPE
        FROM   TABLE(FN_NP_ML_ANALYST_FB('ms_win_securityauditing', 'analyst1', 'both'))
        where
        ADJ_ANOMALY_SCORE>=50
        UNION ALL
        SELECT adj_zscore :: string AS zscore,
               user_name,
               adj_anomaly_score,
               'WG_FW_NETWORKTRAFFIC'      AS source,
               event_date,
               TYPE
        FROM   TABLE(FN_NP_ML_ANALYST_FB('wg_fw_networktraffic', 'analyst1', 'both'))
        where
        ADJ_ANOMALY_SCORE>=50
        UNION ALL
        SELECT adj_zscore :: string AS zscore,
               user_name,
               adj_anomaly_score,
               'MS_EXCH_MESSAGETRACKING'     AS source,
               event_date,
               TYPE
        FROM   TABLE(FN_NP_ML_ANALYST_FB('ms_exch_messagetracking', 'analyst1', 'both'))
        where
        ADJ_ANOMALY_SCORE>=50
        UNION ALL
        SELECT adj_zscore :: string AS zscore,
               user_name,
               adj_anomaly_score,
               'OS_QUERY'     AS source,
               event_date,
               TYPE
        FROM   TABLE(FN_NP_ML_ANALYST_FB('os_query_loggedinusers', 'analyst1', 'both'))
        where
        ADJ_ANOMALY_SCORE>=50;
		
		
create or replace view NP_VIEW360IP(
	ID,
	EVENT_TIME,
	TIMEOFEVENT,
	SRC_IP,
	SRC_USER_NAME,
	EVENT_DESC,
	REPORTING_HOST,
	EVENT_ID,
	SUBJECT,
	SRC_TYPE,
	EMAIL_SIZE,
	IN_BYTES,
	OUT_BYTES,
	RISK,
	ALERTS_DESCRIPTION,
	HOSTNAME,
	EMAIL_AVG_SIZE_ZSCORE,
	EMAIL_COUNT_ZSCORE,
	EMAILS_SIZE_ZSCORE,
	EXT_RECEIVER_ZSCORE,
	IF_DWNLD_ZSCORE,
	IF_UPLD_ZSCORE,
	FAILEDLOGIN_ZSCORE,
	FILEACTIVITY_ZSCORE,
	LOGINS_ZSCORE,
	PRI_ZSCORE,
	PS_ZSCORE,
	ANOMALY_SCORE,
	RISK_SCORE,
	EVENT_USER_ANOMALY_SCORE,
	EVENT_ENTITY_ANOMALY_SCORE,
	DST_NAME,
	URLLNKDIN_ZSCORE,
	EMAIL_AVG_SIZE_TS,
	EMAIL_COUNT_TS,
	EMAILS_SIZE_TS,
	EXT_RECEIVER_TS,
	IF_DWNLD_TS,
	IF_UPLD_TS,
	FAILEDLOGIN_TS,
	FILEACTIVITY_TS,
	LOGINS_TS,
	PRI_TS,
	PS_TS,
	URLLNKDIN_TS,
	EMAIL_AVG_SIZE_VAL,
	EMAIL_COUNT_VAL,
	EMAILS_SIZE_VAL,
	EXT_RECEIVER_VAL,
	IF_DWNLD_VAL,
	IF_UPLD_VAL,
	FAILEDLOGIN_VAL,
	FILEACTIVITY_VAL,
	LOGINS_VAL,
	PRI_VAL,
	PS_VAL,
	URLLNKDIN_VAL,
	SRC_GEO_CITY,
	SRC_GEO_LAT,
	SRC_GEO_LONG,
	DISPLAYNAME,
	DESCRIPTION,
	EMAILADDRESS,
	ROW_N,
	QRY,
	RAW,
	OS_USERLOGONS_ZSCORE,
	OS_USERLOGOFFS_ZSCORE,
	OS_USERLOGONS_VAL,
	OS_USERLOGOFFS_VAL,
	SRC_DOMAIN
) as
select guid as id,cast(event_time as timestamp) as event_time,event_time as timeofevent,src_ip ,
SPLIT(lower(src_email_addr),'@')[0] :: STRING as src_user_name,
'Email Event Information'  as event_desc,dst_email_addr::String as reporting_host,
'NA' ::string AS event_id,email_subject as subject,'ms_exch_messagetracking' as src_type,
email_size :: string as email_size,'NA' ::string AS in_bytes,
'NA'  AS out_bytes,risk_desc::string as risk,risk_desc::string as alerts_description,
'NA' ::string AS hostname,'NA' ::string AS email_avg_size_zscore,'NA' ::string AS email_count_zscore,'NA' ::string AS emails_size_zscore ,
'NA' ::string AS ext_receiver_zscore,'NA' ::string AS if_dwnld_zscore,'NA' ::string AS if_upld_zscore,'NA' ::string AS failedlogin_zscore,
'NA' ::string AS fileactivity_zscore,'NA' ::string AS logins_zscore ,'NA' ::string AS pri_zscore,'NA' ::string AS ps_zscore,null AS anomaly_score,
null AS risk_score,event_user_anomaly_score,NULL AS event_entity_anomaly_score,'NA' ::string AS dst_name,'NA'  as urllnkdin_zscore,
null AS email_avg_size_ts,null AS email_count_ts,null AS emails_size_ts ,null  AS ext_receiver_ts,null AS if_dwnld_ts,null AS if_upld_ts,
null AS failedlogin_ts,null AS fileactivity_ts,null AS logins_ts ,null AS pri_ts,null AS ps_ts,null AS urllnkdin_ts,null AS email_avg_size_val,
null AS email_count_val,null AS emails_size_val ,null  AS ext_receiver_val,null AS if_dwnld_val,null AS if_upld_val,null AS failedlogin_val,
null AS fileactivity_val,null AS logins_val ,null AS pri_val,null AS ps_val,null AS urllnkdin_val,
'null' ::string as src_geo_city,'null' ::String as src_geo_lat,'null' ::string as src_geo_long,'NA' ::string AS DISPLAYNAME,
'NA' ::string AS DESCRIPTION,'NA' ::string AS EMAILADDRESS, null as row_n,qry::String as qry,raw::String as raw,   
'NA' ::string AS os_userlogons_zscore,
'NA' ::string AS os_userlogoffs_zscore,
null AS os_userlogons_val,
null AS os_userlogoffs_val
,src_domain
from VW_NP_MS_EXCH_MESSAGETRACKING_EVENT_ZD
where  event_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP)
 UNION ALL 
select guid :: string as id,
cast(event_time as timestamp) as event_time,
event_time as timeofevent,
src_ip :: string as src_ip,
src_user_name :: string as src_user_name,
event_desc :: string as event_desc,
'NA':: string as reporting_host,
event_id ::String as event_id,
CASE WHEN event_id = '4634' THEN 'logoff' WHEN event_id ='4624' THEN 'logon' ELSE '-' END as subject,
'ms_win_securityauditing' as src_type,
'NA' :: string as email_size ,'NA' :: string AS in_bytes,'NA' :: string AS out_bytes,
risk_desc :: string as risk,risk_desc::varchar as alerts_description,
RPT_host as hostname,'NA' ::string AS email_avg_size_zscore,
'NA' ::string AS email_count_zscore,'NA' ::string AS emails_size_zscore ,'NA' ::string AS ext_receiver_zscore,'NA' ::string AS if_dwnld_zscore,
'NA' ::string AS if_upld_zscore,'NA' ::string AS failedlogin_zscore,'NA' ::string AS fileactivity_zscore,'NA' ::string AS logins_zscore 
,'NA' ::string AS pri_zscore,'NA' ::string AS ps_zscore,null AS anomaly_score,null AS risk_score,
event_user_anomaly_score,event_entity_anomaly_score,'NA' ::string AS dst_name,'NA'  as urllnkdin_zscore,
null AS email_avg_size_ts,null AS email_count_ts,null AS emails_size_ts ,null  AS ext_receiver_ts,
null AS if_dwnld_ts,null AS if_upld_ts,null AS failedlogin_ts,null AS fileactivity_ts,null AS logins_ts ,
null AS pri_ts,null AS ps_ts,null AS urllnkdin_ts,null AS email_avg_size_val,null AS email_count_val,
null AS emails_size_val,null  AS ext_receiver_val,null AS if_dwnld_val,null AS if_upld_val,null AS failedlogin_val,
null AS fileactivity_val,null AS logins_val ,null AS pri_val,null AS ps_val,null AS urllnkdin_val,src_geo_city::String as src_geo_city
,src_geo_lat ::String as src_geo_lat,
src_geo_long::String as src_geo_long,'NA' ::string AS DISPLAYNAME,'NA' ::string AS DESCRIPTION,'NA' ::string AS EMAILADDRESS, null as row_n,qry::String as qry,raw::String as raw, 
'NA' ::string AS os_userlogons_zscore,
'NA' ::string AS os_userlogoffs_zscore,
null AS os_userlogons_val,
null AS os_userlogoffs_val
,src_domain   
from VW_NP_MS_WIN_SECURITYAUDITING_EVENT_ZD
where  event_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP)
 UNION ALL 
select guid as id,cast(event_time as timestamp) as event_time,event_time as timeofevent,src_ip ,src_user_name ,
'Watchguard Traffic Information' as event_desc,'NA' as reporting_host,'NA' as event_id,'NA' as subject,'wg_fw_networktraffic' as src_type,
'NA' AS email_size ,in_bytes:: string as in_bytes,out_bytes:: string as out_bytes,risk_desc ::String as risk,
risk_desc ::String as alerts_description,RPT_HOST as  hostname,'NA' AS email_avg_size_zscore,'NA' AS email_count_zscore,
'NA' AS emails_size_zscore ,'NA' AS ext_receiver_zscore,'NA' AS if_dwnld_zscore,'NA' AS if_upld_zscore,'NA' AS failedlogin_zscore,
'NA' AS fileactivity_zscore,'NA' AS logins_zscore ,'NA' AS pri_zscore,'NA' AS ps_zscore,null AS anomaly_score,null AS risk_score,
event_user_anomaly_score,event_entity_anomaly_score,dst_domain as dst_name,'NA'  as urllnkdin_zscore,null AS email_avg_size_ts,
null AS email_count_ts,null AS emails_size_ts ,null  AS ext_receiver_ts,null AS if_dwnld_ts,null AS if_upld_ts,null AS failedlogin_ts,
null AS fileactivity_ts,null AS logins_ts ,null AS pri_ts,null AS ps_ts,null AS urllnkdin_ts,null AS email_avg_size_val,null AS email_count_val,
null AS emails_size_val ,null  AS ext_receiver_val,null AS if_dwnld_val,null AS if_upld_val,null AS failedlogin_val,null AS fileactivity_val,
null AS logins_val ,null AS pri_val,null AS ps_val,null AS urllnkdin_val,src_geo_city::String as src_geo_city,src_geo_lat::string as src_geo_lat ,
src_geo_long::String as src_geo_long ,'NA' as DISPLAYNAME,'NA' as DESCRIPTION,
'NA' as EMAILADDRESS, null as row_n,qry::String as qry,raw::String as raw,   
'NA' ::string AS os_userlogons_zscore,
'NA' ::string AS os_userlogoffs_zscore,
null AS os_userlogons_val,
null AS os_userlogoffs_val
,src_domain   
from VW_NP_WG_FW_NETWORKTRAFFIC_EVENT_ZD
where  event_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP)
UNION ALL 
select * from (select 'NA' AS id,current_date() as event_time,current_date() as timeofevent,'NA' as src_ip,lower(ACCOUNT_NAME) as src_user_name, 
'NA' as event_desc,'NA' as reporting_host,'NA' as event_id,'NA' as subject,'ms_ad_users' as src_type,'NA' as email_size ,'NA' AS in_bytes,  
'NA'  AS out_bytes,'NA' as risk,'NA' as  alerts_description, 'NA' as hostname,'NA' AS email_avg_size_zscore,'NA' AS email_count_zscore,   
'NA' AS emails_size_zscore ,'NA' AS ext_receiver_zscore,'NA' AS if_dwnld_zscore,'NA' AS if_upld_zscore,'NA' AS failedlogin_zscore,    
'NA' AS fileactivity_zscore,'NA' AS logins_zscore ,'NA' AS pri_zscore,'NA' AS ps_zscore,null AS anomaly_score,null AS risk_score, 
null as event_user_anomaly_score,null as event_entity_anomaly_score,'NA' AS dst_name,'NA'  as urllnkdin_zscore,null AS email_avg_size_ts, 
null AS email_count_ts,null AS emails_size_ts ,null  AS ext_receiver_ts,null AS if_dwnld_ts,null AS if_upld_ts,null AS failedlogin_ts,    
null AS fileactivity_ts,null AS logins_ts ,null AS pri_ts,null AS ps_ts,null AS urllnkdin_ts,null AS email_avg_size_val,null AS email_count_val,  
null AS emails_size_val ,null  AS ext_receiver_val,null AS if_dwnld_val,null AS if_upld_val,null AS failedlogin_val,null AS fileactivity_val, 
null AS logins_val ,null AS pri_val,null AS ps_val,null AS urllnkdin_val,city as  src_geo_city,'NA' as src_geo_lat,'NA' as src_geo_long,DISPLAY_NAME as DISPLAYNAME, 
DESC as DESCRIPTION,EMAIL_ADDR as EMAILADDRESS, ROW_NUMBER() over ( partition by ACCOUNT_NAME order by CHANGED_TIME desc) as row_n ,'NA' as qry,'NA'  as raw,    
'NA' ::string AS os_userlogons_zscore,
'NA' ::string AS os_userlogoffs_zscore,
null AS os_userlogons_val,
null AS os_userlogoffs_val
,'sstech.com' as SRC_DOMAIN               
from MS_AD_USERS_ODM  
where   DESC!='') where row_n=1
 UNION ALL 
select 'NA' AS id,cast(event_date as date) as event_time,event_date as timeofevent,
IFF(type='entity',name,true) as src_ip,IFF(type='user',name,true) as src_user_name,
'NA' as event_desc,'NA'  as reporting_host,'NA' as event_id,'NA'  as subject,
'ml_aggregate' as src_type,'NA' AS email_size ,'NA' as in_bytes,'NA' as out_bytes,
'NA' as risk,'NA' as alerts_description,'NA' as hostname,
coalesce(max(zscore:email_avg_size),'NA') :: string AS email_avg_size_zscore,
coalesce(max(zscore:email_count),'NA') :: string AS email_count_zscore,
coalesce(max(zscore:email_size),'NA') :: string AS emails_size_zscore ,
coalesce(max(zscore:ext_receiver_count),'NA') :: string AS ext_receiver_zscore,
coalesce(max(zscore:if_dwnld_count),'NA') :: string AS if_dwnld_zscore,
coalesce(max(zscore:if_upld_count),'NA') :: string AS if_upld_zscore,
coalesce(max(zscore:failedlogin_count),'NA') :: string AS failedlogin_zscore,
coalesce(max(zscore:fileactivity_count),'NA') :: string AS fileactivity_zscore,
coalesce(max(zscore:logins_count),'NA') :: string AS logins_zscore ,coalesce(max(zscore:pri_count),'NA') :: string AS pri_zscore,
coalesce(max(zscore:ps_count),'NA') :: string AS ps_zscore,max(MAX_ANOMALY_SCORE)  AS anomaly_score,max(RISK_SCORE)  AS risk_score,
null as event_user_anomaly_score,null as event_entity_anomaly_score,'NA' AS dst_name,
coalesce(max(zscore:urllnkdin_count),'NA') :: string AS urllnkdin_zscore,
max(ts_agg:email_avg_size) :: numeric AS email_avg_size_ts,max(ts_agg:email_count) :: numeric AS email_count_ts,
max(ts_agg:email_size) :: numeric AS emails_size_ts ,max(ts_agg:ext_receiver_count) :: numeric AS ext_receiver_ts,
max(ts_agg:if_dwnld_count) :: numeric AS if_dwnld_ts,max(ts_agg:if_upld_count) :: numeric AS if_upld_ts,
max(ts_agg:failedlogin_count) :: numeric AS failedlogin_ts,max(ts_agg:fileactivity_count) :: numeric AS fileactivity_ts,
max(ts_agg:logins_count) :: numeric AS logins_ts ,max(ts_agg:pri_count) :: numeric AS pri_ts,max(ts_agg:ps_count) :: numeric AS ps_ts,
max(ts_agg:urllnkdin_count) :: numeric AS urllnkdin_ts,max(feature_agg:email_avg_size) :: numeric AS email_avg_size_val,
max(feature_agg:email_count) :: numeric AS email_count_val,max(feature_agg:email_size) :: numeric AS emails_size_val ,
max(feature_agg:ext_receiver_count) :: numeric AS ext_receiver_val,max(feature_agg:if_dwnld_count) :: numeric AS if_dwnld_val,
max(feature_agg:if_upld_count) :: numeric AS if_upld_val,max(feature_agg:failedlogin_count) :: numeric AS failedlogin_val,
max(feature_agg:fileactivity_count) :: numeric AS fileactivity_val,max(feature_agg:logins_count) :: numeric AS logins_val ,
max(feature_agg:pri_count) :: numeric AS pri_val,max(feature_agg:ps_count) :: numeric AS ps_val,
max(feature_agg:urllnkdin_count) :: numeric AS urllnkdin_val,'null' as src_geo_city,'null' as src_geo_lat,'null' as src_geo_long,'NA' as DISPLAYNAME,
'NA' as DESCRIPTION,'NA' as EMAILADDRESS, null as row_n,'NA' as qry,'NA'  as raw,   
'NA' ::string AS os_userlogons_zscore,
'NA' ::string AS os_userlogoffs_zscore,
null AS os_userlogons_val,
null AS os_userlogoffs_val 
,'sstech.com' as SRC_DOMAIN
from table(FN_NP_ML_OVERALL_AGG('analyst1'))  
where EVENT_DATE >= DATEADD(DAY, -30, CURRENT_TIMESTAMP) group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14;		





CREATE OR REPLACE FUNCTION "FN_ML_OVERALL_AGG"("P_ANALYST_NM" VARCHAR(16777216))
RETURNS TABLE ("ANALYST_NAME" VARCHAR(16777216), "EVENT_DATE" DATE, "TYPE" VARCHAR(16777216), "NAME" VARCHAR(16777216), "FEATURE_AGG" VARIANT, "ZSCORE" VARIANT, "TS_AGG" VARIANT, "TOTAL_ALERT_COUNT" FLOAT, "MAX_ANOMALY_SCORE" FLOAT, "MAX_ALERT_SCORE" FLOAT, "RISK_SCORE" FLOAT)
LANGUAGE SQL
AS '
SELECT 
   analyst_name,
   event_date, 
   type,
   name,
   feature_agg,
   adj_zscore as zscore,
   ts_agg,
   sum(alert_count) total_alert_count, 
   round(max(adj_anomaly_score),4) max_anomaly_score, 
   round(max(alert_score),4) max_alert_score,
   round((max(alert_score) * 0.3) + (max_anomaly_score * 0.7),4) risk_score
FROM 
(
  SELECT
    P_ANALYST_NM as analyst_name,
    event_date,
    type,
    lower(split(trim(user_name),''@'')[0]) as name,
    feature_val as feature_agg,
    adj_zscore,
    ts_agg,
    alert_agg:alert_count alert_count,
    max(alert_agg:alert_count) OVER(PARTITION BY event_date,type) max_alert_count,
    case when NVL(max_alert_count,0) !=0 THEN (alert_agg:alert_count/max_alert_count)*100 ELSE 0 END alert_score,
    adj_anomaly_score
  FROM table(FN_ML_ANALYST_FB(''windowsnxlog'',P_ANALYST_NM,''both''))
UNION ALL
  SELECT
    P_ANALYST_NM as analyst_name,
    event_date,
    type,
    lower(split(trim(user_name),''@'')[0]) as name,
    feature_val as feature_agg,
    adj_zscore,
    ts_agg,
    alert_agg:alert_count alert_count,
    max(alert_agg:alert_count) OVER(PARTITION BY event_date,type) max_alert_count,
    case when NVL(max_alert_count,0) !=0 THEN (alert_agg:alert_count/max_alert_count)*100 ELSE 0 END alert_score,
    adj_anomaly_score
  FROM table(FN_ML_ANALYST_FB(''wgtraffic'',P_ANALYST_NM,''both''))
UNION ALL
  SELECT
    P_ANALYST_NM as analyst_name,
    event_date,
    type,
    lower(split(trim(user_name),''@'')[0]) as name,
    feature_val as feature_agg,
    adj_zscore,
    ts_agg,
    alert_agg:alert_count alert_count,
    max(alert_agg:alert_count) OVER(PARTITION BY event_date,type) max_alert_count,
    case when NVL(max_alert_count,0) !=0 THEN (alert_agg:alert_count/max_alert_count)*100 ELSE 0 END alert_score,
    adj_anomaly_score
  FROM table(FN_ML_ANALYST_FB(''msexchange'',P_ANALYST_NM,''both''))
)
GROUP BY 1,2,3,4,5,6,7
';


