use schema dev_demo_schema;


create or replace TABLE NP_UEBA_BANDWIDTH_USAGE_BY_COUNT (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);


create or replace TABLE NP_UEBA_BANDWIDTH_USAGE_BY_SIZE (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);


create or replace TABLE NP_UEBA_BOT_ATTACK (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);


create or replace TABLE NP_UEBA_END_POINT_INDICATORS (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);



create or replace TABLE NP_UEBA_FILE_DOWNLOADS (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);



create or replace TABLE NP_UEBA_HIGH_VOLUME_FILE_ACCESS (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);


create or replace TABLE NP_UEBA_PRIVILEGED_ACCOUNT_USAGE (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);



create or replace TABLE NP_UEBA_UNUSUAL_DOMAIN (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);


create or replace TABLE NP_UEBA_VPN_SESSIONS (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);


create or replace TABLE OUTLIERS_UEBA (
	EVENT_DATE VARCHAR(16777216),
	ID VARCHAR(16777216),
	X FLOAT,
	Y FLOAT,
	SCORE FLOAT,
	REPORT_NAME VARCHAR(16777216),
	RESULT VARCHAR(16777216),
	IS_THREAT VARCHAR(16777216),
	PEERS VARCHAR(16777216),
	TYPE VARCHAR(16777216)
);



create or replace TABLE WGTRAFFIC_TESTING_UEBA_BANDWIDTH_SCORE (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARCHAR(16777216),
	ALERT_AGG VARCHAR(16777216),
	ZSCORE VARCHAR(16777216),
	PAS_KMEANS FLOAT NOT NULL,
	PAS_ISOLATION FLOAT NOT NULL,
	PAS_SVM FLOAT NOT NULL,
	PAS FLOAT NOT NULL,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);

create or replace TABLE WG_FW_NETWORKTRAFFIC (
	GUID VARCHAR(16777216),
	PARSING_TIME TIMESTAMP_NTZ(9),
	SYSLOG_EVENT_DATETIME TIMESTAMP_NTZ(9),
	ORIGINAL_STRING VARIANT,
	ENRICHMENT_JSON VARIANT,
	ANTIVIRUS_ERROR VARCHAR(16777216),
	ALERT_JSON VARIANT,
	APP_CAT_NAME VARCHAR(16777216),
	APP_NAME VARCHAR(16777216),
	BOTNET VARCHAR(16777216),
	ELAPSED_TIME VARCHAR(16777216),
	POLICY VARCHAR(16777216),
	ARG VARCHAR(16777216),
	SYSLOG_HOST VARCHAR(16777216),
	SRC_USER VARCHAR(16777216),
	IP_SRC_ADDR VARCHAR(16777216),
	IP_SRC_ADDR_NAT VARCHAR(16777216),
	IP_SRC_PORT NUMBER(38,0),
	IP_SRC_PORT_NAT NUMBER(38,0),
	IN_BYTES NUMBER(38,0),
	OUT_BYTES NUMBER(38,0),
	IP_DST_ADDR VARCHAR(16777216),
	IP_DST_ADDR_NAT VARCHAR(16777216),
	IP_DST_PORT NUMBER(38,0),
	IP_DST_PORT_NAT NUMBER(38,0),
	DSTNAME VARCHAR(16777216),
	DST_USER VARCHAR(16777216),
	MSG VARCHAR(16777216),
	MESSAGE_ID VARCHAR(16777216),
	MESSAGE_DESC VARCHAR(16777216),
	MSG_HOST VARCHAR(16777216),
	DST_INTF VARCHAR(16777216),
	FILENAME VARCHAR(16777216),
	ORIGINAL_SOURCE VARCHAR(16777216),
	PATH VARCHAR(16777216),
	PROTO VARCHAR(16777216),
	PROXY_ACTION VARCHAR(16777216),
	VIRUS VARCHAR(16777216),
	WEB_CAT VARCHAR(16777216),
	DURATION VARCHAR(16777216),
	METHOD VARCHAR(16777216),
	DISPOSITION VARCHAR(16777216),
	PROCESSING_DTTM TIMESTAMP_NTZ(9),
	DATA_FILE_NAME VARCHAR(16777216)
);

create or replace TABLE UEBA_BANDWIDTH_USAGE_BY_COUNT (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);

create or replace TABLE UEBA_BANDWIDTH_USAGE_BY_SIZE (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);


create or replace TABLE UEBA_BOT_ATTACK (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);


create or replace TABLE UEBA_END_POINT_INDICATORS (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);


create or replace TABLE UEBA_FILE_DOWNLOADS (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);



create or replace TABLE UEBA_HIGH_VOLUME_FILE_ACCESS (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);


create or replace TABLE UEBA_PRIVILEGED_ACCOUNT_USAGE (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);


create or replace TABLE UEBA_UNUSUAL_DOMAIN (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);


create or replace TABLE UEBA_VPN_SESSIONS (
	EVENT_START_DATE DATE,
	EVENT_HOUR NUMBER(38,18),
	EVENT_END_DATE DATE,
	TIME_FRAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	FEATURE_AGG VARIANT,
	ZSCORE VARIANT,
	PAS FLOAT,
	PAS_ALGORITHM VARIANT,
	EXECUTION_TIMESTAMP NUMBER(38,0) NOT NULL
);




create or replace view OUTLIERS_UEBA_GREATER_THAN_80(
	EVENT_DATE,
	ID,
	X,
	Y,
	SCORE,
	REPORT_NAME,
	RESULT,
	IS_THREAT,
	PEERS,
	TYPE
) as
select * from OUTLIERS_UEBA where score >= 80.0 ;


create or replace view UEBA_DETAILS_VIEW(
	EVENT_TIME,
	IN_BYTES,
	OUT_BYTES,
	SRC_USER_NAME,
	SRC_IP,
	DST_IP,
	SRC_PORT,
	DST_PORT
) as 
  select event_time,IN_BYTES,out_bytes,src_user_name,src_ip,dst_ip,src_port,dst_port from wg_fw_networktraffic_odm
;


create or replace view UEBA_OUTLIERS_UEBA(
	EVENT_DATE,
	ID,
	X,
	Y,
	SCORE,
	REPORT_NAME,
	TYPE
) as
select event_start_date::date as event_date,name as id,coalesce(feature_agg:upld_bytes,0) ::bigint x,
coalesce(feature_agg:dwnld_bytes,0) ::bigint y,
pas as score,'Bandwidth-Usage-by-Size' as Report_name,type from UEBA_BANDWIDTH_USAGE_BY_SIZE where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:if_upld_count,0) ::bigint x,coalesce(feature_agg:if_dwnld_count,0) ::bigint y,
pas as score,'Bandwidth-Usage-by-Count' as Report_name,type from UEBA_BANDWIDTH_USAGE_BY_COUNT  where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:dst_count,0) ::bigint x,coalesce(feature_agg:download_count,0) ::bigint y,
pas as score,'File-Downloads' as Report_name,type from UEBA_FILE_DOWNLOADS  where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:dst_port_count,0) ::bigint x,coalesce(feature_agg:src_count,0) ::bigint y,
pas as score,'Bot-Attack' as Report_name,type from UEBA_BOT_ATTACK  where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:bandwidth,0) ::bigint x,coalesce(feature_agg:privelege_total,0) ::bigint y,
pas as score,'Privileged-Account-Usage' as Report_name,type from UEBA_PRIVILEGED_ACCOUNT_USAGE  where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:end_point_remote_count,0) ::bigint x,coalesce(feature_agg:sent_bytes,0) ::bigint y,
pas as score,'Endpoint-Indicators-of-Compromise' as Report_name,type from UEBA_END_POINT_INDICATORS  where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:bandwidth,0) ::bigint x,coalesce(feature_agg:high_volume_count,0) ::bigint y,
pas as score,'High-Volume-File-Access' as Report_name,type from UEBA_HIGH_VOLUME_FILE_ACCESS  where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:upld_bytes,0) ::bigint x,
coalesce(feature_agg:distinct_domain_count,0) ::bigint y,
pas as score,'Unusual-Domain' as Report_name,type from UEBA_UNUSUAL_DOMAIN  where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:deviating_distance,0) ::bigint x,
coalesce(feature_agg:no_of_sessions,0) ::bigint y,
pas as score,'VPN-Conenctions' as Report_name,type from UEBA_VPN_SESSIONS  where score >=70.0 
;


create or replace view VW_NP_OUTLIERS_UEBA(
	EVENT_DATE,
	ID,
	X,
	Y,
	SCORE,
	REPORT_NAME,
	TYPE
) as
select event_start_date::date as event_date,name as id,coalesce(feature_agg:upld_bytes,0) ::bigint x,
coalesce(feature_agg:dwnld_bytes,0) ::bigint y,
pas as score,'Bandwidth-Usage-by-Size' as Report_name,type from NP_UEBA_BANDWIDTH_USAGE_BY_SIZE where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:if_upld_count,0) ::bigint x,coalesce(feature_agg:if_dwnld_count,0) ::bigint y,
pas as score,'Bandwidth-Usage-by-Count' as Report_name,type from NP_UEBA_BANDWIDTH_USAGE_BY_COUNT  where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:dst_count,0) ::bigint x,coalesce(feature_agg:download_count,0) ::bigint y,
pas as score,'File-Downloads' as Report_name,type from NP_UEBA_FILE_DOWNLOADS  where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:dst_port_count,0) ::bigint x,coalesce(feature_agg:src_count,0) ::bigint y,
pas as score,'Bot-Attack' as Report_name,type from NP_UEBA_BOT_ATTACK  where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:end_point_remote_count,0) ::bigint x,coalesce(feature_agg:sent_bytes,0) ::bigint y,
pas as score,'Endpoint-Indicators-of-Compromise' as Report_name,type from NP_UEBA_END_POINT_INDICATORS  where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:upld_bytes,0) ::bigint x,
coalesce(feature_agg:distinct_domain_count,0) ::bigint y,
pas as score,'Unusual-Domain' as Report_name,type from NP_UEBA_UNUSUAL_DOMAIN  where score >=70.0 
 UNION ALL 
select event_start_date::date as event_date,name as id,coalesce(feature_agg:deviating_distance,0) ::bigint x,
coalesce(feature_agg:no_of_sessions,0) ::bigint y,
pas as score,'VPN-Conenctions' as Report_name,type from NP_UEBA_VPN_SESSIONS  where score >=70.0
 UNION ALL 
select event_start_date::date as event_date,name as id,coalesce(feature_agg:bandwidth,0) ::bigint x,coalesce(feature_agg:high_volume_count,0) ::bigint y,
pas as score,'High-Volume-File-Access' as Report_name,type from NP_UEBA_HIGH_VOLUME_FILE_ACCESS  where score >=70.0 
UNION ALL
select event_start_date::date as event_date,name as id,coalesce(feature_agg:bandwidth,0) ::bigint x,coalesce(feature_agg:privelege_total,0) ::bigint y,
pas as score,'Privileged-Account-Usage' as Report_name,type from NP_UEBA_PRIVILEGED_ACCOUNT_USAGE  where score >=70.0;


