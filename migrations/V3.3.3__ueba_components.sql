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

create or replace view WG_FW_NETWORKTRAFFIC_ODM(
	DVC_PRODUCT,
	DVC_VENDOR,
	DVC_VERSION,
	SRC_TYPE,
	APP_NAME,
	AV_ERROR,
	BOTNET_NAME,
	CAT_APP_NAME,
	CAT_WEB_NAME,
	COMMIT_TIME,
	DETECTED_HOST,
	DISPOSITION,
	DST_DOMAIN,
	DST_GEO_CITY,
	DST_GEO_COUNTRY,
	DST_RDNS_DOMAIN,
	DST_RDNS_NAT_DOMAIN,
	DST_GEO_LAT,
	DST_GEO_LONG,
	DST_IF,
	DST_IP,
	DST_NAT_IP,
	DST_PORT,
	DST_NAT_PORT,
	DST_USER_NAME,
	EVENT_ID,
	EVENT_TIME,
	FILE_NAME,
	FLAGS,
	GUID,
	IN_BYTES,
	MALWARE_NAME,
	MESSAGE_DESC,
	MSG,
	ORIG_IP,
	OUT_BYTES,
	PARSING_TIME,
	PATH,
	PROTO,
	QRY,
	RISK_DESC,
	RPT_HOST,
	SESSION_DURATION,
	SRC_GEO_CITY,
	SRC_GEO_COUNTRY,
	SRC_RDNS_DOMAIN,
	SRC_RDNS_NAT_DOMAIN,
	SRC_GEO_LAT,
	SRC_GEO_LONG,
	SRC_IP,
	SRC_NAT_IP,
	SRC_PORT,
	SRC_NAT_PORT,
	SRC_USER_NAME,
	RAW
) as
SELECT
    'Firebox'                                        	 AS DVC_PRODUCT,
    'WatchGuard'                                     	 AS DVC_VENDOR,
    '*'                                              	 AS DVC_VERSION,
    'Network Traffic'                                	 AS SRC_TYPE,
    APP_NAME                                         	 AS APP_NAME,
    ANTIVIRUS_ERROR                                  	 AS AV_ERROR,
    BOTNET                                           	 AS BOTNET_NAME,
    APP_CAT_NAME                                     	 AS CAT_APP_NAME,
    WEB_CAT                                          	 AS CAT_WEB_NAME,
    PROCESSING_DTTM                                  	 AS COMMIT_TIME,
    MSG_HOST                                         	 AS DETECTED_HOST,
    CASE
        WHEN MESSAGE_ID = '1AFF0025' THEN PROXY_ACTION
        ELSE DISPOSITION
    END                                              	 AS DISPOSITION,
    DSTNAME                                          	 AS DST_DOMAIN,
    TO_VARCHAR(ENRICHMENT_JSON['DST_GEO_CITY'])      	 AS DST_GEO_CITY,
    TO_VARCHAR(ENRICHMENT_JSON['DST_GEO_COUNTRY'])   	 AS DST_GEO_COUNTRY,
    TO_VARCHAR(ENRICHMENT_JSON['DST_RDNS_DOMAIN'])   	 AS DST_RDNS_DOMAIN,
    TO_VARCHAR(ENRICHMENT_JSON['DST_RDNS_NAT_DOMAIN'])   AS DST_RDNS_NAT_DOMAIN,
    TO_NUMBER(ENRICHMENT_JSON['DST_GEO_LAT'], 6, 3)  	 AS DST_GEO_LAT,
    TO_NUMBER(ENRICHMENT_JSON['DST_GEO_LONG'], 6, 3) 	 AS DST_GEO_LONG,
    DST_INTF                                         	 AS DST_IF,
    IP_DST_ADDR                                      	 AS DST_IP,
    IP_DST_ADDR_NAT                                  	 AS DST_NAT_IP,
    IP_DST_PORT                                      	 AS DST_PORT,
    IP_DST_PORT_NAT                                  	 AS DST_NAT_PORT,
    DST_USER                                         	 AS DST_USER_NAME,
    MESSAGE_ID                                       	 AS EVENT_ID,
    SYSLOG_EVENT_DATETIME                            	 AS EVENT_TIME,
    FILENAME                                         	 AS FILE_NAME,
    POLICY                                           	 AS FLAGS,
    GUID                                             	 AS GUID,
    IN_BYTES                                         	 AS IN_BYTES,
    VIRUS                                            	 AS MALWARE_NAME,
    MESSAGE_DESC                                     	 AS MESSAGE_DESC,
    MSG                                              	 AS MSG,
    ORIGINAL_SOURCE                                  	 AS ORIG_IP,
    OUT_BYTES                                        	 AS OUT_BYTES,
    PARSING_TIME                                     	 AS PARSING_TIME,
    PATH                                             	 AS PATH,
    PROTO                                            	 AS PROTO,
    ARG                                              	 AS QRY,
    ALERT_JSON                                       	 AS RISK_DESC,
    lower(SYSLOG_HOST)                               	 AS RPT_HOST,
    CASE	 
        WHEN MESSAGE_ID = '30000151' THEN DURATION	 
        ELSE ELAPSED_TIME	 
    END                                              	 AS SESSION_DURATION,
    TO_VARCHAR(ENRICHMENT_JSON['SRC_GEO_CITY'])      	 AS SRC_GEO_CITY,
    TO_VARCHAR(ENRICHMENT_JSON['SRC_GEO_COUNTRY'])   	 AS SRC_GEO_COUNTRY,
    TO_VARCHAR(ENRICHMENT_JSON['SRC_RDNS_DOMAIN'])   	 AS SRC_RDNS_DOMAIN,
    TO_VARCHAR(ENRICHMENT_JSON['SRC_RDNS_NAT_DOMAIN']	 )   AS SRC_RDNS_NAT_DOMAIN,
    TO_VARCHAR(ENRICHMENT_JSON['SRC_GEO_LAT'])       	 AS SRC_GEO_LAT,
    TO_NUMBER(ENRICHMENT_JSON['SRC_GEO_LONG'], 6, 3) 	 AS SRC_GEO_LONG,
    IP_SRC_ADDR                                      	 AS SRC_IP,
    IP_SRC_ADDR_NAT                                  	 AS SRC_NAT_IP,
    IP_SRC_PORT                                      	 AS SRC_PORT,
    IP_SRC_PORT_NAT                                  	 AS SRC_NAT_PORT,
    lower(split(SRC_USER,'@')[0])                    	 AS SRC_USER_NAME,
    ORIGINAL_STRING                                  	 AS RAW
	 
-- METHOD AS HTTP_METHOD
-- ORIGINAL_STRING AS RAW
FROM
    WG_FW_NETWORKTRAFFIC;



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


