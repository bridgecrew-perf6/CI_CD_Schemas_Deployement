use schema dev_demo_schema;

create or replace task TASK_NP_SESSION_TRACKING_INFO_TEST
	warehouse=alert_demo_wh
	schedule='USING CRON 0 1 * * * UTC'
	as call SP_NP_SESSION_TRACKING_INSERT_TEST();
	



CREATE OR REPLACE PROCEDURE "SP_NP_SESSION_TRACKING_INSERT_TEST"()
RETURNS VARCHAR(100)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
var delqry = `DELETE FROM NP_SESSION_TRACKING_INFO_TEST WHERE EVENT_DATE >= (select coalesce(max(event_date),''2020-08-01'' :: date) from NP_SESSION_TRACKING_INFO_TEST)`
var query  = `
INSERT INTO NP_SESSION_TRACKING_INFO_TEST(EVENT_DATE,SESSION_ID,USER_NAME,HOST_NAME,SESSION_START_TIME,SESSION_END_TIME,EVENT_LIST,EVENT_COUNT, LOCK_UNLOCK_EVENTS, LOGON_START_TIME, PARENT_LOGON_TYPE, LINKED_LOGINID, ELEVATED_TOKEN, ASSIGNED_IPS)
 with BASEPOP as
 (
 select
 event_time,
 lower(RPT_HOST) as HOST_NAME,
 event_id,
 LOGON_TYPE,
 src_session_id as SRC_LOGONID,
 DST_SESSION_ID as TGT_LOGONID,
 LINKED_SESSION_ID,
 src_user_name,
 dst_user_name,
 case when coalesce(SRC_LOGONID,''0x0'') not in (''0x3e7'',''0x0'',''0x3e4'',''0x3e5'') then 1 else 0 end as SRC_LOGONID_EVENT,
 case when coalesce(TGT_LOGONID,''0x0'') not in (''0x3e7'',''0x0'',''0x3e4'',''0x3e5'') then 1 else 0 end as TGT_LOGONID_EVENT,
 case when event_id = 4624 then 1 else 0 end as LOGIN_EVENT,
 case when event_id IN (4800,4647,4801) then 1 else 0 end as LOCK_UNLOCK_EVENT,
 ADMIN_FLAG AS ELEVATED_TOKEN,
 case when LOCK_UNLOCK_EVENT = 1 then 
      case WHEN EVENT_ID = 4800 THEN ''EXIT - LAPTOP_LOCKED''
           WHEN EVENT_ID = 4647 THEN ''EXIT - LAPTOP SIGNOUT/RESTART/SHUTDOWN'' 
           WHEN EVENT_ID = 4801 THEN ''ENTRY - LAPTOP_UNLOCKED'' 
      end 
 end AS LOCK_UNLOCK_TYPE,
 case when SRC_LOGONID = TGT_LOGONID THEN SRC_LOGONID
      when coalesce(SRC_LOGONID,''0x0'') not in (''0x3e7'',''0x0'',''0x3e4'',''0x3e5'') THEN SRC_LOGONID
      when coalesce(TGT_LOGONID,''0x0'') not in (''0x3e7'',''0x0'',''0x3e4'',''0x3e5'') then TGT_LOGONID
 end as MASTER_SESSION_ID,
 case when coalesce(SRC_LOGONID,''0x0'') not in (''0x3e7'',''0x0'',''0x3e4'',''0x3e5'') then lower(src_user_name)
      when coalesce(TGT_LOGONID,''0x0'') not in (''0x3e7'',''0x0'',''0x3e4'',''0x3e5'') then lower(dst_user_name)
 end as user_name
 from MS_WIN_SECURITYAUDITING_ODM
 WHERE EVENT_TIME :: DATE > (select coalesce(max(event_date),''2020-08-01'' :: date) from NP_SESSION_TRACKING_INFO_TEST)
 AND (coalesce(TGT_LOGONID,''0x0'') not in (''0x3e7'',''0x0'',''0x3e4'',''0x3e5'') OR coalesce(SRC_LOGONID,''0x0'') not in (''0x3e7'',''0x0'',''0x3e4'',''0x3e5''))
 ),
 procpop as
 (
  select
  EVENT_TIME :: DATE AS EVENT_DATE,
  MASTER_SESSION_ID AS SESSION_ID,
  HOST_NAME,
  USER_NAME,
  min(event_time) as SESSION_START_TIME, 
  max(event_time) as SESSION_END_TIME,
  array_agg(distinct event_id) as EVENT_LIST,
  count(*) as EVENT_COUNT,
  object_agg(CASE WHEN LOCK_UNLOCK_EVENT = 1 THEN EVENT_TIME END,CASE WHEN LOCK_UNLOCK_EVENT = 1 THEN LOCK_UNLOCK_TYPE::variant END) AS LOCK_UNLOCK_EVENTS
  from basepop
  group by 1,2,3,4
 ),
 orig_event as
 (
  select 
   EVENT_TIME, 
   LOGON_TYPE, 
   ELEVATED_TOKEN, 
   MASTER_SESSION_ID, 
   LINKED_SESSION_ID 
   from BASEPOP 
   where LOGIN_EVENT = 1
 ),
 lock_unlock_event as
 (
  select 
  MASTER_SESSION_ID,
  object_agg(
  EVENT_TIME,
  CASE WHEN EVENT_ID = 4800 THEN EVENT_ID || '': EXIT - LAPTOP_LOCKED''
       WHEN EVENT_ID = 4647 THEN EVENT_ID || '': EXIT - LAPTOP SIGNOUT/RESTART/SHUTDOWN''
       WHEN EVENT_ID = 4801 THEN EVENT_ID || '': ENTRY - LAPTOP_UNLOCKED''
  END :: variant) AS lock_unlock_events
   FROM
   (
     select 
     distinct
     EVENT_TIME, 
     MASTER_SESSION_ID,
     EVENT_ID
     from BASEPOP 
     where LOCK_UNLOCK_EVENT = 1
   )
  group by 1
 ),
 ip_list as
 (
  SELECT
  distinct
  EVENT_TIME,
  lower(RPT_HOST) as HOST_NAME,
  raw:"SourceAddress" :: varchar AS IP_ADDRESS
  from MS_WIN_SECURITYAUDITING_NETTRAFFIC_ODM
  WHERE EVENT_TIME :: DATE > (select coalesce(max(event_date),''2020-08-01'' :: date) from NP_SESSION_TRACKING_INFO_TEST)
  and event_id in (5156)
  and DIRECTION = ''Outbound''
 )
  select 
  pp.event_date, pp.session_id, pp.user_name, pp.host_name, pp.session_start_time, pp.session_end_time, pp.event_list, pp.event_count, lue.lock_unlock_events,
  oe.event_time as logon_start_time, oe.logon_type as parent_logon_type, oe.linked_session_id as linked_logonid, oe.elevated_token,
  ARRAY_AGG(distinct IP.IP_ADDRESS) AS ASSIGNED_IPS
  from procpop pp
  left join orig_event oe
  on pp.SESSION_ID = oe.master_session_id
  left join lock_unlock_event lue
  on pp.SESSION_ID = lue.master_session_id
  left join ip_list ip
  on ip.host_name = pp.host_name and ip.event_time between pp.session_start_time and pp.session_end_time
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13
  `
snowflake.execute({sqlText: "BEGIN"});
snowflake.execute({sqlText: delqry});
snowflake.execute({sqlText: query});
snowflake.execute({sqlText: "COMMIT"});
return "Succeeded."; // Return a success/error indicator.
';


create or replace TABLE NP_SESSION_TRACKING_INFO (
	EVENT_DATE DATE,
	SRC_USERNAME VARCHAR(16777216),
	HOSTNAME VARCHAR(16777216),
	SESSION_START_TIME TIMESTAMP_NTZ(9),
	SESSION_END_TIME TIMESTAMP_NTZ(9),
	SRC_LOGONID VARCHAR(16777216),
	ASSIGNED_IPS ARRAY,
	EVENTID_WITH_SAME_SRC_LOGONID ARRAY,
	SRC_LOGONID_EVT_COUNT NUMBER(38,0),
	EVENTID_WITH_SAME_TGT_LOGONID ARRAY,
	TGT_LOGONID_EVT_COUNT NUMBER(38,0),
	LOGON_START_TIME TIMESTAMP_NTZ(9),
	LINKED_LOGINID VARCHAR(16777216),
	ELEVATED_TOKEN VARCHAR(16777216),
	PARENT_LOGON_TYPE VARCHAR(16777216),
	SESSION_LOG VARIANT
);


create or replace TABLE NP_SESSION_TRACKING_INFO_TEST (
	EVENT_DATE DATE,
	SESSION_ID VARCHAR(16777216),
	USER_NAME VARCHAR(16777216),
	HOST_NAME VARCHAR(16777216),
	SESSION_START_TIME TIMESTAMP_NTZ(9),
	SESSION_END_TIME TIMESTAMP_NTZ(9),
	EVENT_LIST ARRAY,
	EVENT_COUNT NUMBER(38,0),
	LOCK_UNLOCK_EVENTS VARIANT,
	LOGON_START_TIME TIMESTAMP_NTZ(9),
	PARENT_LOGON_TYPE VARCHAR(16777216),
	LINKED_LOGINID VARCHAR(16777216),
	ELEVATED_TOKEN VARCHAR(16777216),
	ASSIGNED_IPS ARRAY
);


create or replace TABLE SESSION_TRACKING_INFO (
	EVENT_DATE DATE,
	SRC_USERNAME VARCHAR(16777216),
	HOSTNAME VARCHAR(16777216),
	SESSION_START_TIME TIMESTAMP_NTZ(9),
	SESSION_END_TIME TIMESTAMP_NTZ(9),
	SRC_LOGONID VARCHAR(16777216),
	ASSIGNED_IPS ARRAY,
	EVENTID_WITH_SAME_SRC_LOGONID ARRAY,
	SRC_LOGONID_EVT_COUNT NUMBER(38,0),
	EVENTID_WITH_SAME_TGT_LOGONID ARRAY,
	TGT_LOGONID_EVT_COUNT NUMBER(38,0),
	LOGON_START_TIME TIMESTAMP_NTZ(9),
	LINKED_LOGINID VARCHAR(16777216),
	ELEVATED_TOKEN VARCHAR(16777216),
	PARENT_LOGON_TYPE VARCHAR(16777216),
	SESSION_LOG VARIANT
);