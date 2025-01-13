with pageviews as (
-- Common Table Expression (CTE) with the example data 
select 1 as event_id, 99999 as user_id, timestamp('2025-01-01 09:00:00') as event_time, 'lovevery.com' as page_url
union all
select 2, 99999, timestamp('2025-01-01 09:03:00'), 'lovevery.com/products/the-play-kits'
union all
select 3, 99999, timestamp('2025-01-01 09:02:00'), 'lovevery.com/products/the-play-kits#explore'
union all
select 4, 99999, timestamp('2025-01-01 09:15:00'), 'lovevery.com/products/the-play-kits-the-enthusiast'
union all
select 5, 99999, timestamp('2025-01-01 09:48:00'), 'lovevery.com/pages/subscription'
union all
select 6, 99999, timestamp('2025-01-01 11:00:00'), 'lovevery.com/products/the-play-gym'
)

, stg_events as (
-- Get the timestamps of the previous event for the user using the lag window function
select 
  event_id,
  user_id, 
  event_time,
  event_time as page_start_ts, 
  page_url,
  lag(event_time, 1) over (partition by user_id order by event_time asc, event_id asc) as prev_timestamp
from pageviews
)

, stg_sessions as (
--Filter events using 30 min. threshold to get a session. Get the next session start timestamp.
select 
  event_id,
  user_id, 
  event_time,
  prev_timestamp,
  page_start_ts, 
  lead(event_time) over (partition by user_id order by event_time asc, event_id asc) as next_session_start
from stg_events 
where   
  1=1
  and (timestamp_diff(page_start_ts, prev_timestamp, second) > 1800  or prev_timestamp is null)
)

-- Add a session key for each event by joining back with events. 
select
  s.event_id as session_key,
  e.event_id,
  e.user_id,
  e.event_time,
  e.page_url
from stg_events e
left join stg_sessions s 
  on e.user_id = s.user_id
  and e.event_time >= s.page_start_ts
  and (e.event_time < s.next_session_start or s.next_session_start is null)
