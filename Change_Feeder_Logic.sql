with Raw_Clicks as (SELECT
post_evar56 as Adobe_Tracking_ID, 
DATE(timestamp(post_cust_hit_time_gmt), "America/New_York") AS Adobe_Date,
DATETIME(timestamp(post_cust_hit_time_gmt), "America/New_York") AS Adobe_Timestamp,
post_evar19 as Player_Event,
post_evar7 as Binge_Details
FROM `nbcu-ds-prod-001.feed.adobe_clickstream` 
WHERE post_evar56 is not null
and post_cust_hit_time_gmt is not null 
and post_evar7 is not null
and post_evar7 not like "%display"
and DATE(timestamp(post_cust_hit_time_gmt), "America/New_York") between "2022-10-18" and "2022-10-21"),

cte as (select 
Adobe_Tracking_ID,
Adobe_Date,
Adobe_Timestamp,
Player_Event,
Binge_Details,
Video_Start_Type,
device_name,
Feeder_Video,
Feeder_Video_Id,
Display_Name,
video_id,
num_seconds_played_no_ads
from
(SELECT 
Adobe_Tracking_ID,
Adobe_Date,
Adobe_Timestamp,
Player_Event,
Binge_Details,
case when Binge_Details like "%auto-play" then "Auto-Play" 
     when Binge_Details like '%cue%up%click' then "Clicked-Up-Next" 
     when Binge_Details like "%dismiss" then "Dismiss" 
     when Player_Event like "%details:%" and Binge_Details is not null then "Manual-Selection"
     when Binge_Details like '%deeplink%' then "Manual-Selection"
     when Binge_Details like 'rail%click'then "Manual-Selection" 
else null end as Video_Start_Type,
"" device_name,
"" Feeder_Video,
"" Feeder_Video_Id,
case when Player_Event like "%:episodes:%" and Binge_Details is not null then REGEXP_REPLACE(Player_Event, r'peacock:details:episodes:', '')
     when Player_Event like "%:upsell:%" and Binge_Details is not null then REGEXP_REPLACE(Player_Event, r'peacock:details:upsell:', '')
     when Player_Event like "%:more-like-this:%" and Binge_Details is not null then REGEXP_REPLACE(Player_Event, r'peacock:details:more-like-this:', '')
     when Player_Event like "%:extras:%" and Binge_Details is not null then REGEXP_REPLACE(Player_Event, r'peacock:details:extras:', '')
     when Player_Event like "%:details:%" and Binge_Details is not null then REGEXP_REPLACE(Player_Event, r'peacock:details:', '')
     when Binge_Details like "%auto-play" then  REGEXP_EXTRACT(Binge_Details, r"[|][|](.*)[|]")
     when Binge_Details like '%cue%up%click' then  REGEXP_EXTRACT(Binge_Details, r"[|][|](.*)[|]")
     when Binge_Details like 'rail%click'then REGEXP_EXTRACT(Binge_Details, r"[|]([a-zA-Z0-9\s-.:]+)[|]click")
else null end as Display_Name,
"" video_id,
null num_seconds_played_no_ads
FROM Raw_Clicks)
where lower(Display_Name) =  "a friend of the family" and Video_Start_Type is not null),

click_Ready as (select 
Adobe_Tracking_ID,
Adobe_Date,
Adobe_Timestamp,
Player_Event,
Binge_Details,
Video_Start_Type,
device_name,
Lag(Display_Name) over (partition by adobe_tracking_id,adobe_date order by adobe_timestamp) as Feeder_Video,
Feeder_Video_Id,
Display_Name,
video_id,
num_seconds_played_no_ads
from cte),

cte1 as (
select 
Adobe_Tracking_ID,
Adobe_Date,
Adobe_Timestamp,
Player_Event,
Binge_Details,
Video_Start_Type,
device_name,
Feeder_Video,
Feeder_Video_Id,
Display_Name,
video_id,
num_seconds_played_no_ads
from(
SELECT
adobe_tracking_id as Adobe_Tracking_ID,
adobe_date as Adobe_Date,
TIMESTAMP_ADD(adobe_timestamp , INTERVAL -40 second) as Adobe_Timestamp,
"" Player_Event,
"" Binge_Details,
case when Display_Name like '%trailer%' then 'Manual-Selection' else'Vdo_End' end as Video_Start_Type,
device_name,
Lag(display_name) over (partition by adobe_tracking_id,adobe_date order by adobe_timestamp) as Feeder_Video,
Lag(video_id) over (partition by adobe_tracking_id,adobe_date order by adobe_timestamp) as Feeder_Video_Id,
display_name as Display_Name,
video_id,
num_seconds_played_no_ads
FROM 
`nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO`
where adobe_tracking_ID is not null 
and adobe_date  between "2022-10-18" and "2022-10-21"
and media_load = False and num_seconds_played_with_ads > 0)
where lower(Display_Name) =  "a friend of the family" and Video_Start_Type is not null
),

middle_table as (select *
from click_Ready
union all
select *
from cte1),

cte2 as (select b.*,
sum(case when b.Feeder_Video = b.Display_Name then b.num_seconds_played_no_ads else 0 end) over (partition by Adobe_Tracking_ID, Adobe_Date, grp) as Episode_Time
from
(SELECT a.*,
lag(Video_Start_Type) over (partition by Adobe_Tracking_ID,adobe_date order by adobe_timestamp) as Last_Actions,
sum(case when Feeder_Video = Display_Name then 0 else 1 end) over (partition by Adobe_Tracking_ID, Adobe_Date order by Adobe_Timestamp) as grp
FROM 
middle_table a) b),

cte3 as 
(select 
Adobe_Tracking_ID,
Adobe_Date,
Adobe_Timestamp,
Last_Actions,
case when Feeder_Video is null and num_seconds_played_no_ads is not null then "Manual-Selection" 
when lower(Feeder_Video) like '%trailer%' then "Manual-Selection" -- all trailers are manual
when Last_Actions like '%Manual%' and Video_Start_Type = 'Vdo_End' then "Manual-Selection"
when Last_Actions = 'Auto-Play' and Video_Start_Type = 'Vdo_End'and Feeder_Video not like "%trailer%" then "Auto-Play" 
when Last_Actions = 'Clicked-Up-Next' and Video_Start_Type = 'Vdo_End'and Feeder_Video not like "%trailer%" then "Clicked-Up-Next" 
when Feeder_Video <> Display_Name and Episode_Time > 0 and num_seconds_played_no_ads is not null then "Auto-Play"-- episode attribution
when Feeder_Video <> Display_Name and Feeder_Video != "" and Episode_Time = 0 and num_seconds_played_no_ads <= 30 then "Auto-Play"--only watch one show and watch less than 30s
when Feeder_Video <> Display_Name and Feeder_Video != "" and Episode_Time = 0 and num_seconds_played_no_ads > 30 then "Unattributed" -- only watch one show and watch more than 30s
when Video_Start_Type = "Auto-Play" and (Feeder_Video is null or Feeder_Video = "") then "Manual-Selection" -- if cue-up auto but no feeder videos put it to Manual-Selection
else Video_Start_Type
end as Video_Start_Type,
device_name,
Feeder_Video,
Feeder_Video_Id,
Display_Name,
video_id,
num_seconds_played_no_ads,
case when Feeder_Video is null or Feeder_Video <> Display_Name then ifnull(num_seconds_played_no_ads,0) + Episode_Time else 0 end as New_Watch_Time
from cte2
order by 1,2,3)

select *
from cte3
order by 1,2,3

-- select cte2.*,
-- sum(cte2.New_Watch_Time) over (partition by Adobe_Date) as Watch_Total
-- from 
-- (select 
-- Adobe_Date,
-- Video_Start_Type,
-- count(distinct Adobe_Tracking_ID) as Unique_Accounts,
-- round(sum(New_Watch_Time)/3600,2) as New_Watch_Time,
-- from cte1
-- group by 1,2) cte2
-- order by 1,2,4 desc
