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
and DATE(timestamp(post_cust_hit_time_gmt), "America/New_York") = current_date("America/New_York")-1),

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
where Video_Start_Type is not null),

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

rank_set as (select 
Adobe_Date,
Feeder_Video,
Unique_Auto_Binge_Accounts,
Unique_Click_Next_Accounts,
Total_Unique_Accounts,
dense_rank() over (order by Total_Unique_Accounts desc) as Daily_rank
from
(select 
Adobe_Date,
regexp_replace(lower(Feeder_Video), r"[:,.']", '') as Feeder_Video, -- remove punctunation from display name
count(distinct case when Video_Start_Type = "Auto-Play" then Adobe_Tracking_ID else null end) as Unique_Auto_Binge_Accounts,
count (distinct case when Video_Start_Type = "Clicked-Up-Next" then Adobe_Tracking_ID else null end) as Unique_Click_Next_Accounts,
count (distinct case when Video_Start_Type in ("Clicked-Up-Next","Auto-Play") then Adobe_Tracking_ID else null end) as Total_Unique_Accounts,
from click_Ready
where lower(Binge_Details) like "%series%cue%up%" and Feeder_Video is not null and Feeder_Video != "view-all" -- remove epsiode-to-epsiode cases and "View-All"
group by 1,2) a),


Mapping as (
select Epsiodes, STRING_AGG(display_name order by display_name) as Series -- concat mutiple values to one
from
(select regexp_replace(lower(episode_title), r"[:,.']", '') as Epsiodes, display_name
from `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO`
where 1=1
and episode_title is not null 
and lower(episode_title) not in ('yellowstone',
                                'quantum leap',
                                'dateline nbc',
                                'pft live',
                                'americas got talent all stars') -- extend the list to fix the wrong raw data
and adobe_date = current_date("America/New_York")-1
group by 1,2) b
where display_name is not null and display_name != "N/a"
group by 1
)

select 
Adobe_Date,
Daily_rank,
Feeder_Video,
Series,
Unique_Auto_Binge_Accounts,
Unique_Click_Next_Accounts,
Total_Unique_Accounts
from
(select 
Adobe_Date,
Daily_rank,
Feeder_Video,
Unique_Auto_Binge_Accounts,
Unique_Click_Next_Accounts,
Total_Unique_Accounts
from rank_set
where Daily_rank <= 50) c
left join Mapping d on trim(c.Feeder_Video) = trim(d.Epsiodes)
order by 1,7 desc







