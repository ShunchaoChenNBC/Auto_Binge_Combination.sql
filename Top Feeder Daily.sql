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

---------------------------------Map Here so we can do the aggregation after mapping

cte2 as (
select regexp_replace(lower(episode_title), r"[:,.&'!]", '') as Epsiodes, 
case when length(display_name) <= 4 
          or lower(display_name) like "%tv" 
          or lower(display_name) like "%)" 
          or lower(display_name) like "%-dt"
          or lower(display_name) like "%premium"
          or lower(display_name) in ('ktvh-dt','ksnv-dt','Kgwn.2') -- add extreme cases here
          or regexp_contains(display_name, r"(W)[a-zA-Z0-9]+-[a-zA-Z0-9]")
          or regexp_contains(display_name, r"(K)[a-zA-Z0-9]+-[a-zA-Z0-9]")
          then null 
          else regexp_replace(lower(display_name), r"[:,.&'!]", '') --clean series here
          end as Series,-- remove platform names
count (display_name) as Display_Time
from `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO`
where 1=1
and episode_title is not null 
and lower(episode_title) not in ('yellowstone',
                                'quantum leap',
                                'dateline nbc',
                                'pft live',
                                'americas got talent all stars') -- extend the list to fix the wrong raw data
and adobe_date = current_date("America/New_York")-1
group by 1,2
),

Mapping_Middle as (
select Epsiodes, 
Series,
dense_rank() over (partition by Epsiodes order by Display_Time desc) as rk
from cte2
where Series is not null and Series != "N/a" and Epsiodes is not null and Epsiodes != "n/a"
order by 3 desc),

Mapping as (
select Epsiodes, 
Series
from Mapping_Middle
where rk = 1 --- Only keep the highest value
),
------------------------------ Above is the block for Mapping Table 

Combinations as (
select cr.*,
case when m.Series is not null then m.Series else regexp_replace(lower(cr.Feeder_Video), r"[:,.&'!]", '') end as Combination
from click_Ready cr
left join Mapping m on m.Epsiodes = regexp_replace(lower(cr.Feeder_Video), r"[:,.&'!]", '')
),


rank_set as (select 
Adobe_Date,
Combination as Auto_Binge_Source_Titles,
Unique_Auto_Binge_Accounts,
Unique_Click_Next_Accounts,
Total_Unique_Accounts,
dense_rank() over (partition by Adobe_Date order by Total_Unique_Accounts desc) as Daily_Ranks
from
(select 
Adobe_Date,
Combination,
count(distinct case when Video_Start_Type = "Auto-Play" then Adobe_Tracking_ID else null end) as Unique_Auto_Binge_Accounts,
count(distinct case when Video_Start_Type = "Clicked-Up-Next" then Adobe_Tracking_ID else null end) as Unique_Click_Next_Accounts,
count(distinct case when Video_Start_Type in ("Clicked-Up-Next","Auto-Play") then Adobe_Tracking_ID else null end) as Total_Unique_Accounts,
from Combinations 
where lower(Binge_Details) like "%series%cue%up%" 
and Combination is not null 
and Combination != "view-all" -- remove epsiode-to-epsiode cases and "View-All"
and Combination not in (SELECT 
                         regexp_replace(lower(content_channel), r"[:,.&'!]", '')
                         FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` 
                         WHERE 1=1
                         and adobe_date = current_date("America/New_York")-1
                         and content_channel != "N/A"
                         group by 1)  -- remove linear channels from the result
group by 1,2) a)

select 
Adobe_Date,
Daily_Ranks,
Auto_Binge_Source_Titles,
Unique_Auto_Binge_Accounts,
Unique_Click_Next_Accounts,
Total_Unique_Accounts
from rank_set
where Daily_Ranks <= 50
order by 1,6 desc
