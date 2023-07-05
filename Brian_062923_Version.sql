DECLARE sdate DATE DEFAULT '2023-06-08' ; --FROM DATE
DECLARE fromdate DATE DEFAULT CURRENT_DATE()-21 ; --FROM DATE
DECLARE todate DATE DEFAULT CURRENT_DATE()-3;     --TO DATE

-- VID LOOKUP TABLE BASE
with lookupt as (
SELECT
post_evar28, -- SHOW FULL (BINGE TO)
post_prop21, -- VIDEO ID
post_evar72, -- BINGE FROM CLEAN
count(post_evar56),--COUNT OF UIDs
RANK() OVER (PARTITION BY post_prop21 ORDER BY count(post_evar56) DESC) AS day_rank --GETS THE MOST CORRECT METADATA
FROM `nbcu-ds-prod-001.feed.adobe_clickstream` 
WHERE adobe_date_est BETWEEN fromdate AND todate
AND post_evar28 IS NOT NULL
AND post_prop21 IS NOT NULL
GROUP BY 1,2,3
ORDER BY 2,1,4 ASC
),

--LOOKUP TABLE - ONLY TOP TITLE FOR EACH CONTENT/VIDEO ID
lookup2 as (
SELECT post_evar28 as show1,post_evar72 as show2,
post_prop21 as vid FROM lookupt WHERE day_rank = 1),


--BINGE DETAILS
binge1 as (
SELECT  
DATE_SUB(RevisedTimeStamp, INTERVAL 5 hour) as RevisedTimeStamp1,--TIMESTAMP
post_prop47,    -- CUE UP FULL
post_evar72,    -- BINGE FROM CLEAN
lookup2.show2,
post_evar59,
SPLIT(post_prop47, '|')[SAFE_OFFSET(4)] as vid,
post_evar56 as userid-- UIDs
FROM `nbcu-ds-prod-001.feed.adobe_clickstream` as b1
left join lookup2  ON  lookup2.vid = SPLIT(post_prop47, '|')[SAFE_OFFSET(4)]
WHERE adobe_date_est BETWEEN fromdate AND todate
--AND post_prop47 like 'sle%'
AND post_prop47 IS NOT NULL
AND lookup2.show2 IS NOT NULL
AND post_evar56 IS NOT NULL
AND (lower(post_evar28) like '%vanderpump%' OR lower(post_evar28) like '%renfield%' OR lower(post_evar28) like '%yellowstone%')
AND (lower(post_evar28) not like '%trailer%' )
AND (lower(post_prop15) like '%based on a true story%' )
AND (lower(post_prop15) not like '%trailer%' )
AND (post_prop15 like '%|click' OR post_prop15 like '%|auto-play')
),
--BINGERSIDS
bingers1 as (
SELECT distinct userid as userid from binge1-- UIDs
),
--USAGE FOR BINGERS
cons2 as (
SELECT cons1.adobe_timestamp, 
cons1.adobe_tracking_id, 
lower(cons1.program) as theshow, 
ROUND(SUM(cons1.num_seconds_played_no_ads/3600),0) as hours_watched 
FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` as cons1
WHERE adobe_date  BETWEEN fromdate AND todate
--AND num_seconds_played_no_ads> 0
AND num_views_started> 0
AND adobe_tracking_id IN (SELECT distinct userid FROM bingers1)
GROUP BY 1,2,3
),
-- HAVE BINGERS EVER WATCHED BEFORE? 
bingersCons2 as (SELECT DISTINCT a.adobe_tracking_id
FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` as a
WHERE adobe_date BETWEEN sdate AND todate 
AND num_views_started > 0
AND lower(display_name) = 'based on a true story'
AND adobe_tracking_id in (SELECT distinct userid FROM binge1)),


bingersConsOrder as (SELECT adobe_tracking_id, adobe_timestamp as jtimestamp,display_name,binge1.RevisedTimeStamp1 as bingetime,
ROUND(SUM(a.num_seconds_played_no_ads/3600),2) as hours_watched,
RANK() OVER (PARTITION BY adobe_tracking_id
                    ORDER BY adobe_timestamp ASC
                    ) AS day_rank
FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` as a
left join binge1 on a.adobe_tracking_id = binge1.userid
WHERE adobe_date BETWEEN sdate AND todate 
AND num_views_started > 0
AND num_seconds_played_no_ads > 0
AND lower(display_name) IN ('renfield', 'yellowstone', 'based on a true story','vanderpump rules')
AND adobe_tracking_id IN (SELECT * FROM bingersCons2)
GROUP BY 1,2,3,4
ORDER BY 1,2 ASC),


excludedBingers as (SELECT adobe_tracking_id, display_name, MIN(day_rank) FROM bingersConsOrder 
WHERE lower(display_name) = 'based on a true story'
AND (day_rank = 1
OR bingersConsOrder.jtimestamp < CAST(bingersConsOrder.bingetime as datetime))
GROUP BY 1,2
ORDER BY 1,3),

excludedBingersFinal as (SELECT distinct adobe_tracking_id FROM excludedBingers)

--RESULTS
SELECT 
--DATE_TRUNC(binge1.RevisedTimeStamp1,HOUR),
--lower(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(binge1.post_evar72,"\\.+",""),"&",""),"_",""),"'",""),":","")) as Binge_From,
lower(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(binge1.show2,"\\.+",""),"&",""),"_",""),"'",""),":","")) as Binge_To, 
lower(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(cons2.theshow,"\\.+",""),"&",""),"_",""),"'",""),":","")) as Show_Watched, 
count(distinct binge1.userid) as Reach,
SUM(cons2.hours_watched) as Hours
from binge1
left join cons2 on cons2.adobe_tracking_id = binge1.userid
WHERE TIMESTAMP(cons2.adobe_timestamp) > TIMESTAMP_SUB(TIMESTAMP(binge1.RevisedTimeStamp1), INTERVAL 5 minute) 
AND lower(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(binge1.show2,"\\.+",""),"&",""),"_",""),"'",""),":","")) = lower(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(cons2.theshow,"\\.+",""),"&",""),"_",""),"'",""),":",""))
AND lower(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(binge1.show2,"\\.+",""),"&",""),"_",""),"'",""),":","")) like '%based on a true story%'
AND binge1.show2 != binge1.post_evar72
AND binge1.userid NOT IN (SELECT * FROM excludedBingersFinal)
GROUP BY 1,2--,3
ORDER BY 4 desc
