This query is using Google Big Query ;
-- 1. Create a query to get average amount of duration (in minute) per month.
SELECT
  DATE_TRUNC(DATE(start_date), month) AS month,
  ROUND (AVG(duration_sec/60),2) AS avg_durations_minute
FROM
  `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
WHERE
  EXTRACT(year
  FROM
    start_date) IN (2014,
    2015,
    2015,
    2016,
    2017)
GROUP BY
  month
ORDER BY
  month ASC
 
-- 2. Create a query to get total trips and total number of unique bikes grouped by region name
SELECT
  coalesce((info.region_id),
    0) AS region_id,
  coalesce((region.name),
    'unspecified') AS region_name,
  COUNT(trips.trip_id) AS total_trips,
  COUNT(DISTINCT trips.bike_number) AS total_bike
FROM
  `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` AS trips
LEFT JOIN
  `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` AS info
ON
  trips.start_station_id = info.station_id
LEFT JOIN
  `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` AS region
ON
  info.region_id = region.region_id
WHERE
  EXTRACT (year
  FROM
    start_date) BETWEEN 2014
  AND 2017
GROUP BY
  region_name,
  region_id
ORDER BY
  region_id DESC

-- 3. Get latest detail trip in each region :
-trip_id
-Duration_sec
-Start_date
-Start_duration_name
-member_gender

WITH
  X AS (
  SELECT
    trips.trip_id AS trip_id,
    trips.duration_sec AS duration_sec,
    trips.start_date AS start_date,
    trips.start_station_name AS start_station_name,
    coalesce(trips.member_gender,
      'unspecified') AS member_gender,
    region.name AS region,
    ROW_NUMBER() OVER(PARTITION BY region.name ORDER BY start_date DESC) AS rn
  FROM
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` AS trips
  LEFT JOIN
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` AS info
  ON
    trips.start_station_id = info.station_id
  LEFT JOIN
    `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` AS region
  ON
    info.region_id = region.region_id
  WHERE
    trips.end_date >= '2014-01-01'
    AND trips.end_date < '2018-01-01')
SELECT
  *
FROM
  X
WHERE
  rn = 1

-- 4. Create a query to get trip data and add 1 column to show total cumulative total trips in that region

with agg_region as (
SELECT 
  c.name as region_name
  , count(distinct trip_id) total_trips
  FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` as a
  left join `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` as b
    on a.start_station_id = b.station_id
  left join `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` as c
    on b.region_id = c.region_id
  where date_trunc(date(end_date), month) >= '2014-01-01'
  and date_trunc(date(end_date), month) < '2018-01-01'
  group by 1
)

SELECT 
  trip_id
  , duration_sec
  , a.start_date
  , a.start_station_name
  , b.region_id
  , c.name as region_name
  , total_trips
  FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` as a
  left join `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` as b
    on a.start_station_id = b.station_id
  left join `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` as c
    on b.region_id = c.region_id
  left join agg_region  as d
    on c.name = d.region_name
  where date_trunc(date(end_date), month) >= '2014-01-01'
  and date_trunc(date(end_date), month) < '2018-01-01'  
  
  -- 5. Find the youngest and oldest age of the members, of each gender. Assume this year is the length of their life.
  with age_table AS
(
  SELECT
  member_gender,
  SAFE_SUBTRACT(2021,
      member_birth_year) AS age
FROM
  `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
  where date_trunc(date(start_date), month) >= '2014-01-01' and date_trunc(date(start_date), month) < '2018-01-01'
)
SELECT
  member_gender,
  MIN(age) AS youngest_age,
  MAX(age) AS oldest_age
FROM
  age_table
GROUP BY
  1
