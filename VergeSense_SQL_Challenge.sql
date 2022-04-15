/*
Sense SQL Challenge


Sense has a few dozen fake sensors deployed in a fake customer building. 
All sensors are expected to "report" to the Sense cloud on a roughly 10-minute cadence. 
When a sensor reports, it captures a count of all the people within its field of view.

In this challenge you will be given access to mock Sense data in a PostgreSQL database with 3 tables. 
Two of the tables are static data about a fleet of "Sensors" and their "Partitions". 
The third is a feed of sensor reports and their person counts that have come from those sensors over the course of a week.

Tables: 

sensors: Sense sensor devices alive and reporting in the customer building. Sensors have a primary key id and a name. 

partitions: A partition represents a region of interest under a sensor camera’s field of view. 
A sensor can have one or more partitions. Sensor reports (described below) are specific to a unique sensor + partition pair. 
Note: each time a sensor with multiple partitions reports, we will get multiple new entries in the Person Counts table - one per partition of that sensor.
The Partitions table has a primary key id and foreign key sensor_id, which joins to the Sensors table.

person_counts: Each row in this table corresponds to a report from a sensor over one of that sensor's partitions, plus a primary key id column. 
The columns include:
    • count_timestamp: the timestamp that the report was delivered (in UTC).
    • person_count: the number of people within the sensor's partition at that timestamp.
    • device_id: An identifier of the sensor+partition pair that generated this report. 
                 The device_id is the string concatenation of "<sensor_name> / <partition_id>" 
                 (note the forward slash in the middle, and the fact that a sensor name and a partition id are what are being concatenated).

Challenge 1:
	The person_counts table’s device_id column is in the format: <sensor_name> / <partition_id>. 
       Create a temporary View off of the person_counts table that has sensor_name and partition_id split out into their own columns. 
       This view should also have count_timestamp and person_counts columns. This view can be used in the following challenges.

Challenge 2:
       Find the average person_count across sensor reports with a non-zero person_count by sensor by day across all days. 
       Assume UTC timezones for all timestamps. Feel free to use your View from part 1 in this challenge.
       Output columns should include sensor_name, date, and avg_person_count

Challenge 3:
       Find the “Device Heath” of each sensor across all days. 
       Device Health is a measure of , but with the nuance that a health sensor is expected to report once every ten minutes 
       (though they have been known to over-report or under-report) during working hours (from 9am to 5pm). 
       Feel free to use your View from part 1 in this challenge.
       The Device Health should be represented as a string color with the following conditions:
              Green: >= 99% device health
              Yellow: >=95% device health
              Orange: >=90% device health
              Red: <90% device health
       Output columns should include sensor_name and device_health_color
*/

--Checking the data

--checking that if there is a partition covered by multiple sensors, if exist, it may cause double counting in person.
--in this case, we have 16 distinct partitions all covered by different sensors. no double counting.
--But it seems like partition '109483' does not exist in table partitions, this could be an issue.
with person_counts_view as (
select split_part(device_id, '/', 1) as sensor_name,
       split_part(device_id, '/', 2) as partition_id,
       person_count
from person_counts)
select partition_id,
       p.id partition_id_from_partition_t,
       count(distinct sensor_name) num_sensor,
       string_agg(distinct sensor_name, '|' order by sensor_name) sensor_name_from_counts,
       string_agg(distinct s.name, '|' order by s.name) sensor_name_from_sensor_t,
       sum(person_count) person_count_sum
from person_counts_view pcv
     left outer join partitions p on pcv.partition_id::int = p.id
     left outer join sensors s on p.sensor_id = s.id
group by 1,2
order by 3 desc
;
--Also, seems like there are some duplicate counts in some partitions at some timestamps
select device_id,
       count_timestamp,
       person_count,
       count(device_id),
       max(person_count),
       min(person_count)
from person_counts
group by 1,2,3
having count(device_id) > 1
order by 4 desc
;

--Challenge 1: Create a temporary View off of the person_counts table
create view person_counts_view as (
select distinct split_part(device_id, '/', 1) as sensor_name,
       split_part(device_id, '/', 2) as partition_id,
       count_timestamp,
       person_count
from person_counts
                                  )
;

--Follow up question: In reviewing the person_counts table and others, please note any oddities or discrepancies that you find notable.
--As I stated above, partition '109483' does not exist in table partitions, but it is in person_counts table, this could be an issue

--Challenge 2: Find the average person_count across sensor reports with a non-zero person_count by sensor by day across all days.

--exclude zero person-count reports: When the area covered by the sensor was occupied, this would show how busy that area would be.
--This may indicate the usage of the area (whether the area is too crowded) during business hours.
--We can also use it to predict the date (or day of the week) that the area will be heavily occupied.
select sensor_name,
       count_timestamp::date as Date,
       extract(isodow from count_timestamp::date) as day_of_week,
       count(distinct partition_id) num_partitions,
       avg(person_count) avg_person_count
from person_counts_view
where person_count > 0
group by 1,2
order by 1,2
;

--include zero person-count reports: this may cause bias because it also counts off-work hours that no one would use that area.
select sensor_name,
       count_timestamp::date as Date,
       extract(isodow from count_timestamp::date) as day_of_week,
       avg(person_count) avg_person_count
from person_counts_view
group by 1,2
order by 1,2
;

--However, if look at avg person count in each hour (include zero person-count reports), we could predict when is the best time for cleaning in that area.
select sensor_name,
       count_timestamp::date as Date,
       extract(isodow from count_timestamp::date) as day_of_week,
       extract(hour from count_timestamp) as hour,
       avg(person_count) avg_person_count
from person_counts_view
group by 1,2,3,4
order by 1,2,4
;

--Challenge 3: Find the “Device Health” of each sensor across all days.

--the max time for report is at 2020-9-25 13:04, which means we don't have any data from 2020-9-25 13:10 to 16:49 (4 hours) during working hours
select count_timestamp::date as dates,
       min(count_timestamp),
       max(count_timestamp)
from person_counts_view
group by 1
order by 1
;

--So the main point here is to find out if there is at least 1 report every 10 minutes during working hours
select sensor_name,
       count(every_10_min) as observed_num_reports,
       ((max(count_date) - min(count_date) + 1) --num_days
       * 47) - 4*6 --expected_reports_every_day
       as expected_num_reports,
       round(count(every_10_min)*100.0/ --observed_num_reports
       (((max(count_date) - min(count_date) + 1) --num_days
       * 47) - 4*6), 1) --expected_reports_every_day
       as condition_p,
       case when count(every_10_min)*100.0/ --observed_num_reports
                 (((max(count_date) - min(count_date) + 1) --num_days
                 * 47) - 4*6) --expected_reports_every_day --expected_num_reports
                 >= 99 then 'Green'
            when count(every_10_min)*100.0/ --observed_num_reports
                 (((max(count_date) - min(count_date) + 1) --num_days
                 * 47) - 4*6) --expected_reports_every_day --expected_num_reports
                 >= 95 then 'Yellow'
            when count(every_10_min)*100.0/ --observed_num_reports
                 (((max(count_date) - min(count_date) + 1) --num_days
                 * 47) - 4*6) --expected_reports_every_day --expected_num_reports
                 >= 90 then 'Orange'
            when count(every_10_min)*100.0/ --observed_num_reports
                 (((max(count_date) - min(count_date) + 1) --num_days
                 * 47) - 4*6) --expected_reports_every_day --expected_num_reports
                 < 90 then 'Red' end
       as device_health_color
from (
         select sensor_name,
                count_date,
                every_10_min,
                min(count_timestamp) as First_time_in_10_min
         from (
                  select sensor_name,
                         count_timestamp,
                         count_timestamp::date as count_date,
                         floor(((extract(hour from count_timestamp) - 9) * 60 + extract(minute from count_timestamp)) /
                               10.0)           as every_10_min
                  from person_counts_view
                  where extract(hour from count_timestamp) between 9 and 16
                  group by 1, 2
                  order by 1
              ) a
         group by 1, 2, 3
     ) b
group by 1
order by 1
;

--Follow up question: What might be some reasonable causes for devices that appear to have poor device health, and how might you go about investigating these causes?
--Poor device health may be caused by insufficient battery power in the device.
--or it may be caused by poor signal in the area.
--in these cases, I would replace the device with a newly charged device (with full battery), and investigate it in the next few days or so;
--if the condition changed to Green, then we know it was the insufficient battery issue; if the condition did not change, it may be a signal problem in the area.
