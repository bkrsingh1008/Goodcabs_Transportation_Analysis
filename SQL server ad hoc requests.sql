use resume_challenge;
select * from city_target_passenger_rating;
select * from monthly_target_new_passengers;
select * from monthly_target_trips;
select * from dim_city;
select * from dim_date;
select * from dim_repeat_trip_distribution;
select * from fact_passenger_summary;
select * from fact_trips;

EXEC sp_rename 'fact_trips.distance_travelled(km)', 'distance_travelled_km', 'COLUMN';


-- Business Request-1 City-Level fare and trip summary report --
with cte as(
select 
dc.city_name, 
count(ft.trip_id) as 'total_trips',
sum(ft.fare_amount) as 'total_revenue', 
sum(ft.distance_travelled_km) as 'total_distance_travelled'
from dim_city dc join fact_trips ft
on dc.city_id = ft.city_id
group by dc.city_name)
select *, 
(total_revenue/total_distance_travelled) as 'avg_fare_per_km',
(total_revenue/total_trips) as 'avg_fare_per_trip',
concat(round((total_trips * 100 / (SELECT SUM(total_trips) FROM cte)),2),'%') as '%_contribution_to_total_trips' 
from cte;

-- Business Request-2 Monthly City-Level Trips Target Performance Report --
with actual_trips as (
select city_id, DATENAME(month, date) as 'month_name', DATENAME(year, date) as 'year', COUNT(*) as 'actual_trips'
from fact_trips
group by city_id, DATENAME(month, date), DATENAME(year, date)),

target_trips as (
select city_id, DATENAME(month, month) as 'month_name', DATENAME(year, month) as 'year', SUM(total_target_trips) as 'target_trips'
from monthly_target_trips
group by city_id, DATENAME(month, month), DATENAME(year, month)
),

result_query as (
select c1.city_id, c1.month_name, c1.actual_trips, c2.target_trips,
case 
when c1.actual_trips > c2.target_trips then 'Above Target' else 'Below Target' end as 'performance_status',
(c1.actual_trips - c2.target_trips)*100/ c2.target_trips as '%_difference'
from actual_trips c1 join target_trips c2
on c1.city_id = c2.city_id and c1.month_name = c2.month_name and c1.year = c2.year)

select c3.city_id, dc.city_name , c3.month_name, c3.actual_trips, c3.target_trips,
case when c3.actual_trips > c3.target_trips then 'Above Target' else 'Below Target' end as 'performance_status',
(c3.actual_trips - c3.target_trips)*100/ c3.target_trips as '%_difference'
from result_query c3 join dim_city dc
on c3.city_id = dc.city_id;

-- Business Request-3 City-Level Repeat passenger trip frequency report --
with cte as (
select dc.city_id, td.trip_count, SUM(td.repeat_passenger_count) as 'repeat_passenger_per_trips'
from dim_city dc join dim_repeat_trip_distribution td
on dc.city_id = td.city_id
group by dc.city_id, td.trip_count),

cte1 as (
select c1.city_id, 
sum(case when c1.trip_count = '9-Trips' then c1.repeat_passenger_per_trips end)*100/sum(c1.repeat_passenger_per_trips) as '9-Trips',
sum(case when c1.trip_count = '8-Trips' then c1.repeat_passenger_per_trips end)*100/sum(c1.repeat_passenger_per_trips) as '8-Trips',
sum(case when c1.trip_count = '7-Trips' then c1.repeat_passenger_per_trips end)*100/sum(c1.repeat_passenger_per_trips) as '7-Trips',
sum(case when c1.trip_count = '6-Trips' then c1.repeat_passenger_per_trips end)*100/sum(c1.repeat_passenger_per_trips) as '6-Trips',
sum(case when c1.trip_count = '5-Trips' then c1.repeat_passenger_per_trips end)*100/sum(c1.repeat_passenger_per_trips) as '5-Trips',
sum(case when c1.trip_count = '4-Trips' then c1.repeat_passenger_per_trips end)*100/sum(c1.repeat_passenger_per_trips) as '4-Trips',
sum(case when c1.trip_count = '3-Trips' then c1.repeat_passenger_per_trips end)*100/sum(c1.repeat_passenger_per_trips) as '3-Trips',
sum(case when c1.trip_count = '2-Trips' then c1.repeat_passenger_per_trips end)*100/sum(c1.repeat_passenger_per_trips) as '2-Trips',
sum(case when c1.trip_count = '10-Trips' then c1.repeat_passenger_per_trips end)*100/sum(c1.repeat_passenger_per_trips) as '10-Trips'
from cte c1
group by c1.city_id
)
select * from cte1;

-- Business Request-4 Identify cities with highest and lowest total new passengers --
with cte as (
select dc.city_name, SUM(new_passengers) as 'total_new_passengers'
from fact_passenger_summary fps join dim_city dc on
fps.city_id = dc.city_id
group by dc.city_name),

cte1 as (
select *,
RANK() over(order by total_new_passengers) as 'lowest_rank',
RANK() over(order by total_new_passengers desc) as 'highest_rank'
from cte c1)

select c2.city_name, c2.total_new_passengers, case when highest_rank <=3 then 'Top 3' else 'Bottom 3' end as 'city_category'
from cte1 c2
where highest_rank <=3 or lowest_rank <=3;

-- Business Request-5 Identify month with highest revenue for each city --
with cte as (
select city_id, SUM(fare_amount) as 'total_revenue'
from fact_trips
group by city_id),

cte1 as (
select city_id, DATENAME(month, date) as 'monthName', SUM(fare_amount) as 'revenue'
from fact_trips
group by city_id, DATENAME(month, date)),

cte2 as (
select c1.city_id, c2.monthName, c2.revenue, c1.total_revenue, cast(round((c2.revenue*100/c1.total_revenue),2) as decimal(10,2)) as '%_contribution'
from cte c1 join cte1 c2
on c1.city_id = c2.city_id),

cte3 as (
select dc.city_name, (c3.monthName) as 'highest_revenue_month', c3.revenue, c3.[%_contribution]
from cte2 c3 join dim_city dc 
on c3.city_id = dc.city_id
)

select * from (
select *,
RANK() over(partition by city_name order by revenue desc) as 'rnk'
from cte3) as A
where A.rnk = 1;

-- Business Request-6 Repeat Passenger Rate Analysis --
with cte as (
select city_id, DATENAME(month, month) as 'month', repeat_passengers, total_passengers,
cast(round((repeat_passengers*100/total_passengers),2) as decimal(10,2)) as 'repeat_passenger_rate(%)'
from fact_passenger_summary),

cte1 as (
select cte.city_id,
cast(round((SUM(cte.repeat_passengers)*100/SUM(cte.total_passengers)),2) as decimal(10,2)) as 'overall_repeat_passengers(%)'
from cte
group by city_id),

cte2 as (
select cte.city_id, cte.month, cte.repeat_passengers, cte.total_passengers, cte.[repeat_passenger_rate(%)], cte1.[overall_repeat_passengers(%)]
from cte join cte1 
on cte.city_id = cte1.city_id)

select dc.city_name, cte2.month, cte2.repeat_passengers, cte2.total_passengers, cte2.[repeat_passenger_rate(%)],
cte2.[overall_repeat_passengers(%)]
from cte2 join dim_city dc on
cte2.city_id = dc.city_id;








