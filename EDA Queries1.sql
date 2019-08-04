--basic queries
select distinct city_id, count(distinct customer_id) cnt_cust,min(order_date) min_order,max(order_date) max_order,count(distinct route_id) cnt_route,
count(distinct society_id) cnt_society, count(distinct order_date) cnt_date, count(distinct product_id) as cnt_product, sum(total_cost) as total_rev,
sum(product_quantity) as cnt_qty, count(distinct order_id) as cnt_order, count(distinct store_id)as cnt_store  from temp.hack_data_sri group by city_id;

--drop in customers
select rank1,
case when rev <50 then '0-50'
  when rev <100 then '50-100'
  when rev <200 then '100-200'
  when rev <500 then '200-500'
  else '>500' end as rev_bucket, count(distinct customer_id)
from
(select distinct customer_id, order_date1, rev, row_number() over (partition by customer_id order by order_date1) as rank1 --,min_order_date
from
(select distinct customer_id, order_date1, sum(total_cost) as rev--, min(order_date1) as min_order_date
from (select *, cast(order_date as date) order_date1 from temp.hack_data_sri where subscription =0 and city_id in ('1120336', '1120672', '1120784','1120560','1120448')
and selling_price_per_unit != 0)
 group by customer_id, order_date1)
--where min_order_date< '2019-07-01'
group by  customer_id, order_date1, rev ) 
group by rank1,
(case when rev <50 then '0-50'
  when rev <100 then '50-100'
  when rev <200 then '100-200'
  when rev <500 then '200-500'
  else '>500' end) ;

-- Cust subscription time from joining

select distinct a.city_id,a.customer_id, min_date, sub_min_date,(sub_min_date - min_date) as date_diff, count(distinct order_id) freq, sum(total_cost) as rev, rev/freq as AOV
from
(select * from (select distinct city_id,customer_id, min(order_date1) min_date, min(case when subscription = 1 then order_date1 else current_date end) as sub_min_date  from 
(select *, cast(order_date as date) order_date1 from temp.hack_data_sri) group by city_id,customer_id) )a--where sub_min_date is not null) a
left join
(select *, cast(order_date as date) order_date1 from temp.hack_data_sri) b
on a.customer_id = b.customer_id and b.order_date1 between min_date and sub_min_date
group by a.city_id, a.customer_id, min_date, sub_min_date;

--Repeat Rate, Repeat Freq, Repeat AOV
select city_id,cast(week as numeric) week,count(freq_7) cnt_freq_7,count(freq_14) cnt_freq_14,count(freq_21) cnt_freq_21, count(freq_28) cnt_freq_28,
sum(freq_7) s_freq_7,sum(freq_14) s_freq_14,sum(freq_21) s_freq_21, sum(freq_28) s_freq_28,
avg(aov_seven) aov_7,avg(aov_one_four) aov_14,avg(aov_two_one) aov_21,avg(aov_two_eight) aov_28,
 1.00*s_freq_7/cnt_freq_7 f7,1.00*s_freq_14/cnt_freq_14 f14, 1.00*s_freq_21/cnt_freq_21 f21, 1.00*s_freq_28/cnt_freq_28 f28 from temp.cust_breakdown_sk group by city_id, week;


--AOV for subscribed and unsubscribed
drop table if exists temp.hack_data_sri_order ; 
create table temp.hack_data_sri_order as
select *,
case when cnt_sub >=1 then 1 else 0 end as sub_order
from
(select distinct city_id,route_id,order_date, society_id,customer_id,order_id, sum(subscription) cnt_sub, sum(total_cost) total_rev, sum(product_quantity) as qty, count(product_quantity) as units
from temp.hack_data_sri group by city_id,route_id,order_date, society_id,customer_id,order_id)
;

select city_id,sub_order, society_id, count(distinct customer_id) customer, count(order_id) cnt_order, sum(total_rev)/count(order_id) aov from temp.hack_data_sri_order 
group by city_id,sub_order, society_id ;

select city_id, cutomer_id, sub_order, count(distinct order_id) as freq, sum(total_rev)/count(order_id) aov from temp.hack_data_sri_order
group by city_id, cutomer_id, sub_order;


--Citi analysis
select concat(date_part('year',order_date1),LPAD(date_part('month',order_date1),2,0)) monthyear, city_id, sum(total_cost) total_cost,
sum(product_quantity) product_quantity, count(distinct order_id) order_total 
from (
select cast(order_date as date) order_date1, * from temp.hack_data_sri 
--where subcategory_id = '1125264'
)
--where city_id in ('1120112', '1120224')
group by monthyear, city_id 


--performance of each cohorts of customers across 7 days, 14 days, 21 days, 28 days
drop table temp.cust_breakdown_sk;
create table temp.cust_breakdown_sk as (
select city_id,customer_id, freq_7, freq_14, freq_21, freq_28, 
        case when freq_7 is null then null else rev_7/freq_7 end as aov_seven, 
        case when freq_14 is null then null else rev_14/freq_14 end as aov_one_four, 
        case when freq_21 is null then null else  rev_21/freq_21 end as aov_two_one, 
        case when freq_28 is null then null else  rev_28/freq_28 END as aov_two_eight, week 
from(      
select city_id,customer_id, min_order_date,sum(freq_7) freq_7, sum(freq_14)freq_14, sum(freq_21) freq_21, sum(freq_28) freq_28, 
sum(rev_7) rev_7, sum(rev_14)rev_14, sum(rev_21) rev_21, sum(rev_28) rev_28
from (select a.city_id, a.customer_id, (case when (order_date1 between min_order_date and seven_min_date) then total_order else 0 end) as freq_7,
      (case when (order_date1 between seven_min_date and fourteen_min_date) then total_order else null end) freq_14,
      (case when (order_date1 between fourteen_min_date and two_one_min_date) then total_order else null end) freq_21,
      (case when (order_date1 between two_one_min_date and two_eight_min_date) then total_order else null end) freq_28,
      (case when (order_date1 between min_order_date and seven_min_date) then revenue else null end) as rev_7,
      (case when (order_date1 between seven_min_date and fourteen_min_date) then revenue else null end) rev_14,
      (case when (order_date1 between fourteen_min_date and two_one_min_date) then revenue else null end) rev_21,
      (case when (order_date1 between two_one_min_date and two_eight_min_date) then revenue else null end) rev_28, min_order_date
from (
    (select city_id, customer_id, min(order_date1) min_order_date, min(order_date1)+7 as seven_min_date, 
     min(order_date1)+14 as fourteen_min_date, min(order_date1)+21 as two_one_min_date, min(order_date1)+28 as two_eight_min_date  
     from (SELECT *, cast(order_date as date) order_date1 from temp.hack_data_sri where subscription = 0)
     group by city_id,customer_id) a
     left join
      (select customer_id, week, cast(order_date as date) order_date1, count(distinct order_id) total_order, sum(total_cost) revenue, sum(total_cost)/count(distinct order_id) as aov
      from temp.hack_data_sri a
      left join temp.hack_data_sri_dt_wk b on b.date_1= cast(a.order_date as date)
      group by customer_id,order_date1, week) b     
on a.customer_id = b.customer_id)
--group by a.customer_id, b.order_date1,min_order_date ,seven_min_date, b.total_order
)
group by city_id, customer_id,min_order_date) a0
left join temp.hack_data_sri_dt_wk b0 on b0.date_1= cast(a0.min_order_date as date)
order by freq_7 desc);
