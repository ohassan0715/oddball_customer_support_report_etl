CREATE OR REPLACE VIEW customer_support_report AS
select 
    date_trunc('MONTH', interaction_start)::date    as month,
    i.channel,
    cc.contact_center_name,
    coalesce(sc.department,'Unknown')       as department,
    count(distinct interaction_id)          as total_interactions,
    count(distinct 
        case when channel = 'phone'
        then interaction_id end)            as total_calls,
    sum(i.call_duration_minutes)            as total_call_duration
from interactions i
left join contact_centers cc on i.contact_center_id = cc.contact_center_id
left join service_categories sc on i.category_id = sc.category_id
group by 1,2,3,4
;
-- Q1: What were the total number of interactions handled by each contact center in Q1 2025?
select 
    concat('Q', quarter(month),' ',year(month)) as quarter,
    contact_center_name,
    sum(total_interactions) as total_interactions
from customer_support_report
where quarter(month) = 1
group by 1,2
-- debug: interactions may be incorrect for contact centers.
-- Validated live
/*
Atlanta GA SE	8
Richmond VA E	7
Boston MA NE	13
*/
;
-- Q2: Which month (Jan, Feb, or Mar) had the highest total interaction volume?
Select
    month,
    sum(total_interactions) as total_interactions,
    RANK() OVER(order by sum(total_interactions) desc) as rank
from customer_support_report
group by 1
order by 3
-- February has the highest interaction volume (10 interactions).
;
-- Q3: Which contact center had the longest average phone call duration (total_call_duration)?
select
    contact_center_name,
    floor((sum(total_call_duration))/sum(total_calls),2) as avg_call_duration,
    RANK() OVER(order by (sum(total_call_duration))/sum(total_calls) desc) as rank
from customer_support_report
where channel = 'phone'
group by 1
order by 3
;
-- Boston had the highest avg call duration (12.72 minutes)
-- Why might this be the case based on the interactions data?
select *,
    timediff(MINUTE,agent_resolution_timestamp,interaction_end) as timediff
from interactions
--where contact_center_id = 'CC001' --Boston
where channel = 'phone'
order by call_duration_minutes desc

-- What approach would you recommend to measure agent work time more accurately?
-- Boston's the only center that uses a survey, adding 5 minutes to total duration


