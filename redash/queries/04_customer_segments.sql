-- Chart: pie or bar. Category = segment, value = customers.
select segment, count(*) as customers
from marts.dim_customers
group by segment
order by customers desc
