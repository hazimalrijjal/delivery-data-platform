-- Chart: cohort or table. One row per cohort month and months since the first order.
-- Note: the first cohort contains customers who already existed before the data starts, so it looks high.
select cohort_month, months_since_first, active_customers, cohort_size, retention
from marts.mart_cohort_retention
order by cohort_month, months_since_first
