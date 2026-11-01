-- Chart: table. What the pipeline found and fixed or excluded (the same numbers the Airflow gate reads).
select check_name, severity, rows_affected, rows_checked, pct_affected
from marts.dq_report
order by rows_affected desc
