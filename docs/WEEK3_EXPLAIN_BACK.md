# Week 3: explain it back

1. **What is a sensor, and why `mode="reschedule"`?**
2. **Why does `dq_gate` have `retries=0` while `load_raw` has `retries=2`?**
3. **What does `catchup=False` stop from happening? What would `max_active_runs=1` protect here?**
4. **Why is `load_raw` safe to re-run?** (delete this batch's rows, then insert, inside one transaction)
5. **Why is the logic in `pipeline/tasks.py` and not inside the DAG file?** (testing without Airflow)
6. **What does `adapter.dispatch` do, and why is `quantile_cont` wrapped in a macro?**
7. **Why did `dbt.type_float()` change the numbers on DuckDB?** (32-bit float) And why is `delivery_minutes` a decimal now? (an average that lands exactly on x.x5 rounded differently depending on summation order)
8. **What breaks in the BigQuery sandbox and how was it handled?** (no DML: merge, delete)
9. **TIMESTAMP vs DATETIME in BigQuery: what failed and how was it fixed?**
10. **What is proven by "the compiled SQL parses as BigQuery SQL", and what is not?**
