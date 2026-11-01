# Week 2: explain it back

Be able to answer these out loud without looking. If you cannot, ask for a line-by-line walkthrough of that file.

1. **Why is `fct_orders` incremental, and what does "1 day overlap" protect against?** (late rows; the watermark comes from the table itself)
2. **What does `unique_key` + `delete+insert` do when an order is processed twice?**
3. **When must you run `--full-refresh`?** (the promised-minutes change in this project is the real example: old rows kept old values)
4. **`mart_customer_orders`: what does `row_number() over (partition by customer_id order by placed_ts, order_id)` return, and why is `order_id` in the order by?** (ties)
5. **`lag()` vs a self join: what does lag give you for the first order of a customer?** (NULL)
6. **Rolling 7-day average in `mart_courier_daily`: why `range between interval 6 days preceding` and not `rows between 6 preceding`?** (courier-days with no deliveries have no row)
7. **Why are flagged orders excluded from the fact table instead of deleted from staging?**
8. **`dq_report`: which checks are "fixed" and which are "excluded", and how do you know the numbers are right?** (manifest.json)
9. **What is GMV here, and how is commission computed?**
10. **Why is the first cohort's retention so high?** (left-censoring)
