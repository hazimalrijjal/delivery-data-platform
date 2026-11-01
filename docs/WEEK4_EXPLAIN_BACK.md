# Week 4: explain it back

1. **Why are two engines (dbt on DuckDB and PySpark) producing the same table, and what does the reconciliation test prove?**
2. **What went wrong with `64.85 * 0.1` in floating point, and why did it only show up when two engines were compared?**
3. **Why must money be a decimal, and where in the dbt models and the Spark job is that enforced?**
4. **Event time vs processing time: which one do the windows use, and why?**
5. **What is a watermark? What happens to an event that arrives after the watermark has passed its window?**
6. **Why is `dropDuplicatesWithinWatermark` used instead of `dropDuplicates`?** (state would grow forever)
7. **Why does the test put the late event in batch 4 and not batch 2?** (two chained stateful steps)
8. **Append mode vs update mode: why does the Parquet sink need append, and why do the newest windows not appear yet?**
9. **What is the checkpoint folder for? What would happen without it after a restart?**
10. **Why is the Kafka message key the order id?** (same partition, same order)
11. **Why did the first version of the streaming tests only pass when the machine's time zone was UTC?**
12. **Which parts of this week have not been run against a real Kafka broker, and how would you say so in an interview?**
