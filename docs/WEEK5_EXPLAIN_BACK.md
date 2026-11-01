# Week 5: explain it back

Answer in your own words (2-4 sentences each). These are the questions an interviewer is likely to ask.

1. What is Terraform, and why create the datasets with it instead of letting dbt create them?
2. What does `terraform plan` do that `terraform apply` does not? Why run it first?
3. Why did you have to delete the old datasets before the first `terraform apply`?
4. Why does Redash use a service account with only "Data Viewer" on `marts` and "Job User" on the project? What could go wrong with your own login?
5. Why is the JSON key kept out of the project folder and out of GitHub?
6. Look at the architecture diagram. Follow one order from the CSV file to a chart on the dashboard and name every step.
7. The dashboard queries read only `marts`. Why not `raw` or `staging`?
8. You found 709489.92 vs 709496.31 for commission. How did the cross-engine check find the bug, and what was the fix?
9. Which parts of this project did you run yourself and which were never run by anyone? (Be exact: Terraform and Redash status depends on what you ran.)
