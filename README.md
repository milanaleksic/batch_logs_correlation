# Correlate AWS Batch jobs and logs

Allows deep debugging of async flows by correlating information from centralized logging
with the AWS Batch job listing to analyze which action caused which job, the statuses of current jobs
in lack of deep search API of AWS Batch jobs.

Usage with 3 status responses from AWS Batch ListJobs API and a single extracted CSV from datadog

```
go run cmd/main.go 
-input-file-batch
succeeded.json
-input-file-batch
failed.json
-input-file-batch
runnable.json
-input-file-logs
extract.csv
```