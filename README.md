# AWS Lakehouse Project

## Overview
In this Data Engineering with AWS project, the objective was to build a data lakehouse solution for sensor data which can be used by data scientists to train a machine learning model.

## Tools/Technologies
Python, Spark, SQL, AWS Glue, S3, Athena, CLI, IAM

## Summary of Loading and ETL Tasks
### Landing Zone
* Use Glue Studio to ingest data from an S3 bucket
* Manually create a Glue Table using Glue Console from JSON data
* Use Athena to query the Landing Zone.

### Trusted Zone
* Configure Glue Studio to dynamically update a Glue Table schema from JSON data
* Use Athena to query Trusted Glue Tables
* Join tables with Glue Jobs
* Filter protected PII with Spark in Glue Jobs

### Curated Zone
* Write a Glue Job to join trusted data
* Write a Glue Job to create curated data

## References
* Udacity cirriculum, project requirements and specifications.
* AWS documentation.



















