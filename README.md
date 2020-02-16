# s3-lambda-glue

*s3-lambda-glue* is a lambda piece of work which helps in data migration from on-prem to RDS and provides an easy-and-fastway to migrate your data to AWS Cloud applications.

Currently, the focus is primarily on migrating data using CSV or Delimited file.

# Overview

*s3-lambda-glue* is a part of Lambda-Glue Architecture for data migration solution

![s3-lambda-glue](https://github.com/sandeep-bharadwaj-bheemaraju/s3-lambda-glue/blob/master/dashboard/web/img/s3-lambda-glue.png)

# Solution Design

To migrate the data from legacy system to AWS database, *s3-lambda-glue* acts as a trigger for the whole data migration and follows below steps.

* **Legacy system shall export the data to S3 bucket in agreed CSV format.**
* **Upon successful upload, S3 shall trigger a notification message to s3-lambda-glue.**
* **s3-lambda-glue shall read the configuration from Dynamo DB and moves the files from ready directory to in-process directory.**
* **Triggers AWS Glue Job.**
* **Persists the received job id, file names and job status to Dynamo DB.**

## Requirements

* `java`/`javac` (Java 8 runtime environment and compiler)
* `gradle` (The build system for Java)

Step by Step installation procedure is clearly explained in the **Lambda Glue Architecture for Batch Processing.pdf** file in the repository.

