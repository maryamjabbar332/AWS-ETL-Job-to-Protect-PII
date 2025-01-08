# AWS-ETL-Job-to-Protect-PII
AWS ETL Job to Protect PII

#### AWS Glue ETL Job for Protecting PII
This repository contains a Python script for an AWS Glue ETL job that processes sanctions data and protects Personally Identifiable Information (PII). The job identifies sensitive entities in the dataset, applies transformations, ensures data quality, and outputs a cleaned and compliant dataset to an S3 bucket.

#### Documentation
Overview
This AWS Glue ETL job processes a dataset containing sanctions data, identifies PII (e.g., names), and redacts sensitive information. The workflow ensures data quality while securely storing the cleaned data in an S3 bucket in Parquet format.

The job is ideal for compliance with data privacy regulations like GDPR, CCPA, or similar standards.

#### ETL Job Steps
The ETL job follows these steps:

#### Load Data:

The sanctions data is ingested from a CSV file stored in an S3 bucket.
### Transformations:

### Drop unnecessary fields (e.g., "Phones").
Detect and redact sensitive information (e.g., replace detected names with "Hide").

### Data Quality Validation:

Ensures the dataset meets defined quality rules (e.g., column count > 0).

### Output Processed Data:

Writes the cleaned dataset to an S3 bucket in Parquet format with Snappy compression.

###### Input & Output
Type	Details
Input	Sanctions dataset stored in an S3 bucket (Sanctioned_List_By_Countries.csv) in CSV format.
Output	Cleaned dataset stored in another S3 bucket in Parquet format (pii-cleaned-db/pii-data).
