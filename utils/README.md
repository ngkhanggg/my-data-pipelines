# Utils

This folder contains utility scripts and helper functions that provide reusable features across multiple script.

## Contents

- `log_utils`: This script records job events to a dedicated PostgreSQL log table and notifies designated recipients about job status via email.
- `postgresql_utils`: This script handles establishing a connection to any PostgreSQL table via retrieving connection information from a secret manager and performs several read/write/DML SQL queries.
- `sns_notification_utils`: This script helps send emails to a SNS topic so that the topic can notify its configured recipients.
