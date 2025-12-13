
# Overview
This repository implements a lambda + lakehouse architecture for streaming audio metadata / events using Spark Streaming, Kafka, Airflow, dbt, Docker and AWS components. It demonstrates how to ingest streaming events, process them in near real-time, store them in a lakehouse, and orchestrate batch/stream pipelines for analytics and downstream consumers.

# Primary goals

- Ingest streaming events (song plays, user actions, metadata) reliably via Kafka.
- Process streams with Spark Streaming (structured streaming) and write to a Lakehouse (S3 + Parquet + Redshift).
- Build transform layers and analytical models with dbt.
- Orchestrate pipelines using Airflow.
- Provide repeatable deployments using Docker and AWS (ECR, S3, EMR).
- Demonstrate best practices for monitoring.

# Key technologies

- Kafka (ingest, pub/sub)
- Apache Spark (stream processing — Structured Streaming)
- Lakehouse storage on AWS S3 (Parquet + Redshift)
- dbt (transformations, modeling)
- Apache Airflow (orchestration)
- Docker / docker-compose
- AWS tooling: S3, EC2, EMR, IAM. Redshift
- Python

# Data flow

- Producers (application, mobile, web) publish events to Kafka topics (eventsim will handle this stage).
- Spark Structured Streaming consumes topics, enriches and applies streaming transformations.
- Processed streams are written to the Lakehouse on S3 (partitioned Parquet format).
- Batch/near-real-time dbt models run to produce curated tables and marts.
- Airflow orchestrates dbt runs, Spark job scheduling, and operational tasks (cleanups, backups).
- PowerBI shows realtime/batch dashboards for analytic purposes.
