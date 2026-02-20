# 🎮 League of Legends Lakehouse: End-to-End CBLOL Analytics
An automated, cloud-native Data Engineering platform built to extract, process, and analyze League of Legends competitive match data (CBLOL).

## 🎯 Business Context
This project aims to bridge the gap between raw eSports data and actionable insights. By building a robust self-service BI platform, the goal is to empower Coaching Staffs and Data Analysts to easily evaluate player performance, draft statistics, and match trends. This automates the heavy lifting of data engineering, freeing analysts to focus purely on game strategy and decision-making.

## Architecture
<img width="2586" height="1030" alt="project_diagram_lol_lakehouse" src="https://github.com/user-attachments/assets/d95d0452-6605-4dd0-b8fa-c4620ce73f5a" />

## 🛠️ Tech Stack
* **Infrastructure as Code:** Terraform
* **Cloud Provider:** Microsoft Azure (ADLS Gen2, Databricks, Key Vault)
* **Orchestration:** Apache Airflow (Dockerized)
* **Data Processing:** PySpark & Delta Lake (Medallion Architecture)
* **Language:** Python 3.12 (Requests, API Integration)

## Week 1: The Foundation (Completed)
* Repository initialized with strict `.gitignore`.
* Local Airflow environment provisioned via `docker-compose`.
* Cloud Infrastructure deployed using Terraform:
    * Resource Group created.
    * ADLS Gen2 initialized with Hierarchical Namespace (Bronze, Silver, Gold zones).
    * Databricks Workspace instantiated.

## Next Steps: Week 2 (Data Ingestion)
* Develop Python scripts to extract match data from the Riot Games API.
* Implement robust logic to handle API Rate Limits and pagination.
* Ingest raw JSON data into the Bronze layer of the Azure Data Lake.
