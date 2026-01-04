# SBI CAN2025 Data Pipeline Challenge

<div align="center">
  <img src="assets/logo.png" alt="SBI CAN2025 Logo" width="250"/>
</div>

> **Transforming raw football statistics into championship-winning insights through advanced data engineering.**

## Project Overview
An end-to-end data engineering pipeline designed for the **SBI CAN 2025 Challenge**. This project automates the ingestion, processing, and visualization of football performance data using a modern tech stack including **Kafka, Spark, Airflow, PostgreSQL, Grafana, and Streamlit**.

---

## System Architecture
The pipeline follows a Lambda architecture for scalable and reliable data processing. It ingests real-time data via **Kafka**, processes it using **Apache Spark**, stores it in **HDFS & PostgreSQL**, and orchestrates everything with **Apache Airflow**.

---

## Orchestration with Airflow
We use **Apache Airflow** to schedule and monitor the entire workflow. The DAG handles:
1.  **Data Ingestion**: Streaming data from CSV to Kafka and HDFS.
2.  **Spark ETL**: Transforming raw data into a Star Schema (Gold Layer).
3.  **Data Quality Checks**: Ensuring data integrity before loading into the warehouse.

![Airflow DAG](assets/image3.png)
![Airflow DAG](assets/image2.png)

---

## Data Visualization

### Dashboard Overview
A comprehensive **Grafana dashboard** provides high-level insights into player performance, including goals, assists, and passing accuracy across different teams.

![Dashboard Overview](assets/image.png)

### Filtered Analysis
Interactive filters allow users to drill down into specific player stats, comparing potential MVPs and analyzing defensive contributions in real-time.

![Filtered Dashboard](assets/image1.png)

---

## RAG Chatbot (Streamlit)
A **Streamlit-based AI Assistant** powered by **RAG (Retrieval-Augmented Generation)** allows users to query the dataset using natural language. It retrieves relevant documentation and stats to provide context-aware answers about the players and tournament rules.

![Filtered Dashboard](assets/streamlit.png)

---

### Conclusion
**Leveraging the power of data to redefine how we analyze the beautiful game.**
