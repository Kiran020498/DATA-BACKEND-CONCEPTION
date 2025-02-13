Project Overview

This project involves setting up and utilizing Apache Spark, Kafka, and Zookeeper for data processing and analysis. The dataset used is Daily Household Transactions.csv, and the analysis is conducted using Scala within a Spark environment.

Contents

1. Configuration and Setup

docker-compose.yml: Defines the services required to run Kafka and Zookeeper in Docker.

initializing docker.txt: Instructions for setting up Docker.

installing jdk.png: Screenshot showing JDK installation.

installializing zooker.txt: Instructions for initializing Zookeeper.

zookeeper started in docker desktop through cmd.png: Screenshot confirming Zookeeper startup.

kafka started in docker desktop through cmd.png: Screenshot confirming Kafka startup.

kafka and zookeeper started in docker desktop.png: Another confirmation screenshot.

2. Kafka Operations

creating test topic on kafka broker.png: Screenshot showing Kafka topic creation.

testing kafka producer send message to kafka test topic.png: Testing Kafka producer by sending a message.

kafka producer successfully sent message to kafka test topic.png: Confirmation of message transmission.

data ingested thorugh kafka.png: Screenshot showing data ingestion via Kafka.

3. Apache Spark Analysis

analysis.scala: Scala script for data processing in Spark.

basic func check on apache spark through scala.png: Screenshot of basic function testing in Spark.

dataframe created in spark environment.png: Shows a DataFrame created in Spark.

initial data stats.png: Initial descriptive statistics of the dataset.

formatted the date column.png: Screenshot of date formatting applied.

discriptive stats.png: Further descriptive statistics.

Apache spark has been set up and working.png: Confirmation of Spark setup and execution.

4. Dataset

Daily Household Transactions.csv: The dataset used for analysis.

Setup Instructions

Ensure Docker and Docker Compose are installed.

Start Zookeeper and Kafka using:

docker-compose up -d

Verify Kafka and Zookeeper are running using:

docker ps

Create a test topic in Kafka.

Run the analysis.scala script in Apache Spark to process the dataset.