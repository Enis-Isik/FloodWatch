# FloodWatch: Real-Time Global Flood Monitoring System

![Status](https://img.shields.io/badge/Status-Active-success)
![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.3-orange)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Latest-red)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)

**FloodWatch** is a distributed streaming application designed to detect and predict flood risks in real-time across five distinct global locations. Unlike static dashboards, FloodWatch treats environmental monitoring as a high-velocity data stream, leveraging **Apache Kafka** for ingestion, **PySpark Structured Streaming** for processing, and a continuous **MLOps** pipeline that retrains the predictive model on the fly.

---

## 📖 Table of Contents
- [Project Overview](#project-overview)
- [System Architecture](#system-architecture)
- [Data Strategy & Sources](#data-strategy--sources)
- [MLOps Pipeline](#mlops-pipeline)
- [Project Structure](#project-structure)
- [Installation & Usage](#installation--usage)

---

## 🚀 Project Overview

This system monitors flood risks in **Washington D.C., St. Louis, New Orleans, Maputo (Mozambique), and Kristianstad (Sweden)**. It ingests live rainfall and river gauge data, processes it through a Machine Learning model, and visualizes the risk status instantly.

**Key Features:**
* **Real-Time Ingestion:** Sub-second data collection from multiple international APIs.
* **Hybrid Data Processing:** Normalizes data from physical sensors (USGS) and hydrological models (Open-Meteo).
* **Continuous Learning:** The system automatically retrains its ML model every 5 minutes using the latest live data without requiring a restart.
* **Hot-Reloading:** The Spark engine detects updated models and swaps them instantly during runtime.

---

## 🏗 System Architecture

The project is containerized using **Docker** to ensure reproducibility and uses **Apache Kafka** as the central nervous system.

### Why Docker?
Docker containerizes the infrastructure (Zookeeper and Kafka), eliminating OS-compatibility issues. It ensures that the message broker runs identically on macOS, Linux, or Windows without polluting the host machine.

### Why Apache Kafka?
Kafka decouples the data ingestion (Producer) from the processing (Spark).
* **Decoupling:** The Producer publishes data to the `flood_data` topic without needing to know if the Spark consumer is active.
* **Fault Tolerance:** Kafka buffers messages if the Spark processor crashes, ensuring zero data loss.

---

## 📊 Data Strategy & Sources

We utilize a **Hybrid Data Pipeline** to unify data from different continents and sensor types.

### Data Sources
1.  **USGS Water Services (USA):** Real-time physical river gauge height (ft) for US cities.
2.  **Open-Meteo Flood API (International):** Hydrological model estimates ($m^3/s$) for Maputo and Sweden.
3.  **OpenWeatherMap:** Real-time 1-hour precipitation data.
4.  **Open-Meteo Weather:** 24-hour accumulated rainfall history (critical for flash flood prediction).

### Preprocessing & Normalization
To make the Machine Learning model universal across different rivers, raw data is normalized:
* **Level Percent:** $Current Level / Flood Threshold$
* This ensures the model sees a standardized "Risk Factor" (where 1.0 = Threshold) rather than arbitrary units like feet or cubic meters.

### Training Data
* **Acquisition:** A custom ETL script (`fetch_historical_data.py`) mined the last 365 days of hourly data for all target locations.
* **Self-Labeling:** We calculated the **95th percentile** of water levels for each river over the past year. Any data point exceeding this mark was labeled "Danger" (1.0), creating a statistically valid ground-truth dataset.

---

## 🧠 MLOps Pipeline

FloodWatch implements a **Continuous Training Loop**:

1.  **Inference:** The Spark Processor predicts risk for incoming data and simultaneously saves raw inputs to a "Live Storage" layer.
2.  **Retraining:** The `train_model.py` service wakes up every 5 minutes.
3.  **Evolution:** It merges the static 1-year history with the new live data collected in the last window.
4.  **Hot Swap:** The model is retrained and overwritten. The Spark Streaming job detects the file change and loads the new "brain" for the next micro-batch.

---

## 📂 Project Structure

| File | Description |
| :--- | :--- |
| `producer.py` | **The Source.** Fetches live data from APIs, handles fallback logic (simulating river rise if sensors fail during rain), and publishes JSON to Kafka. |
| `spark_processor.py` | **The Engine.** A PySpark Structured Streaming job. Features the "Hot-Reload" logic to update the ML model mid-stream. |
| `train_model.py` | **The Updater.** Runs an infinite loop to retrain the Logistic Regression model every 5 minutes using combined historical + live data. |
| `dashboard.py` | **The UI.** A Streamlit application rendering real-time maps (PyDeck), metrics, and status tables. |
| `fetch_historical_data.py`| **ETL Utility.** Fetches the initial 1-year baseline dataset and calculates flood thresholds. |
| `docker-compose.yml` | **Infrastructure.** Defines the Kafka and Zookeeper services. |

---

## ⚡ Installation & Usage

### Prerequisites
* Python 3.8+
* Docker & Docker Compose
* Java 11 (for Spark/Kafka)

### To Run the Project run the commands below step by step:

### Step 1: Start Infrastructure
Initialize the Kafka message broker.
```
docker-compose up -d
```
### Step 2: Start Data Producer
Begin fetching live environmental data
```
python3 producer.py
```
### Step 3: Start Spark Engine
In a new terminal, launch the streaming processor
```
python3 spark_processor.py
```
### Step 4: Start Re-Training the Model
In a new terminal, activate the continuous learning
```
python3 train_model.py
```
### Step 5: Launch Dashboard
In a new terminal, open the visualization interface
```
streamlit run dashboard.py
```
### Screen Shot for the Terminal Files Running Synchronously:

<img width="1132" height="759" alt="image" src="https://github.com/user-attachments/assets/aed73ab8-6b91-427c-a4fd-363b80416f5e" />

### Some Screen Shots for the UI:

1. The Main UI Page:
   
<img width="1364" height="660" alt="image" src="https://github.com/user-attachments/assets/d88f85cb-d4ee-4ad1-a822-3e08fa1e1890" />

2. Example Danger Decision For a Specific City(River):
   
<img width="1413" height="741" alt="image" src="https://github.com/user-attachments/assets/762ab7a5-1cfd-45ac-9e46-64d9020036d0" />

3. Example Safe Decision For a Specific City(River):
   
<img width="1412" height="734" alt="image" src="https://github.com/user-attachments/assets/84d0c0a6-8085-43af-963a-96f7fbbacc28" />





