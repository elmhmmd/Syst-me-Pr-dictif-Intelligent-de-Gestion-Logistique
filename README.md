# Système Prédictif Intelligent de Gestion Logistique

A real-time machine learning pipeline for predicting delivery risks in logistics operations using Apache Spark ML, with streaming data processing and interactive visualization.

## Overview

This intelligent logistics management system:
- **Predicts delivery delays** using Random Forest classification
- **Processes real-time supply chain data** through a streaming pipeline
- **Provides interactive dashboards** for monitoring and analysis
- **Stores results** in PostgreSQL and MongoDB
- **Orchestrates workflows** using Apache Airflow

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────┐
│  Data Producer  │────▶│  Socket Bridge  │────▶│  Spark Streaming    │
│    (FastAPI)    │     │ (WebSocket→TCP) │     │    + ML Model       │
└─────────────────┘     └─────────────────┘     └──────────┬──────────┘
                                                           │
                                               ┌───────────┴───────────┐
                                               ▼                       ▼
                                        ┌────────────┐          ┌────────────┐
                                        │ PostgreSQL │          │  MongoDB   │
                                        │(predictions)│         │ (stats)    │
                                        └─────┬──────┘          └──────┬─────┘
                                              │                        │
                                              └──────────┬─────────────┘
                                                         ▼
                                               ┌─────────────────┐
                                               │    Streamlit    │
                                               │   Dashboard     │
                                               └─────────────────┘
```

## Tech Stack

| Category | Technologies |
|----------|-------------|
| **Big Data & ML** | Apache Spark 4.0.1, PySpark ML, scikit-learn |
| **Web Framework** | FastAPI, Streamlit, Uvicorn |
| **Databases** | PostgreSQL 13, MongoDB 5.0 |
| **Orchestration** | Apache Airflow |
| **Real-time** | WebSockets, TCP Sockets |

## Project Structure

```
├── interface.py              # Streamlit prediction interface
├── dashboard.py              # Real-time monitoring dashboard
├── data_server.py            # FastAPI data producer
├── socket_bridge.py          # WebSocket ↔ TCP bridge
├── spark_streaming_job.py    # Spark streaming with ML predictions
├── airflow_dag.py            # Airflow DAG orchestration
├── check_pipeline.py         # Health check script
├── start_pipeline.sh         # Start all services
├── stop_pipeline.sh          # Stop all services
├── docker-compose.yml        # PostgreSQL + MongoDB containers
├── delivery_risk_model/      # Trained Spark ML model
├── Gestion_Logistique_Notebook.ipynb  # Model training notebook
└── DataCoSupplyChainDataset.csv       # Training dataset (180K+ orders)
```

## Machine Learning Model

### Algorithm
- **Type:** Binary Classification (on-time vs late delivery)
- **Model:** Random Forest Classifier
- **Optimization:** 5-Fold Cross Validation with hyperparameter tuning

### Features
| Feature | Description |
|---------|-------------|
| Days for shipment (scheduled) | Expected delivery timeline (0-4 days) |
| Category Id | Product category (1-76) |
| Order Item Quantity | Number of items (1-5) |
| Shipping Mode | Delivery method (First Class, Second Class, Same Day, Standard Class) |
| Distance | Haversine distance between origin and destination |

### Performance
- **ROC-AUC:** ~0.76
- **Accuracy:** ~71.5%

## Getting Started

### Prerequisites
- Python 3.12+
- Apache Spark 4.0.1
- Docker & Docker Compose
- Java (for Spark)

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd Syst-me-Pr-dictif-Intelligent-de-Gestion-Logistique
```

2. **Create and activate virtual environment**
```bash
python -m venv Gestion_Logistique
source Gestion_Logistique/bin/activate
```

3. **Install dependencies**
```bash
pip install pyspark fastapi uvicorn streamlit websockets pymongo psycopg2-binary pandas seaborn matplotlib altair requests
```

4. **Start databases**
```bash
docker-compose up -d
```

### Running the Pipeline

**Option 1: Using shell script**
```bash
./start_pipeline.sh
```

**Option 2: Manual startup**
```bash
# Terminal 1: Start FastAPI data producer
python data_server.py

# Terminal 2: Start socket bridge
python socket_bridge.py

# Terminal 3: Start Spark streaming job
spark-submit spark_streaming_job.py

# Terminal 4: Start dashboard
streamlit run dashboard.py
```

**Option 3: Using Airflow**
```bash
# Trigger DAG: logistics_realtime_pipeline
```

### Accessing Services

| Service | URL |
|---------|-----|
| Prediction Interface | http://localhost:8501 |
| FastAPI Docs | http://localhost:8000/docs |
| Real-time Dashboard | http://localhost:8501 |

### Health Check
```bash
python check_pipeline.py
```

### Stopping Services
```bash
./stop_pipeline.sh
# Or: docker-compose down
```

## Service Ports

| Service | Port |
|---------|------|
| FastAPI | 8000 |
| Socket Bridge (TCP) | 9999 |
| Streamlit | 8501 |
| PostgreSQL | 5432 |
| MongoDB | 27017 |

## Data Pipeline Flow

1. **Data Producer** generates random orders at 1 order/second
2. **Socket Bridge** forwards WebSocket data to TCP for Spark compatibility
3. **Spark Streaming** processes 5-second micro-batches with ML model
4. **Predictions** are written to PostgreSQL (individual) and MongoDB (aggregated)
5. **Dashboard** displays real-time KPIs and visualizations

## Dashboards

### Prediction Interface (`interface.py`)
- Single order risk prediction form
- Interactive parameter input
- Confidence score display

### Monitoring Dashboard (`dashboard.py`)
- KPIs: total orders, average risk, last update
- Charts: risk by shipping mode, order volume vs distance
- Auto-refresh every 5 seconds

## Dataset

The project uses the **DataCo Supply Chain Dataset**:
- **Records:** 180,519 orders
- **Period:** 2015-2017
- **Markets:** Global (Africa, Europe, LATAM, Pacific Asia, USCA)
- **Features:** 55+ attributes

## Model Training

The model training pipeline is documented in `Gestion_Logistique_Notebook.ipynb`:

1. Data loading and exploration
2. Duplicate column detection and removal
3. Feature engineering (geocoding, distance calculation)
4. Outlier detection using IQR method
5. Pipeline construction with VectorAssembler + RandomForestClassifier
6. Cross-validation with hyperparameter tuning
7. Model evaluation and saving

## Database Schema

### PostgreSQL - `predictions_realtime`
Stores individual prediction results with features and risk scores.

### MongoDB - `aggregated_stats`
Stores aggregated statistics grouped by shipping mode.
