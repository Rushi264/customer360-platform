# Customer 360 Data Platform 🚀

A production-grade, enterprise-level data engineering platform demonstrating modern data architecture patterns including Data Vault 2.0, ETL orchestration, REST APIs, and real-time streaming.

**Status:** ✅ Complete & Fully Functional

---

## 📚 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Documentation](#api-documentation)
- [Data Pipeline](#data-pipeline)
- [Real-Time Streaming](#real-time-streaming)
- [Performance Metrics](#performance-metrics)
- [Future Enhancements](#future-enhancements)
- [Author](#author)

---

## 🎯 Overview

Customer 360 Data Platform is an end-to-end data solution that demonstrates:

✅ **Enterprise Data Warehouse** - Data Vault 2.0 schema with 435,000+ records  
✅ **Automated ETL** - Apache Airflow DAGs with quality checks  
✅ **REST APIs** - FastAPI with 5 production-ready endpoints  
✅ **Real-Time Streaming** - Kafka event processing pipeline  
✅ **Complete Orchestration** - 8 Docker services working seamlessly  

### **Use Case**

This system processes customer, product, and transaction data to provide:
- Real-time customer 360 profiles
- Product performance analytics
- Order history and metrics
- Live event stream processing
- Business intelligence ready data

---

## 🏗️ Architecture

### **5-Layer Architecture**

A complete data platform with:
- **Data Sources:** CSV files (10K customers, 5K products, 100K transactions)
- **Ingestion Layer:** Apache Airflow with automated nightly execution
- **Data Warehouse:** PostgreSQL with Data Vault 2.0 schema (435K+ records)
- **Business Layer:** Denormalized SQL views for BI tools
- **Consumption Layer:** REST APIs, Kafka streaming, dashboards

---

## 💻 Technology Stack

- **Language:** Python 3.12
- **API Framework:** FastAPI
- **Database:** PostgreSQL 15
- **Orchestration:** Apache Airflow 2.x
- **Real-Time:** Apache Kafka
- **Infrastructure:** Docker & Docker Compose (8 services)
- **Data Pattern:** Data Vault 2.0 with SCD Type 2

---

## ⭐ Key Features

### **1. Enterprise Data Warehouse**
- Data Vault 2.0 architecture with 435,000+ records
- Full history tracking and audit trail
- 3 Hubs, 3 Satellites, 2 Links

### **2. Automated ETL Pipeline**
- Daily execution with quality validation
- Retry logic (3x with exponential backoff)
- SLA monitoring (15-minute target)
- Pre and post-load data quality checks

### **3. REST API Service**
- 5 production endpoints with Swagger docs
- GET /customers, /products, /orders
- Pagination and error handling

### **4. Real-Time Streaming**
- Kafka event processing
- Real-time metrics aggregation
- Stream consumer processing
- Handles 1000+ events/second

---

## 📁 Project Structure
```
customer360-platform/
├── api/
│   └── main.py                    # FastAPI (5 endpoints)
├── dags/
│   ├── customer_data_pipeline.py  # Basic DAG
│   └── customer_data_pipeline_enhanced.py  # Production DAG
├── streaming/
│   ├── producer.py                # Kafka producer
│   └── consumer.py                # Stream processor
├── sql/                           # Database scripts
├── data/                          # CSV files
├── docker-compose.yml
└── README.md
```

---

## 🚀 Installation

### **Prerequisites**
- Docker & Docker Compose
- Python 3.8+
- Git
- 4GB RAM, 10GB disk

### **Quick Start**
```bash
# Clone repository
git clone https://github.com/Rushi264/customer360-platform.git
cd customer360-platform

# Start services
docker-compose up -d

# Initialize database
docker exec -it customer360-postgres psql -U dataeng -d customer360 < sql/01_create_schemas.sql
# ... run other SQL scripts

# Access services
# Airflow: http://localhost:8080
# API: http://localhost:8000/docs
```

---

## ⚡ Quick Start

### **1. Run Data Pipeline**
```bash
# Visit http://localhost:8080
# Click Airflow DAG and trigger execution
```

### **2. Test APIs**
```bash
# Visit http://localhost:8000/docs for interactive docs
curl http://localhost:8000/customers?limit=5
```

### **3. Run Streaming**
```bash
# Terminal 1
python streaming/consumer.py

# Terminal 2
python streaming/producer.py
```

---

## 📈 Performance Metrics
```
Input Data:
  • Customers: 10,000
  • Products: 5,000
  • Transactions: 100,000

Data Warehouse:
  • Total Records: 435,000+
  • ETL Duration: 5-10 minutes
  • API Response: ~100ms average

Streaming:
  • Throughput: 1000+ events/second
  • Latency: <1 second
  • Insert Rate: 100+ inserts/minute
```

---

## 🔮 Future Enhancements

- JWT authentication
- Redis caching
- Rate limiting
- Data quality dashboard
- CI/CD pipeline
- Multi-tenant support
- Machine learning models
- Prometheus/Grafana monitoring

---

## 👤 Author

**Rushikesh Deshmukh**  
Data Engineer | 3+ years experience | Python, Data Engineering, System Design

---

## 📝 License

MIT License

---

**Last Updated:** January 21, 2026  
**Status:** ✅ Production Ready  
**Version:** 1.0.0

⭐ If this project helped you, please give it a star!
