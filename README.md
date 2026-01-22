# Customer 360 Data Platform 🚀

A production-grade, enterprise-level data engineering platform demonstrating modern data architecture patterns including Data Vault 2.0, ETL orchestration, REST APIs, dashboards, and real-time streaming.

**Status:** ✅ Complete & Fully Functional

---

## 📚 Table of Contents

- [Overview](#-overview)
- [Dashboards](#-dashboards)
- [Screenshots](#-screenshots)
- [Architecture](#️-architecture)
- [Technology Stack](#-technology-stack)
- [Key Features](#-key-features)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [API Documentation](#-api-documentation)

---

## 🎯 Overview

Customer 360 Data Platform is an end-to-end data solution that demonstrates:

✅ **Enterprise Data Warehouse** - Data Vault 2.0 schema with 435,000+ records
✅ **Automated ETL** - Apache Airflow DAGs with quality checks
✅ **REST APIs** - FastAPI with 5 production-ready endpoints
✅ **Professional Dashboards** - Metabase visualization layer
✅ **Real-Time Streaming** - Kafka event processing pipeline
✅ **Complete Orchestration** - 8 Docker services working seamlessly

---

## 📊 Dashboards

### Access Metabase

\\\ash
docker-compose up -d
open http://localhost:3000
\\\

### Customer 360 Overview Dashboard

**Metrics:**
- **Total Customers:** 10,000
- **Lifetime Revenue:** \,819,757,272.68
- **Average Order Value:** \.78
- **Total Orders:** 100,000

---

## 📸 Screenshots

### Login & Welcome Screen

![Metabase Login](docs/screenshots/Login_Page.png)

### Customer 360 Dashboard - Main View

![Customer 360 Dashboard](docs/screenshots/Dashbaord.png)

### Advanced Visualizations

![Dashboard Charts](docs/screenshots/Chart.png)

---

## 💻 Technology Stack

- **Backend:** Python 3.12, FastAPI
- **Database:** PostgreSQL 15
- **BI Tool:** Metabase
- **Orchestration:** Apache Airflow
- **Streaming:** Apache Kafka
- **Containerization:** Docker & Docker Compose

---

## 🚀 Installation

\\\ash
git clone https://github.com/Rushi264/customer360-platform.git
cd customer360-platform
docker-compose up -d
\\\

---

## ⚡ Quick Start

### Access Services

- **Dashboards:** http://localhost:3000
- **API Docs:** http://localhost:8000/docs
- **Airflow:** http://localhost:8080

### Test API

\\\ash
curl http://localhost:8000/customers?limit=5
curl http://localhost:8000/products?limit=5
\\\

---

## 📊 Project Stats

- **Lines of Code:** 2,000+
- **API Endpoints:** 5 production-ready
- **Data Records:** 435,000+
- **Docker Services:** 8
- **Database Tables:** 20+

---

**Last Updated:** January 22, 2026
**Status:** ✅ Production Ready

⭐ If this project helped you, please consider giving it a star!
