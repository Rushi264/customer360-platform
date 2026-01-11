\# Customer 360 Data Platform



A production-grade data engineering platform built with Apache Airflow, Kafka, PostgreSQL, and Docker for processing customer data at scale.



\## 🎯 Project Overview



This project demonstrates end-to-end data engineering skills including:

\- \*\*Data ingestion\*\* from multiple sources (CSV, APIs, databases)

\- \*\*Data modeling\*\* using Data Vault 2.0 methodology

\- \*\*Workflow orchestration\*\* with Apache Airflow

\- \*\*Real-time streaming\*\* with Apache Kafka

\- \*\*Machine learning\*\* pipelines for customer analytics

\- \*\*RESTful APIs\*\* with FastAPI



\## 🏗️ Architecture

```

┌─────────────────────────────────────────────────────────┐

│  Data Sources → Airflow → PostgreSQL → Analytics/ML    │

│                    ↓                                     │

│                  Kafka (Real-time)                      │

└─────────────────────────────────────────────────────────┘

```



\## 🛠️ Tech Stack



\- \*\*Orchestration\*\*: Apache Airflow 2.8.1

\- \*\*Database\*\*: PostgreSQL 15

\- \*\*Streaming\*\*: Apache Kafka 7.5.0

\- \*\*Storage\*\*: MinIO (S3-compatible)

\- \*\*Caching\*\*: Redis 7

\- \*\*ML Tracking\*\*: MLflow

\- \*\*Container Platform\*\*: Docker \& Docker Compose

\- \*\*Languages\*\*: Python 3.11, SQL



\## 📊 Data



\- \*\*Customers\*\*: 10,000 records

\- \*\*Products\*\*: 5,000 records

\- \*\*Transactions\*\*: 100,000 records

\- \*\*Total Dataset\*\*: 115,000+ records



\## 🚀 Getting Started



\### Prerequisites



\- Docker Desktop

\- Python 3.9+

\- Git



\### Installation



1\. \*\*Clone the repository\*\*

```bash

git clone https://github.com/YOUR\_USERNAME/customer360-platform.git

cd customer360-platform

```



2\. \*\*Create Python virtual environment\*\*

```bash

python -m venv venv

source venv/bin/activate  # On Windows: venv\\Scripts\\activate

pip install -r requirements.txt

```



3\. \*\*Generate sample data\*\*

```bash

cd data\_generator

python generate\_all\_data.py

cd ..

```



4\. \*\*Start all services\*\*

```bash

docker-compose up -d

```



5\. \*\*Wait 3-5 minutes for services to initialize\*\*



6\. \*\*Load data into PostgreSQL\*\*

```bash

python load\_data\_to\_postgres.py

```



\### Access Services



| Service | URL | Credentials |

|---------|-----|-------------|

| \*\*Airflow\*\* | http://localhost:8080 | admin / admin123 |

| \*\*MinIO\*\* | http://localhost:9001 | minioadmin / minioadmin123 |

| \*\*MLflow\*\* | http://localhost:5000 | No login required |



\## 📁 Project Structure

```

customer360-platform/

├── data/                    # Generated sample data

├── data\_generator/          # Scripts to generate test data

├── dags/                    # Airflow DAG definitions

├── sql/                     # Database schemas and transformations

├── etl\_framework/           # Metadata-driven ETL framework

├── streaming/               # Kafka producers/consumers

├── ml/                      # Machine learning models

├── notebooks/               # Jupyter notebooks for analysis

├── tests/                   # Unit and integration tests

├── docker-compose.yml       # Docker services configuration

├── requirements.txt         # Python dependencies

└── README.md               # This file

```



\## 🔄 Data Pipeline



1\. \*\*Ingestion\*\*: CSV files → PostgreSQL staging tables

2\. \*\*Transformation\*\*: Staging → Data Vault (Hubs, Links, Satellites)

3\. \*\*Aggregation\*\*: Data Vault → Business views

4\. \*\*Analytics\*\*: Business views → ML models \& dashboards



\## 🎓 Key Features



\### Data Vault 2.0 Modeling

\- \*\*Hubs\*\*: Business keys (customers, products, orders)

\- \*\*Links\*\*: Relationships between entities

\- \*\*Satellites\*\*: Historical attribute tracking (SCD Type 2)



\### Metadata-Driven Framework

\- Configuration-based pipeline generation

\- Dynamic schema evolution

\- Automated data lineage tracking



\### Real-time Processing

\- Kafka event streaming

\- Clickstream analytics

\- Real-time aggregations



\### Machine Learning

\- Customer segmentation (K-means)

\- Churn prediction (Random Forest)

\- Experiment tracking with MLflow



\## 📈 Roadmap



\- \[x] Infrastructure setup (Week 1)

\- \[ ] Airflow DAGs implementation (Week 2)

\- \[ ] Data Vault modeling (Week 2)

\- \[ ] FastAPI REST service (Week 3)

\- \[ ] Kafka streaming pipeline (Week 3)

\- \[ ] ML pipeline development (Week 4)

\- \[ ] Dashboard with Streamlit (Week 5)



\## 🤝 Contributing



This is a learning project, but suggestions and improvements are welcome!



\## 📝 License



This project is open source and available under the MIT License.



\## 👤 Author



\*\*Your Name\*\*

\- GitHub: \[@YOUR\_USERNAME](https://github.com/YOUR\_USERNAME)

\- LinkedIn: \[Your Profile](https://linkedin.com/in/YOUR\_PROFILE)



\## 🙏 Acknowledgments



\- Inspired by enterprise data platforms at Amazon, Netflix, and Uber

\- Built as a portfolio project to demonstrate data engineering skills



---



\*\*⭐ If you find this project helpful, please give it a star!\*\*

