# 📡 Telecom Customer Churn Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-336791?style=flat-square&logo=postgresql)
![dbt](https://img.shields.io/badge/dbt-1.11-FF694B?style=flat-square&logo=dbt)
![Tests](https://img.shields.io/badge/Tests-26%2F26%20Passing-brightgreen?style=flat-square)
![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen?style=flat-square)

A production-grade data engineering pipeline that processes **15,000 MTN Ghana subscriber records**, engineers churn risk features, scores every subscriber's likelihood to leave, and loads results into a live **PostgreSQL** warehouse — with a full **dbt analytical layer**, **Airflow DAG**, and **Kafka stream simulator**.

Built to mirror real subscriber analytics workflows used by **MTN Ghana**.

---

## 🏗️ System Architecture

```
[Subscriber CRM Data Source]
           │
           ▼
     ┌───────────┐
     │  EXTRACT  │  ← Generates 15,000 synthetic MTN Ghana subscriber records
     └───────────┘
           │
           ▼
     ┌───────────┐
     │ TRANSFORM │  ← RFM-style churn feature engineering + risk scoring
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   LOAD    │  ← PostgreSQL warehouse (churn_dw schema)
     └───────────┘
           │
           ▼
     ┌───────────┐
     │    dbt    │  ← Analytical layer: 1 staging view + 3 mart tables
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   Kafka   │  ← Real-time churn event stream: Producer + 3 Consumers
     └───────────┘
```

---

## ✅ What The Pipeline Does

### Extract
- Generates 15,000 realistic MTN Ghana subscriber records
- Fields include: region, plan type, usage data, complaints, network drop rate
- Covers all 7 Ghana regions with realistic distribution

### Transform — Churn Feature Engineering
Every subscriber receives 6 engineered features:

| Feature | Description |
|---|---|
| `tenure_months` | How long on the network |
| `days_since_active` | Recency of last activity |
| `avg_monthly_spend` | Spend normalised by tenure |
| `engagement_score` | Composite of data, voice, SMS, recency (0–100) |
| `churn_risk_score` | Weighted risk model (0–100) |
| `revenue_at_risk_ghs` | Monthly spend × churn probability |

### Churn Risk Model
| Factor | Weight |
|---|---|
| Days since last activity | 35% |
| Number of complaints | 25% |
| Network drop rate | 20% |
| Engagement score | 20% |

### Risk Tiers
| Score | Tier | Action |
|---|---|---|
| 76–100 | Critical | Immediate outreach — personalised retention bundle |
| 51–75 | High | Targeted discount offer within 48 hours |
| 26–50 | Medium | Enroll in loyalty programme |
| 0–25 | Low | Standard engagement — monitor monthly |

### Load
- Batch upserts into PostgreSQL (churn_dw schema)
- Auto-falls back to CSV if database unavailable

---

## 🔁 dbt Analytical Layer

4 models built on top of PostgreSQL:

| Model | Type | Description |
|---|---|---|
| stg_churn_subscribers | View | Cleaned subscribers + activity/tenure segments |
| mart_churn_by_region | Table | Churn metrics per Ghana region |
| mart_churn_by_plan | Table | Churn metrics per subscription plan |
| mart_critical_subscribers | Table | 13,488 High/Critical risk subscribers |

```bash
cd dbt
dbt run --profiles-dir .    # Run all 4 models
dbt test --profiles-dir .   # Run 4 data quality tests
```

---

## 🌊 Kafka Stream Simulator

Real-time churn risk event streaming:

```bash
python kafka_churn_simulator.py
```

```
Topic          : telecom.churn.risk.events
Partitions     : 3
Producer Rate  : 10 events/sec
Duration       : 60 seconds

Producer        → generates live subscriber churn risk events
RiskConsumer    → flags Critical and High tier subscribers (partition 0)
MetricsConsumer → aggregates real-time churn KPIs (partition 1)
AlertConsumer   → logs all events to JSONL file (partition 2)

Final Results:
  Total Events Produced  : 595
  Critical Risk Detected : 3
  High Risk Detected     : 134
  Churn Rate Detected    : 19.7%
  Revenue at Risk        : GHS 4,569.39
  Retention Alerts       : 126
  Top Risk Region        : Greater Accra
  Top Risk Plan          : Daily Bundle
```

---

## 🧪 Unit Tests — 26/26 Passing

```bash
pytest test_churn_pipeline.py -v
# 26 passed in 28.50s
```

| Test Class | Tests | Coverage |
|---|---|---|
| TestExtract | 8 | Row count, columns, uniqueness, valid values |
| TestTransform | 13 | Score ranges, tiers, revenue, retention actions |
| TestIntegration | 5 | End-to-end, distribution, churn rate realism |

---

## 📋 Airflow DAG

Scheduled pipeline at `dags/churn_pipeline_dag.py`:
- Runs **every Sunday at 07:00 AM UTC** (weekly subscriber re-scoring)
- 5 tasks: extract, transform, load, dbt refresh, notify
- XCom passes metrics between tasks
- Email alerts on failure with 2 retries

---

## 📊 Sample Pipeline Output

```
=================================================================
   TELECOM CHURN ANALYTICS PIPELINE — RUN SUMMARY
=================================================================
  Total Subscribers Analysed  : 15,000
  Actual Churn Rate           : 18.3%
  Critical Risk Subscribers   : 390
  Total Revenue at Risk       : GHS 406,424.56
-----------------------------------------------------------------
  CHURN RISK TIER BREAKDOWN:
    Critical   : 390   subscribers (2.6%)
    High       : 13,098 subscribers (87.3%)
    Medium     : 1,512  subscribers (10.1%)
    Low        : 0      subscribers (0.0%)
-----------------------------------------------------------------
  REVENUE AT RISK BY REGION:
    Greater Accra        : GHS 121,942.91
    Ashanti              : GHS 104,089.98
    Western              : GHS  60,064.64
    Eastern              : GHS  48,214.89
    Northern             : GHS  31,901.82
    Volta                : GHS  24,222.41
    Brong-Ahafo          : GHS  15,987.91
=================================================================
```

---

## 🚀 How To Run

```bash
# 1. Clone the repo
git clone https://github.com/lawrykoomson/Telecom-Churn-Analytics-Pipeline.git
cd Telecom-Churn-Analytics-Pipeline

# 2. Create virtual environment with Python 3.11
py -3.11 -m venv venv
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create PostgreSQL database
psql -U postgres -c "CREATE DATABASE telecom_analytics;"

# 5. Configure environment
copy .env.example .env
# Edit .env with your PostgreSQL credentials

# 6. Run the pipeline
python churn_pipeline.py

# 7. Run unit tests
pytest test_churn_pipeline.py -v

# 8. Run dbt models
cd dbt
set DBT_POSTGRES_PASSWORD=your_password
dbt run --profiles-dir .
dbt test --profiles-dir .

# 9. Run Kafka stream simulator
cd ..
python kafka_churn_simulator.py
```

---

## 📦 Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core pipeline language |
| Pandas | Data transformation and feature engineering |
| NumPy | Numerical operations |
| psycopg2 | PostgreSQL database connector |
| dbt-postgres | Analytical transformation layer |
| Apache Airflow | Pipeline orchestration DAG |
| pytest | Unit testing framework |
| python-dotenv | Environment variable management |

---

## 🔮 Roadmap

- [x] ETL pipeline with PostgreSQL live load
- [x] 26 unit tests — all passing
- [x] dbt analytical layer — 4 models, 4 tests passing
- [x] Apache Airflow DAG — weekly scheduled runs
- [x] Kafka stream simulator — 3 consumer groups
- [x] Power BI dashboard connected to PostgreSQL
- [ ] Docker containerisation

---

## 👨‍💻 Author

**Lawrence Koomson**
BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
🔗 [LinkedIn](https://linkedin.com/in/lawrykoomson) | [GitHub](https://github.com/lawrykoomson)
