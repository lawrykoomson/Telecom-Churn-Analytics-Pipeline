# 📡 Telecom Customer Churn Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.14-blue?style=flat-square&logo=python)
![Pandas](https://img.shields.io/badge/Pandas-3.0.2-150458?style=flat-square&logo=pandas)
![MySQL](https://img.shields.io/badge/MySQL-9.6-4479A1?style=flat-square&logo=mysql)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)

A production-grade data engineering pipeline that processes telecom subscriber data, engineers churn risk features, scores each subscriber's likelihood to leave, and loads results into MySQL — ready for Power BI dashboards and retention campaigns.

Built to mirror subscriber analytics workflows used by **MTN Ghana**.

---

## 🏗️ Pipeline Architecture
```
[CRM / Subscriber Data Source]
           │
           ▼
     ┌───────────┐
     │  EXTRACT  │  ← 15,000 subscriber records with usage data
     └───────────┘
           │
           ▼
     ┌───────────┐
     │ TRANSFORM │  ← Feature engineering + churn risk scoring
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   LOAD    │  ← MySQL analytics table or CSV fallback
     └───────────┘
           │
           ▼
     ┌───────────┐
     │  REPORT   │  ← Churn summary by region, plan, risk tier
     └───────────┘
```

---

## ✅ Features

### Feature Engineering
Every subscriber receives:
- **Tenure months** — how long they've been on the network
- **Days since active** — recency of engagement
- **Engagement score (0–100)** — composite of data, voice, SMS, recency
- **Churn risk score (0–100)** — weighted risk model
- **Risk tier** — Low / Medium / High / Critical
- **Revenue at risk (GHS)** — monthly spend × churn probability
- **Retention action** — automated recommendation per subscriber

### Churn Risk Model
| Factor | Weight |
|---|---|
| Days since last activity | 35% |
| Number of complaints | 25% |
| Network drop rate | 20% |
| Engagement score | 20% |

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
    Critical   : 390  subscribers (2.6%)
    High       : 13,098 subscribers (87.3%)
    Medium     : 1,512 subscribers (10.1%)
    Low        : 0 subscribers (0.0%)
-----------------------------------------------------------------
  REVENUE AT RISK BY REGION:
    Greater Accra        : GHS 121,942.91
    Ashanti              : GHS 104,089.98
    Western              : GHS  60,064.64
    Eastern              : GHS  48,214.89
    Northern             : GHS  31,901.82
    Volta                : GHS  24,222.41
    Brong-Ahafo          : GHS  15,987.91
-----------------------------------------------------------------
  CHURN RISK BY PLAN TYPE:
                  Subscribers  Avg_Risk_Score  Revenue_At_Risk
  Daily Bundle         4522           57.88        122,375.01
  Weekly Bundle        3774           57.80        101,790.75
  Monthly Bundle       2938           57.94         80,899.54
  Pay As You Go        2234           57.57         60,328.92
  Enterprise           1532           58.11         41,030.34
=================================================================
```

---

## 🚀 How To Run

### 1. Clone the repo
```bash
git clone https://github.com/lawrykoomson/Telecom-Churn-Analytics-Pipeline.git
cd Telecom-Churn-Analytics-Pipeline
```

### 2. Create virtual environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment
```bash
copy .env.example .env
# Edit .env with your MySQL credentials (optional)
```

### 5. Run the pipeline
```bash
python churn_pipeline.py
```
> No database? Results auto-save to `data/processed/` as CSV.

---

## 📦 Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.14 | Core pipeline language |
| Pandas | Data transformation and feature engineering |
| NumPy | Numerical operations |
| mysql-connector-python | MySQL database connector |
| python-dotenv | Environment variable management |

---

## 🔮 Future Improvements
- [ ] Replace rule-based scoring with XGBoost ML model
- [ ] Apache Airflow DAG for weekly scheduled runs
- [ ] Power BI dashboard connected to MySQL
- [ ] SMS/email alert trigger for Critical tier subscribers
- [ ] A/B test retention offer effectiveness tracking

---

## 👨‍💻 Author

**Lawrence Koomson**
BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
🔗 [LinkedIn](https://linkedin.com/in/lawrykoomson) | [GitHub](https://github.com/lawrykoomson)