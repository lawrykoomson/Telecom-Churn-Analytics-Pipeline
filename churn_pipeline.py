"""
Telecom Customer Churn Analytics Pipeline
==========================================
Ingests subscriber data, engineers churn risk features,
scores each subscriber, and loads into PostgreSQL.

Targets: MTN Ghana

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("churn_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "telecom_analytics"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

PROCESSED_PATH = Path("data/processed/")

REGIONS = [
    "Greater Accra", "Ashanti", "Western",
    "Eastern", "Northern", "Volta", "Brong-Ahafo"
]
PLANS = [
    "Daily Bundle", "Weekly Bundle",
    "Monthly Bundle", "Pay As You Go", "Enterprise"
]


def extract() -> pd.DataFrame:
    logger.info("[EXTRACT] Generating synthetic subscriber data...")
    np.random.seed(99)
    n = 15000

    join_dates = [
        datetime(2020, 1, 1) + timedelta(days=int(d))
        for d in np.random.randint(0, 1460, n)
    ]
    last_active = [
        d + timedelta(days=int(np.random.randint(0, 365)))
        for d in join_dates
    ]

    df = pd.DataFrame({
        "subscriber_id":         [f"SUB{str(i).zfill(7)}" for i in range(1, n+1)],
        "msisdn":                [f"024{''.join(np.random.choice(list('0123456789'), 7).tolist())}" for _ in range(n)],
        "region":                np.random.choice(REGIONS, n, p=[0.30,0.25,0.15,0.12,0.08,0.06,0.04]),
        "plan_type":             np.random.choice(PLANS,   n, p=[0.30,0.25,0.20,0.15,0.10]),
        "join_date":             [d.date() for d in join_dates],
        "last_active_date":      [d.date() for d in last_active],
        "monthly_spend_ghs":     np.abs(np.random.normal(45, 30, n)).round(2),
        "data_usage_gb":         np.abs(np.random.normal(3.5, 2.5, n)).round(3),
        "voice_minutes":         np.abs(np.random.normal(120, 80, n)).round(1),
        "sms_count":             np.abs(np.random.normal(40, 30, n)).astype(int),
        "num_complaints":        np.random.choice([0,1,2,3,4,5], n, p=[0.60,0.20,0.10,0.06,0.03,0.01]),
        "network_drop_rate_pct": np.abs(np.random.normal(2.5, 2.0, n)).round(2),
        "roaming_enabled":       np.random.choice([True, False], n, p=[0.15, 0.85]),
        "momo_linked":           np.random.choice([True, False], n, p=[0.70, 0.30]),
        "is_churned":            np.random.choice([0, 1], n, p=[0.82, 0.18]),
    })

    logger.info(f"[EXTRACT] Generated {len(df):,} subscriber records.")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] Engineering churn features...")
    today = datetime.today().date()

    df["last_active_date"] = pd.to_datetime(df["last_active_date"]).dt.date
    df["days_since_active"] = df["last_active_date"].apply(
        lambda x: (today - x).days
    )

    df["join_date"] = pd.to_datetime(df["join_date"]).dt.date
    df["tenure_months"] = df["join_date"].apply(
        lambda x: max(1, round((today - x).days / 30))
    )

    df["avg_monthly_spend"] = (
        df["monthly_spend_ghs"] / df["tenure_months"]
    ).round(2)

    df["engagement_score"] = (
        (df["data_usage_gb"].clip(0, 10) / 10 * 30) +
        (df["voice_minutes"].clip(0, 300) / 300 * 30) +
        (df["sms_count"].clip(0, 100) / 100 * 20) +
        ((1 - df["days_since_active"].clip(0, 90) / 90) * 20)
    ).round(1)

    df["churn_risk_score"] = (
        (df["days_since_active"].clip(0, 90) / 90 * 35) +
        (df["num_complaints"].clip(0, 5) / 5 * 25) +
        (df["network_drop_rate_pct"].clip(0, 10) / 10 * 20) +
        ((1 - df["engagement_score"] / 100) * 20)
    ).round(1)

    df["churn_risk_tier"] = pd.cut(
        df["churn_risk_score"],
        bins=[-1, 25, 50, 75, 100],
        labels=["Low", "Medium", "High", "Critical"]
    ).astype(str)

    df["revenue_at_risk_ghs"] = (
        df["monthly_spend_ghs"] * df["churn_risk_score"] / 100
    ).round(2)

    def retention_action(row):
        if row["churn_risk_tier"] == "Critical":
            return "Immediate outreach — offer personalised retention bundle"
        elif row["churn_risk_tier"] == "High":
            return "Send targeted discount offer within 48 hours"
        elif row["churn_risk_tier"] == "Medium":
            return "Enroll in loyalty programme"
        else:
            return "Standard engagement — monitor monthly"

    df["retention_action"] = df.apply(retention_action, axis=1)
    df["processed_at"] = datetime.now()

    logger.info("[TRANSFORM] Feature engineering complete.")
    logger.info(f"[TRANSFORM] Risk tiers: {df['churn_risk_tier'].value_counts().to_dict()}")
    return df


def load(df: pd.DataFrame):
    logger.info("[LOAD] Attempting PostgreSQL connection...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS churn_dw;
                CREATE TABLE IF NOT EXISTS churn_dw.subscriber_churn_analysis (
                    subscriber_id           VARCHAR(15) PRIMARY KEY,
                    msisdn                  VARCHAR(15),
                    region                  VARCHAR(50),
                    plan_type               VARCHAR(30),
                    join_date               DATE,
                    last_active_date        DATE,
                    tenure_months           INT,
                    days_since_active       INT,
                    monthly_spend_ghs       NUMERIC(10,2),
                    avg_monthly_spend       NUMERIC(10,2),
                    data_usage_gb           NUMERIC(8,3),
                    voice_minutes           NUMERIC(8,1),
                    sms_count               INT,
                    num_complaints          SMALLINT,
                    network_drop_rate_pct   NUMERIC(5,2),
                    roaming_enabled         BOOLEAN,
                    momo_linked             BOOLEAN,
                    engagement_score        NUMERIC(5,1),
                    churn_risk_score        NUMERIC(5,1),
                    churn_risk_tier         VARCHAR(10),
                    revenue_at_risk_ghs     NUMERIC(10,2),
                    retention_action        TEXT,
                    is_churned              SMALLINT,
                    processed_at            TIMESTAMP
                );
            """)
            conn.commit()

        load_cols = [
            "subscriber_id","msisdn","region","plan_type",
            "join_date","last_active_date","tenure_months",
            "days_since_active","monthly_spend_ghs","avg_monthly_spend",
            "data_usage_gb","voice_minutes","sms_count","num_complaints",
            "network_drop_rate_pct","roaming_enabled","momo_linked",
            "engagement_score","churn_risk_score","churn_risk_tier",
            "revenue_at_risk_ghs","retention_action","is_churned","processed_at"
        ]

        records = [tuple(r) for r in df[load_cols].itertuples(index=False)]

        with conn.cursor() as cur:
            execute_values(cur,
                f"""INSERT INTO churn_dw.subscriber_churn_analysis
                    ({','.join(load_cols)}) VALUES %s
                    ON CONFLICT (subscriber_id) DO UPDATE SET
                    churn_risk_score=EXCLUDED.churn_risk_score,
                    churn_risk_tier=EXCLUDED.churn_risk_tier,
                    revenue_at_risk_ghs=EXCLUDED.revenue_at_risk_ghs,
                    processed_at=EXCLUDED.processed_at""",
                records, page_size=500
            )
            conn.commit()

        conn.close()
        logger.info(f"[LOAD] Successfully loaded {len(df):,} records into PostgreSQL.")

    except Exception as e:
        logger.warning(f"[LOAD] PostgreSQL unavailable ({e})")
        logger.info("[LOAD] Falling back to CSV export...")
        fallback = PROCESSED_PATH / f"churn_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(fallback, index=False)
        logger.info(f"[LOAD] Saved to {fallback}")


def print_summary(df: pd.DataFrame):
    total_risk = df["revenue_at_risk_ghs"].sum()
    churn_rate = df["is_churned"].mean() * 100
    critical   = (df["churn_risk_tier"] == "Critical").sum()

    print("\n" + "="*65)
    print("   TELECOM CHURN ANALYTICS PIPELINE — RUN SUMMARY")
    print("="*65)
    print(f"  Total Subscribers Analysed  : {len(df):,}")
    print(f"  Actual Churn Rate           : {churn_rate:.1f}%")
    print(f"  Critical Risk Subscribers   : {critical:,}")
    print(f"  Total Revenue at Risk       : GHS {total_risk:,.2f}")
    print("-"*65)
    print("  CHURN RISK TIER BREAKDOWN:")
    tier_counts = df["churn_risk_tier"].value_counts()
    for tier in ["Critical", "High", "Medium", "Low"]:
        count = tier_counts.get(tier, 0)
        pct   = count / len(df) * 100
        print(f"    {tier:<10} : {count:,} subscribers ({pct:.1f}%)")
    print("-"*65)
    print("  REVENUE AT RISK BY REGION:")
    region_risk = (
        df.groupby("region")["revenue_at_risk_ghs"]
        .sum().sort_values(ascending=False)
    )
    for region, val in region_risk.items():
        print(f"    {region:<20} : GHS {val:,.2f}")
    print("="*65 + "\n")


def run_pipeline():
    logger.info("=" * 60)
    logger.info("  TELECOM CHURN ANALYTICS PIPELINE — STARTED")
    logger.info("=" * 60)
    start = datetime.now()
    df = extract()
    df = transform(df)
    load(df)
    print_summary(df)
    duration = (datetime.now() - start).total_seconds()
    logger.info(f"PIPELINE COMPLETED in {duration:.2f} seconds")


if __name__ == "__main__":
    run_pipeline()