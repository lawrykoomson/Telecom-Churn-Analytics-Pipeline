"""
Unit Tests — Telecom Churn Analytics Pipeline
===============================================
Run with: pytest test_churn_pipeline.py -v

Author: Lawrence Koomson
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from churn_pipeline import extract, transform


class TestExtract:

    def test_returns_dataframe(self):
        df = extract()
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self):
        df = extract()
        assert len(df) == 15000

    def test_required_columns_present(self):
        df = extract()
        required = [
            "subscriber_id","msisdn","region","plan_type",
            "join_date","last_active_date","monthly_spend_ghs",
            "data_usage_gb","voice_minutes","sms_count",
            "num_complaints","network_drop_rate_pct",
            "roaming_enabled","momo_linked","is_churned"
        ]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_subscriber_ids_unique(self):
        df = extract()
        assert df["subscriber_id"].nunique() == len(df)

    def test_churn_column_binary(self):
        df = extract()
        assert set(df["is_churned"].unique()).issubset({0, 1})

    def test_monthly_spend_positive(self):
        df = extract()
        assert (df["monthly_spend_ghs"] >= 0).all()

    def test_regions_are_valid(self):
        df = extract()
        valid = {"Greater Accra","Ashanti","Western",
                 "Eastern","Northern","Volta","Brong-Ahafo"}
        assert set(df["region"].unique()).issubset(valid)

    def test_plans_are_valid(self):
        df = extract()
        valid = {"Daily Bundle","Weekly Bundle","Monthly Bundle",
                 "Pay As You Go","Enterprise"}
        assert set(df["plan_type"].unique()).issubset(valid)


class TestTransform:

    @pytest.fixture
    def transformed(self):
        df = extract()
        return transform(df)

    def test_churn_risk_score_range(self, transformed):
        assert transformed["churn_risk_score"].between(0, 100).all()

    def test_engagement_score_range(self, transformed):
        assert transformed["engagement_score"].between(0, 100).all()

    def test_risk_tiers_valid(self, transformed):
        valid = {"Low","Medium","High","Critical"}
        assert set(transformed["churn_risk_tier"].unique()).issubset(valid)

    def test_tenure_months_positive(self, transformed):
        assert (transformed["tenure_months"] >= 1).all()

    def test_days_since_active_non_negative(self, transformed):
        assert (transformed["days_since_active"] >= 0).all()

    def test_revenue_at_risk_non_negative(self, transformed):
        assert (transformed["revenue_at_risk_ghs"] >= 0).all()

    def test_retention_action_populated(self, transformed):
        assert transformed["retention_action"].isna().sum() == 0

    def test_avg_monthly_spend_calculated(self, transformed):
        assert "avg_monthly_spend" in transformed.columns
        assert (transformed["avg_monthly_spend"] >= 0).all()

    def test_processed_at_exists(self, transformed):
        assert "processed_at" in transformed.columns

    def test_no_null_risk_scores(self, transformed):
        assert transformed["churn_risk_score"].isna().sum() == 0

    def test_critical_higher_than_high(self, transformed):
        critical = transformed[transformed["churn_risk_tier"] == "Critical"]["churn_risk_score"]
        high     = transformed[transformed["churn_risk_tier"] == "High"]["churn_risk_score"]
        if len(critical) > 0 and len(high) > 0:
            assert critical.mean() > high.mean()

    def test_revenue_at_risk_not_exceed_spend(self, transformed):
        assert (transformed["revenue_at_risk_ghs"] <=
                transformed["monthly_spend_ghs"] + 0.01).all()

    def test_row_count_preserved(self, transformed):
        df = extract()
        assert len(transformed) == len(df)


class TestIntegration:

    def test_full_pipeline_runs(self):
        df     = extract()
        result = transform(df)
        assert len(result) == len(df)

    def test_segment_distribution_reasonable(self):
        df     = extract()
        result = transform(df)
        tiers  = result["churn_risk_tier"].value_counts(normalize=True)
        assert tiers.max() < 0.95

    def test_total_revenue_at_risk_positive(self):
        df     = extract()
        result = transform(df)
        assert result["revenue_at_risk_ghs"].sum() > 0

    def test_churn_rate_realistic(self):
        df     = extract()
        result = transform(df)
        churn_rate = result["is_churned"].mean()
        assert 0.05 < churn_rate < 0.40

    def test_no_duplicate_subscriber_ids(self):
        df     = extract()
        result = transform(df)
        assert result["subscriber_id"].duplicated().sum() == 0