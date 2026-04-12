/*
  Mart Model: mart_critical_subscribers
  =======================================
  Surfaces all Critical and High risk subscribers
  for immediate retention team action.

  Grain: One row per at-risk subscriber
  Author: Lawrence Koomson
*/

with staged as (

    select * from {{ ref('stg_churn_subscribers') }}

),

at_risk as (

    select
        subscriber_id,
        msisdn,
        region,
        plan_type,
        tenure_segment,
        activity_status,

        tenure_months,
        days_since_active,
        monthly_spend_ghs,
        revenue_at_risk_ghs,

        engagement_score,
        churn_risk_score,
        churn_risk_tier,
        num_complaints,
        network_drop_rate_pct,

        roaming_enabled,
        momo_linked,
        is_churned,

        retention_action,
        processed_at,

        rank() over (
            order by churn_risk_score desc, revenue_at_risk_ghs desc
        )                                               as risk_rank

    from staged
    where churn_risk_tier in ('CRITICAL', 'HIGH')

)

select * from at_risk
order by risk_rank