/*
  Staging Model: stg_churn_subscribers
  ======================================
  Cleans and standardises the raw subscriber churn table.
  Single source of truth for all downstream churn models.

  Source: churn_dw.subscriber_churn_analysis
  Author: Lawrence Koomson
*/

with source as (

    select * from {{ source('churn_dw', 'subscriber_churn_analysis') }}

),

staged as (

    select
        subscriber_id,
        msisdn,
        upper(trim(region))                         as region,
        initcap(trim(plan_type))                    as plan_type,
        join_date,
        last_active_date,
        tenure_months,
        days_since_active,

        monthly_spend_ghs,
        avg_monthly_spend,
        data_usage_gb,
        voice_minutes,
        sms_count,
        num_complaints,
        network_drop_rate_pct,

        roaming_enabled,
        momo_linked,

        engagement_score,
        churn_risk_score,
        upper(churn_risk_tier)                      as churn_risk_tier,
        revenue_at_risk_ghs,
        retention_action,
        is_churned,
        processed_at,

        case
            when days_since_active <= 30  then 'Active'
            when days_since_active <= 90  then 'At Risk'
            when days_since_active <= 180 then 'Dormant'
            else 'Inactive'
        end                                         as activity_status,

        case
            when tenure_months < 6   then 'New (0-6 months)'
            when tenure_months < 12  then 'Growing (6-12 months)'
            when tenure_months < 24  then 'Established (1-2 years)'
            else 'Loyal (2+ years)'
        end                                         as tenure_segment

    from source
    where subscriber_id is not null

)

select * from staged