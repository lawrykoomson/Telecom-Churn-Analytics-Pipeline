/*
  Mart Model: mart_churn_by_plan
  ================================
  Aggregates churn metrics by subscription plan type.
  Identifies which plans have highest churn and revenue risk.

  Author: Lawrence Koomson
*/

with staged as (

    select * from {{ ref('stg_churn_subscribers') }}

),

plan_churn as (

    select
        plan_type,

        count(subscriber_id)                            as total_subscribers,
        count(case when is_churned = 1 then 1 end)      as churned_subscribers,

        round(
            count(case when is_churned = 1 then 1 end)::numeric
            / nullif(count(subscriber_id), 0) * 100
        , 2)                                            as churn_rate_pct,

        round(avg(churn_risk_score), 2)                 as avg_risk_score,
        round(avg(engagement_score), 2)                 as avg_engagement_score,
        round(avg(monthly_spend_ghs), 2)                as avg_monthly_spend_ghs,
        round(sum(revenue_at_risk_ghs), 2)              as total_revenue_at_risk_ghs,
        round(avg(tenure_months), 1)                    as avg_tenure_months,
        round(avg(days_since_active), 1)                as avg_days_since_active,
        round(avg(num_complaints), 2)                   as avg_complaints,

        count(case when churn_risk_tier = 'CRITICAL'
                   then 1 end)                          as critical_subscribers,

        round(
            sum(revenue_at_risk_ghs)
            / sum(sum(revenue_at_risk_ghs)) over () * 100
        , 2)                                            as revenue_risk_share_pct

    from staged
    group by plan_type

)

select * from plan_churn
order by total_revenue_at_risk_ghs desc