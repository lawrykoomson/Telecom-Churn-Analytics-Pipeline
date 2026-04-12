/*
  Mart Model: mart_churn_by_region
  ==================================
  Aggregates churn metrics by Ghana region.
  Powers regional churn analysis in Power BI.

  Author: Lawrence Koomson
*/

with staged as (

    select * from {{ ref('stg_churn_subscribers') }}

),

region_churn as (

    select
        region,

        count(subscriber_id)                            as total_subscribers,
        count(case when is_churned = 1 then 1 end)      as churned_subscribers,
        count(case when is_churned = 0 then 1 end)      as active_subscribers,

        round(
            count(case when is_churned = 1 then 1 end)::numeric
            / nullif(count(subscriber_id), 0) * 100
        , 2)                                            as churn_rate_pct,

        round(avg(churn_risk_score), 2)                 as avg_risk_score,
        round(avg(engagement_score), 2)                 as avg_engagement_score,
        round(sum(revenue_at_risk_ghs), 2)              as total_revenue_at_risk_ghs,
        round(avg(monthly_spend_ghs), 2)                as avg_monthly_spend_ghs,

        count(case when churn_risk_tier = 'CRITICAL'
                   then 1 end)                          as critical_count,
        count(case when churn_risk_tier = 'HIGH'
                   then 1 end)                          as high_count,
        count(case when churn_risk_tier = 'MEDIUM'
                   then 1 end)                          as medium_count,
        count(case when churn_risk_tier = 'LOW'
                   then 1 end)                          as low_count,

        round(
            sum(revenue_at_risk_ghs)
            / sum(sum(revenue_at_risk_ghs)) over () * 100
        , 2)                                            as revenue_risk_share_pct

    from staged
    group by region

)

select * from region_churn
order by total_revenue_at_risk_ghs desc