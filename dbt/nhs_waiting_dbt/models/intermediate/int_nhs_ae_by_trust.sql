with staged as (
    select * from {{ ref('stg_nhs_ae_attendances') }}
),

aggregated as (
    select
        org_code,
        org_name,
        parent_org,
        count(distinct period)                          as months_reported,
        sum(total_attendances)                          as total_attendances,
        sum(total_over_4hrs)                            as total_over_4hrs,
        sum(wait_4_12hrs_dta)                           as total_wait_4_12hrs,
        sum(wait_12plus_hrs_dta)                        as total_wait_12plus_hrs,
        sum(total_emergency_admissions)                 as total_emergency_admissions,
        round(avg(breach_rate_pct), 2)                  as avg_breach_rate_pct,
        round(
            sum(total_over_4hrs) / nullif(sum(total_attendances), 0) * 100
        , 2)                                            as overall_breach_rate_pct
    from staged
    group by org_code, org_name, parent_org
)

select * from aggregated
