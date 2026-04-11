with trust_data as (
    select * from {{ ref('int_nhs_ae_by_trust') }}
)

select
    org_code,
    org_name,
    parent_org,
    months_reported,
    total_attendances,
    total_over_4hrs,
    total_wait_4_12hrs,
    total_wait_12plus_hrs,
    total_emergency_admissions,
    avg_breach_rate_pct,
    overall_breach_rate_pct,
    case
        when overall_breach_rate_pct >= 40 then 'CRITICAL'
        when overall_breach_rate_pct >= 25 then 'HIGH'
        when overall_breach_rate_pct >= 10 then 'MEDIUM'
        else 'LOW'
    end as performance_band
from trust_data
order by overall_breach_rate_pct desc nulls last
