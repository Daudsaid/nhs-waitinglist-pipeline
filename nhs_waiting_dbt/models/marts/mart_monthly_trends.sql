with staged as (
    select * from {{ ref('stg_nhs_ae_attendances') }}
)

select
    period,
    count(distinct org_code)                            as trusts_reporting,
    sum(total_attendances)                              as total_attendances,
    sum(total_over_4hrs)                                as total_over_4hrs,
    sum(wait_4_12hrs_dta)                               as total_wait_4_12hrs,
    sum(wait_12plus_hrs_dta)                            as total_wait_12plus_hrs,
    sum(total_emergency_admissions)                     as total_emergency_admissions,
    round(
        sum(total_over_4hrs) / nullif(sum(total_attendances), 0) * 100
    , 2)                                                as national_breach_rate_pct
from staged
group by period
order by period
