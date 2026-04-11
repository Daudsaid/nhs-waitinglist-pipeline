with source as (
    select * from {{ source('raw', 'nhs_ae_attendances') }}
),

staged as (
    select
        period,
        org_code,
        trim(parent_org)        as parent_org,
        trim(org_name)          as org_name,
        ae_attendances_type1,
        ae_attendances_type2,
        ae_attendances_other,
        over_4hrs_type1,
        over_4hrs_type2,
        over_4hrs_other,
        wait_4_12hrs_dta,
        wait_12plus_hrs_dta,
        emergency_admissions_type1,
        emergency_admissions_type2,
        emergency_admissions_other,
        other_emergency_admissions,
        total_attendances,
        total_over_4hrs,
        breach_rate_pct,
        total_emergency_admissions
    from source
    where org_code is not null
    and lower(trim(org_name)) not like 'total%'
)

select * from staged
