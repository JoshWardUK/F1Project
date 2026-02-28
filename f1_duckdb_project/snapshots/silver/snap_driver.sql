{% snapshot snap_driver %}

{{
  config(
    target_schema = 'silver',
    unique_key = 'driver_id',
    strategy = 'check',
    check_cols = 'all'
  )
}}
with drivers as(
select
        driverId                             as driver_id,
        nullif(trim(givenname), '')          as given_name,
        nullif(trim(familyname), '')         as family_name,
        try_cast(dateofbirth as date)        as date_of_birth,
        nullif(trim(drivernationality), '')  as nationality,
        constructorId                        as constructor_id,
        nullif(trim(constructorname), '')    as constructor_name,
        nullif(trim(constructornationality), '') as constructor_nationality,
        row_number() over (
            partition by driverId
            order by season desc
        ) as rn
from {{ ref('bronze_drivers') }}

)
select 
driver_id,
given_name,
family_name,
date_of_birth,
nationality,
constructor_id,
constructor_name,
constructor_nationality
from drivers
where rn = 1

{% endsnapshot %}
