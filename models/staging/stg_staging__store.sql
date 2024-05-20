with 

source as (

    select * from {{ source('staging', 'store') }}

),

renamed as (

    select
        id,
        name,
        address,
        city,
        country,
        created_at,
        typology,
        customer_id

    from source

)

select * from renamed
