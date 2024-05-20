with 

source as (

    select * from {{ source('staging', 'device') }}

),

renamed as (

    select
        id,
        type,
        store_id

    from source

)

select * from renamed
