with 

source as (

    select * from {{ source('staging', 'transaction') }}

),

renamed as (

    select
        id,
        device_id,
        product_name,
        product_sku,
        product_name_name,
        amount,
        status,
        card_number,
        cvv,
        created_at,
        happened_at

    from source

)

select * from renamed
