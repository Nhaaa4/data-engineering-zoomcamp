with source as (
    select * from {{ source('raw_data', 'fhv_tripdata') }}
),

renamed as (
    select 
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        {{ safe_cast('pulocationid', 'integer') }} as pickup_location_id,
        {{ safe_cast('dolocationid', 'integer') }} as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as datetime) as pickup_datetime,
        cast(dropoff_datetime as datetime) as dropoff_datetime,

        -- trip info
        cast(sr_flag as string) as sr_flag

    from source
    where dispatching_base_num is not null
)

select * from renamed
