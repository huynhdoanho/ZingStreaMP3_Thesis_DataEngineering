{{ config(
    materialized = 'table'
) }}

SELECT {{ dbt_utils.generate_surrogate_key(['latitude', 'longitude', 'city', 'stateName']) }} as locationKey,
*
FROM
    (
        SELECT
            distinct city,
            COALESCE({{ source('staging', 'state_codes') }}.state_code, 'NA') as stateCode,
            COALESCE({{ source('staging', 'state_codes') }}.state_name, 'NA') as stateName,
            lat as latitude,
            lon as longitude
        FROM {{ source('external', 'listen_events') }}
        LEFT JOIN {{ source('staging', 'state_codes') }} /*state_codes*/ on listen_events.state = {{ source('staging', 'state_codes') }}.state_code

    ) as T