

SELECT md5(cast(coalesce(cast(latitude as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(longitude as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(city as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(stateName as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as locationKey,
*
FROM
    (
        SELECT
            distinct city,
            COALESCE("dev"."staging"."state_codes".state_code, 'NA') as stateCode,
            COALESCE("dev"."staging"."state_codes".state_name, 'NA') as stateName,
            lat as latitude,
            lon as longitude
        FROM "dev"."spectrum_schema"."listen_events"
        LEFT JOIN "dev"."staging"."state_codes" /*state_codes*/ on listen_events.state = "dev"."staging"."state_codes".state_code

    ) as T