
  
    

  create  table
    "dev"."marts"."dim_users__dbt_tmp"
    
    
    
  as (
    
    
-- level column in the users dimension is considered to be a SCD2 change. Just for the purpose of learning to write SCD2 change queries.
-- The below query is constructed to accommodate changing levels from free to paid and maintaining the latest state of the user along with
-- historical record of the user's level
SELECT md5(cast(coalesce(cast(userId as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(rowActivationDate as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(level as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as userKey, *
FROM
(
    SELECT CAST(userId AS BIGINT) as userId,
        firstName,
        lastName,
        gender,
        level,
        CAST(registration as BIGINT) as registration,
        minDate as rowActivationDate,
        -- Choose the start date from the next record and add that as the expiration date for the current record
        CASE 
            WHEN LEAD(minDate, 1) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY grouped) IS NULL THEN '9999-12-31' 
            ELSE LEAD(minDate, 1) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY grouped) 
        END AS rowExpirationDate,
        -- Assign a flag indicating which is the latest row for easier select queries
        CASE WHEN RANK() OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY grouped desc) = 1 THEN 1 ELSE 0 END AS currentRow
    FROM
    (
        -- Find the earliest date available for each free/paid status change
        SELECT userId, firstName, lastName, gender, registration, level, grouped, cast(min(date) as date) as minDate
        FROM
        -- Create distinct group of each level change to identify the change in level accurately
        (
            SELECT *, SUM(lagged) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as grouped

            FROM
            -- Lag the level and see where the user changes level from free to paid or otherwise
            (
                SELECT t1.*, CASE WHEN LAG(level) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY date) IS NULL OR LAG(level) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY date) <> level THEN 1 ELSE 0 END AS lagged

                from
                -- Select distinct state of user in each timestamp
                (
                    SELECT  distinct userId
                        ,firstName
                        ,lastName
                        ,gender
                        ,registration
                        ,level
                        ,ts AS date
                    FROM "dev"."spectrum_schema"."listen_events"
                    WHERE userId <> 0

                ) as t1
            ) as t2
        ) as t3
        GROUP BY userId, firstName, lastName, gender, registration, level, grouped
    ) as t4

    UNION ALL

    SELECT CAST(userId as BIGINT) as userKey,
        firstName,
        lastName,
        gender,
        level,
        CAST(registration as BIGINT) as registration,
        CAST(min(ts) as date) as rowActivationDate,
        DATE '9999-12-31' as rowExpirationDate,
        1 as currentRow
    FROM "dev"."spectrum_schema"."listen_events"
    WHERE userId = 0 or userId = 1
    GROUP BY userId, firstName, lastName, gender, level, registration
) as T
  );
  