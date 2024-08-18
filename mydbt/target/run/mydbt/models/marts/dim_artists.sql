
  
    

  create  table
    "dev"."marts"."dim_artists__dbt_tmp"
    
    
    
  as (
    

SELECT md5(cast(coalesce(cast(artistId as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS artistKey,
    *
FROM (
        SELECT
            MAX(artist_id) AS artistId,
            MAX(artist_latitude) AS latitude,
            MAX(artist_longitude) AS longitude,
            MAX(artist_location) AS location,
            REPLACE(REPLACE(artist_name, '"', ''), '\\', '') AS name
        FROM "dev"."staging"."songs"
        GROUP BY artist_name

        UNION ALL

        SELECT 'NNNNNNNNNNNNNNN',
            0,
            0,
            'NA',
            'NA'

    ) as T
  );
  