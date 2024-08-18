
  
    

  create  table
    "dev"."marts"."dim_songs__dbt_tmp"
    
    
    
  as (
    

SELECT md5(cast(coalesce(cast(songId as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS songKey,
       *
FROM (

        (
            SELECT song_id as songId,
                REPLACE(REPLACE(artist_name, '"', ''), '\\', '') as artistName,
                duration,
                key,
                key_confidence as keyConfidence,
                loudness,
                song_hotttnesss as songHotness,
                tempo,
                title,
                year
            FROM "dev"."staging"."songs"
        )

    ) as T
  );
  