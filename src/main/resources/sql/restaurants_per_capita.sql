USING 
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
with population as (
  SELECT
    h3,
    sum(population) population_sum
  FROM (
    SELECT
      geo_to_h3(latitude, longitude, 7) h3,
      population,
      latitude,
      longitude
    FROM hrsl
    where 
    	country = 'DEU'
    	and type = 'total_population'
  ) p
  GROUP BY h3
  HAVING sum(population) > 0.0
), restaurants as (
  SELECT
    h3,
    CAST(COUNT(*) AS DOUBLE) restaurant_count
  FROM (
	  SELECT
	    geo_to_h3(lat, lon, 7) h3,
	    lat,
	    lon
	  FROM
	    planet
	  where
	  	type = 'node'
	  	and tags['amenity'] = 'restaurant'
  ) r
  GROUP BY h3
  HAVING COUNT(*) > 1
)
SELECT
  rank() over(ORDER BY COALESCE(restaurant_count, 0.0) / population_sum DESC) "rank",
  COALESCE(restaurant_count, 0.0) / population_sum restaurants_per_capita,
  population.h3 h3,
  population_sum,
  COALESCE(restaurant_count, 0.0) restaurant_count
FROM
  population left join restaurants on population.h3 = restaurants.h3
ORDER BY rank ASC;