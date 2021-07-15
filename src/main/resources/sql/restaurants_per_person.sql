USING
EXTERNAL FUNCTION h3_to_parent(h3 BIGINT, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_to_string(h3 BIGINT) RETURNS VARCHAR LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION cell_area(h3 BIGINT, unit VARCHAR) RETURNS DOUBLE LAMBDA 'h3-athena-udf-handler'
with population AS (
  SELECT
    h3_index,
    sum(population) population_sum
  FROM (
    SELECT 
      h3_to_parent(h3_index, 7) h3_index,
      population,
      h3_index h3_index_old
    FROM hrsl_h3
    WHERE
      country = 'DEU'
      AND type = 'total_population'
  )
  GROUP BY 1
  HAVING sum(population) > 0
), restaurants AS (
  SELECT
    h3_index,
    CAST(COUNT(*) AS DOUBLE) restaurant_count
  FROM (
    SELECT
      h3_to_parent(h3_index, 7) h3_index,
      h3_index h3_index_old
    FROM planet_h3
    WHERE
      type = 'node'
	  AND tags['amenity'] = 'restaurant'
  ) 
  GROUP BY 1
)
SELECT
  rank() over(ORDER BY COALESCE(restaurant_count, 0.0) / population_sum DESC) "rank",
  COALESCE(restaurant_count, 0.0) / population_sum restaurants_per_person,
  h3_to_string(population.h3_index) h3_address,
  population_sum,
  COALESCE(restaurant_count, 0.0) restaurant_count,
  cell_area(population.h3_index, 'm2') area_m2
FROM
  population LEFT JOIN restaurants ON population.h3_index = restaurants.h3_index
ORDER BY rank ASC;