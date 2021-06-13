USING 
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS BIGINT
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_to_geo(h3 BIGINT)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_is_valid(h3 BIGINT)
RETURNS BOOLEAN
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_get_base_cell(h3 BIGINT)
RETURNS INTEGER
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_is_pentagon(h3 BIGINT)
RETURNS BOOLEAN
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_to_geo_boundary(h3 BIGINT)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION k_ring(h3 BIGINT, k INTEGER)
RETURNS ARRAY<BIGINT>
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION cell_area(h3 BIGINT, unit VARCHAR)
RETURNS DOUBLE
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_distance(a BIGINT, b BIGINT)
RETURNS INTEGER
LAMBDA 'h3-athena-udf-handler',


EXTERNAL FUNCTION geo_to_h3_address(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_to_geo(h3_address VARCHAR)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_is_valid(h3_address VARCHAR)
RETURNS BOOLEAN
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_get_base_cell(h3_address VARCHAR)
RETURNS INTEGER
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_is_pentagon(h3_address VARCHAR)
RETURNS BOOLEAN
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_to_geo_boundary(h3_address VARCHAR)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION k_ring(h3_address VARCHAR, k INTEGER)
RETURNS ARRAY<VARCHAR>
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION cell_area(h3 VARCHAR, unit VARCHAR)
RETURNS DOUBLE
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_distance(a VARCHAR, b VARCHAR)
RETURNS INTEGER
LAMBDA 'h3-athena-udf-handler',

EXTERNAL FUNCTION h3_line("start" BIGINT, "end" BIGINT)
RETURNS ARRAY<BIGINT>
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_line(start_address VARCHAR, end_address VARCHAR)
RETURNS ARRAY<VARCHAR>
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION polyfill(points VARCHAR, holes ARRAY<VARCHAR>, res INTEGER)
RETURNS ARRAY<BIGINT>
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION polyfill_address(points VARCHAR, holes ARRAY<VARCHAR>, res INTEGER)
RETURNS ARRAY<VARCHAR>
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_get_resolution(h3 BIGINT)
RETURNS INTEGER
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_get_resolution(h3_address VARCHAR)
RETURNS INTEGER
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_to_parent(h3 BIGINT, res INTEGER)
RETURNS BIGINT
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_to_parent(h3_address VARCHAR, res INTEGER)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_to_children(h3 BIGINT, child_res INTEGER)
RETURNS ARRAY<BIGINT>
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_to_children(h3_address VARCHAR, child_res INTEGER)
RETURNS ARRAY<VARCHAR>
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_to_center_child(h3 BIGINT, child_res INTEGER)
RETURNS BIGINT
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3_to_center_child(h3 VARCHAR, child_res INTEGER)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler'

with tbl1 AS
(
  SELECT
    lat lat,
    lon lng,
    geo_to_h3(lat, lon, 8) h3,
    geo_to_h3(lat, lon, 14) h3_sm,
    geo_to_h3_address(lat, lon, 8) h3_address,
    geo_to_h3_address(lat, lon, 14) h3_address_sm
  FROM
    planet
  LIMIT 100
  ),

tbl2 AS
  (
    SELECT
      *,
      h3_is_valid(h3) h3_valid,
      h3_get_base_cell(h3) h3_basecell,
      h3_is_pentagon(h3) h3_pentagon,
      h3_to_geo(h3) h3_point,
      h3_to_geo_boundary(h3) h3_polygon,
      h3_to_geo_boundary(h3_sm) h3_polygon_sm,
      k_ring(h3, 3) h3_kring,
      cell_area(h3, 'm2') h3_area,
      h3_distance(h3, lag(h3) over ()) h3_distance,
      h3_line(h3, lag(h3, 1) over ()) h3_line,
	    h3_get_resolution(h3) h3_resolution,
	    h3_to_parent(h3, 7) h3_parent,
  	  h3_to_children(h3, 9) h3_children,
  	  h3_to_center_child(h3, 9) h3_center_child,

      h3_is_valid(h3_address) h3_address_valid,
      h3_get_base_cell(h3_address) h3_address_basecell,
      h3_is_pentagon(h3_address) h3_address_pentagon,
      h3_to_geo(h3_address) h3_address_point,
      h3_to_geo_boundary(h3_address) h3_address_polygon,
      h3_to_geo_boundary(h3_address_sm) h3_address_polygon_sm,
      k_ring(h3_address, 3) h3_address_kring,
      cell_area(h3_address, 'm2') h3_address_area,
      h3_distance(h3_address, lag(h3_address) over ()) h3_address_distance,
      h3_line(h3_address, lag(h3_address, 1) over ()) h3_line,
  	  h3_get_resolution(h3_address) h3_address_resolution,
	    h3_to_parent(h3_address, 7) h3_address_parent,
	    h3_to_children(h3_address, 9) h3_address_children,
      h3_to_center_child(h3_address, 9) h3_address_center_child,

    FROM tbl1
  )

SELECT
  *,
  polyfill(h3_polygon, ARRAY[h3_polygon_sm], 10) polyfill,
  polyfill_address(h3_polygon, ARRAY[h3_polygon_sm], 10) polyfill_address
FROM tbl2
;