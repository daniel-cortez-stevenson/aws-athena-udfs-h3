USING EXTERNAL FUNCTION geotoh3(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS BIGINT
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3togeo(h3 BIGINT)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3isvalid(h3 BIGINT)
RETURNS BOOLEAN
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3getbasecell(h3 BIGINT)
RETURNS INTEGER
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3ispentagon(h3 BIGINT)
RETURNS BOOLEAN
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3togeoboundary(h3 BIGINT)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3kring(h3 BIGINT, k INTEGER)
RETURNS ARRAY<BIGINT>
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3area(h3 BIGINT, unit VARCHAR)
RETURNS DOUBLE
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3distance(a BIGINT, b BIGINT)
RETURNS INTEGER
LAMBDA 'h3-athena-udf-handler',


EXTERNAL FUNCTION geotoh3address(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3addresstogeo(h3address VARCHAR)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3addressisvalid(h3address VARCHAR)
RETURNS BOOLEAN
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3addressgetbasecell(h3address VARCHAR)
RETURNS INTEGER
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3addressispentagon(h3address VARCHAR)
RETURNS BOOLEAN
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3addresstogeoboundary(h3address VARCHAR)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3addresskring(h3address VARCHAR, k INTEGER)
RETURNS ARRAY<VARCHAR>
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3addressarea(h3address VARCHAR, unit VARCHAR)
RETURNS DOUBLE
LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION h3addressdistance(a VARCHAR, b VARCHAR)
RETURNS INTEGER
LAMBDA 'h3-athena-udf-handler'

with tbl1 AS
(
  SELECT
    lat lat,
    lon lng,
    geotoh3(lat, lon, 13) h3,
    geotoh3address(lat, lon, 13) h3address
  FROM
    planet
  LIMIT 100
  ),

tbl2 AS
  (
    SELECT
      h3distance(h3, lag(h3) over ()) h3_distance,
      h3addressdistance(h3address, lag(h3address) over ()) h3address_distance,
      *,
      h3isvalid(h3) h3_valid,
      h3getbasecell(h3) h3_basecell,
      h3ispentagon(h3) h3_pentagon,
      h3togeo(h3) h3_point,
      h3togeoboundary(h3) h3_polygon,
      h3kring(h3, 3) h3_kring,
      h3area(h3, 'm2') h3_area,

      h3addressisvalid(h3address) h3address_valid,
      h3addressgetbasecell(h3address) h3address_basecell,
      h3addressispentagon(h3address) h3address_pentagon,
      h3addresstogeo(h3address) h3address_point,
      h3addresstogeoboundary(h3address) h3address_polygon,
      h3addresskring(h3address, 3) h3address_kring,
      h3addressarea(h3address, 'm2') h3address_area
    FROM tbl1
  )

SELECT
  *
FROM tbl2
;