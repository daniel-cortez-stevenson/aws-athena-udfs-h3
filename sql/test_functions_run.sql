USING EXTERNAL FUNCTION geotoh3(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS BIGINT
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION geotoh3address(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS VARCHAR
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3togeo(h3 BIGINT)
RETURNS VARCHAR
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3isvalid(h3 BIGINT)
RETURNS BOOLEAN
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3getbasecell(h3 BIGINT)
RETURNS INTEGER
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3ispentagon(h3 BIGINT)
RETURNS BOOLEAN
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3togeoboundary(h3 BIGINT)
RETURNS VARCHAR
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3kring(h3 BIGINT, k INTEGER)
RETURNS ARRAY<BIGINT>
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3addresstogeo(h3address VARCHAR)
RETURNS VARCHAR
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3addressisvalid(h3address VARCHAR)
RETURNS BOOLEAN
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3addressgetbasecell(h3address VARCHAR)
RETURNS INTEGER
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3addressispentagon(h3address VARCHAR)
RETURNS BOOLEAN
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3addresstogeoboundary(h3address VARCHAR)
RETURNS VARCHAR
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3addresskring(h3address VARCHAR, k INTEGER)
RETURNS ARRAY<VARCHAR>
LAMBDA 'aws_athena_udfs_h3'
SELECT
  *,
  h3isvalid(h3) h3_valid,
  h3getbasecell(h3) h3_basecell,
  h3ispentagon(h3) h3_pentagon,
  h3togeo(h3) h3_point,
  h3togeoboundary(h3) h3_polygon,
  h3kring(h3, 3) h3_kring,

  h3addressisvalid(h3address) h3address_valid,
  h3addressgetbasecell(h3address) h3address_basecell,
  h3addressispentagon(h3address) h3address_pentagon,
  h3addresstogeo(h3address) h3address_point,
  h3addresstogeoboundary(h3address) h3address_polygon,
  h3addresskring(h3address, 3) h3address_kring
 FROM
  (
  SELECT
    lat lat,
    lon lng,
    geotoh3(lat, lon, 13) h3,
    geotoh3address(lat, lon, 13) h3address
  FROM
    planet
  LIMIT 100
  )
;