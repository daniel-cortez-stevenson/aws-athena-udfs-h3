USING EXTERNAL FUNCTION geotoh3index(lat DOUBLE, lon DOUBLE, res INTEGER)
RETURNS BIGINT
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION geotoh3address(lat DOUBLE, lon DOUBLE, res INTEGER)
RETURNS VARCHAR
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3indextogeo(h3index BIGINT)
RETURNS ROW(lat DOUBLE, lng DOUBLE)
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3indexisvalid(h3index BIGINT)
RETURNS BOOLEAN
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3indexgetbasecell(h3index BIGINT)
RETURNS INTEGER
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3indexispentagon(h3index BIGINT)
RETURNS BOOLEAN
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3indextogeoboundary(h3index BIGINT)
RETURNS ARRAY<ROW(lat DOUBLE, lng DOUBLE)>
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3indexkring(h3index BIGINT, k INTEGER)
RETURNS ARRAY<BIGINT>
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3addresstogeo(h3address VARCHAR)
RETURNS ROW(lat DOUBLE, lng DOUBLE)
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
RETURNS ARRAY<ROW(lat DOUBLE, lng DOUBLE)>
LAMBDA 'aws_athena_udfs_h3',
EXTERNAL FUNCTION h3addresskring(h3address VARCHAR, k INTEGER)
RETURNS ARRAY<VARCHAR>
LAMBDA 'aws_athena_udfs_h3'
SELECT
  *,
  h3indexisvalid(h3index) h3index_valid,
  h3indexgetbasecell(h3index) h3index_basecell,
  h3indexispentagon(h3index) h3index_pentagon,
  h3indextogeo(h3index) h3index_point,
  h3indextogeoboundary(h3index) h3index_polygon,
  h3indexkring(h3index, 3) h3index_kring,

  h3addressisvalid(h3address) h3address_valid,
  h3addressgetbasecell(h3address) h3address_basecell,
  h3addressispentagon(h3address) h3address_pentagon,
  h3addresstogeo(h3address) h3address_point,
  h3addresstogeoboundary(h3address) h3address_polygon,
  h3addresskring(h3address, 3) h3address_kring
 FROM
  (
  SELECT
    lat,
    lon,
    geotoh3index(lat, lon, 13) h3index,
    geotoh3address(lat, lon, 13) h3address
  FROM
    planet
  LIMIT 100
  )
;