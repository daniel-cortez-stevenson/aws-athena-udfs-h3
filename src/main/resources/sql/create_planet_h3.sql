CREATE TABLE IF NOT EXISTS planet_h3
AS
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  id,
  "type",
  CASE when (tags != map()) THEN tags END tags,
  geo_to_h3(lat, lon, 15) h3_index,
  15 h3_resolution,
  lat,
  lon,
  CASE when (nds != array[]) THEN nds END nds,
  CASE when (members != array[]) THEN members END members,
  changeset,
  "timestamp",
  uid,
  "user",
  version
FROM
 planet;