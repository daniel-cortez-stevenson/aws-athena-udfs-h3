-- Create an external table for OSM data
CREATE EXTERNAL TABLE planet (
  id BIGINT,
  type STRING,
  tags MAP<STRING,STRING>,
  lat DECIMAL(9,7), -- for nodes
  lon DECIMAL(10,7), -- for nodes
  nds ARRAY<STRUCT<ref: BIGINT>>, -- for ways
  members ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>, -- for relations
  changeset BIGINT,
  timestamp TIMESTAMP,
  uid BIGINT,
  user STRING,
  version BIGINT
)
STORED AS ORCFILE
LOCATION 's3://osm-pds/planet/';