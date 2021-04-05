-- Fetch FB population count for each population type for each OSM poi in Germany
-- Note: Must create Java8 `geoToH3` Athena UDF in eu-west-1 (Ireland)
-- - H3 Java Docs: https://github.com/uber/h3-java/blob/master/src/main/java/com/uber/h3core/H3Core.java#L167-L176
-- - OSM Athena Articls: https://engineering.door2door.io/querying-openstreetmap-buildings-with-aws-athena-ae50bf1bc5f0
-- - Athena GeoSpatial Functions (v2): https://docs.aws.amazon.com/athena/latest/ug/geospatial-functions-list-v2.html
-- - Athena UDF Docs: https://docs.aws.amazon.com/athena/latest/ug/querying-udf.html
USING FUNCTION geotoh3address(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS VARCHAR TYPE LAMBDA_INVOKE
WITH (lambda_name = 'athenaudfsam01')
  SELECT geotoh3address(lat,
         lon,
         11) h3_addr,
         lat,
         lon lng,
         id,
         cardinality(tags) tags_count,
         tags,
         changeset,
         timestamp,
         uid,
         version
FROM planet
WHERE type='node'
        AND cardinality(tags)>0