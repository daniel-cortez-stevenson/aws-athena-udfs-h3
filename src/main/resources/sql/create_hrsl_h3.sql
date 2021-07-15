CREATE TABLE hrsl_h3
WITH ( partitioned_by = ARRAY['month', 'country', 'type'] )
AS
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'ABW',
    'AGO',
    'AIA',
    'ALB',
    'ANR',
    'ARE',
    'ARG',
    'ARM',
    'ASM',
    'ATG',
    'AUS',
    'AUT',
    'AZE',
    'BDI'
  );

INSERT INTO hrsl_h3
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'BEL',
    'BEN',
    'BFA',
    'BGD',
    'BGR',
    'BHR',
    'BHS',
    'BIH',
    'BLR',
    'BLZ',
    'BOL',
    'BRA',
    'BRB',
    'BRN'
  );

USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'BTN',
    'BWA',
    'CAF',
    'CHE',
    'CHL',
    'CIV',
    'CMR',
    'COD',
    'COG',
    'COL',
    'COM',
    'CPV',
    'CRI',
    'CUB'
  );


INSERT INTO hrsl_h3
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'CYM',
    'CYP',
    'CZE',
    'DEU',
    'DJI',
    'DMA',
    'DOM',
    'DZA',
    'ECU',
    'EGY',
    'ERI',
    'ESH',
    'ESP',
    'ETH'
  );

INSERT INTO hrsl_h3
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'FRA',
    'FSM',
    'GAB',
    'GBR',
    'GEO',
    'GGY',
    'GHA',
    'GIB',
    'GIN',
    'GLP',
    'GMB',
    'GNB',
    'GNQ',
    'GRC'
  );

INSERT INTO hrsl_h3
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'GRD',
    'GTM',
    'GUF',
    'GUM',
    'GUY',
    'HKG',
    'HND',
    'HRV',
    'HTI',
    'HUN',
    'IDN',
    'IRN',
    'IRQ',
    'ISL'
  );

INSERT INTO hrsl_h3
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'ITA',
    'JAM',
    'JOR',
    'JPN',
    'KHM',
    'KNA',
    'KOR',
    'KOS',
    'KWT',
    'LAO',
    'LBN',
    'LBR',
    'LBY',
    'LCA'
  );

INSERT INTO hrsl_h3
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'LIE',
    'LKA',
    'LSO',
    'LUX',
    'MAC',
    'MAR',
    'MCO',
    'MDA',
    'MDG',
    'MDV',
    'MEX',
    'MHL',
    'MKD',
    'MLI'
  );

INSERT INTO hrsl_h3
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'MLT',
    'MMR',
    'MNE',
    'MNP',
    'MOZ',
    'MRT',
    'MSR',
    'MUS',
    'MYS',
    'MYT',
    'NAM',
    'NCL',
    'NER',
    'NGA'
  );

INSERT INTO hrsl_h3
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'NIC',
    'NLD',
    'NPL',
    'NRU',
    'OMN',
    'PAN',
    'PCN',
    'PER',
    'PHL',
    'PLW',
    'PNG',
    'POL',
    'PRI',
    'PRT'
  );

INSERT INTO hrsl_h3
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'PRY',
    'PSE',
    'PYF',
    'QAT',
    'REU',
    'ROU',
    'RWA',
    'SAU',
    'SDN',
    'SEN',
    'SGP',
    'SLB',
    'SLE',
    'SLV'
  );

INSERT INTO hrsl_h3
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'SMR',
    'SOM',
    'SRB',
    'SSD',
    'STP',
    'SUR',
    'SVK',
    'SVN',
    'SWZ',
    'SYC',
    'SYR',
    'TCA',
    'TCD',
    'TGO'
  );

INSERT INTO hrsl_h3
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'THA',
    'TLS',
    'TON',
    'TTO',
    'TUN',
    'TUR',
    'TUV',
    'TWN',
    'TZA',
    'UGA',
    'UKR',
    'URY',
    'USA',
    'VCT'
  );

INSERT INTO hrsl_h3
USING
EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS BIGINT LAMBDA 'h3-athena-udf-handler'
SELECT
  geo_to_h3(latitude, longitude, 15) h3_index,
  15 h3_resolution,
  latitude,
  longitude,
  population,
  month,
  country,
  "type"
FROM
  hrsl
WHERE
  country IN (
    'VEN',
    'VGB',
    'VIR',
    'VNM',
    'VUT',
    'WLF',
    'WSM',
    'YEM',
    'ZAF',
    'ZMB',
    'ZWE'
  );
