-- Create an external table for Facebook Population Data-for-Good
CREATE EXTERNAL TABLE IF NOT EXISTS hrsl (
  `latitude` double,
  `longitude` double,
  `population` double
) PARTITIONED BY (
  month string,
  country string,
  type string
)
 ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
LOCATION 's3://dataforgood-fb-data/csv/'
TBLPROPERTIES ('has_encrypted_data'='false', 'skip.header.line.count'='1');

MSCK REPAIR TABLE hrsl;