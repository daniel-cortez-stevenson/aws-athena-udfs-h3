# Athena H3 UDFs

[https://github.com/awslabs/aws-athena-query-federation/tree/master/athena-udfs]()

## Setup

1. Create an AWS Athena Workgroup
- AWS Region: `eu-west-1`
- Workgroup Name: `AmazonAthenaPreviewFunctionality`
- Athena Query Engine: `Athena engine version 2`

2. Local installs

```bash
brew tap AdoptOpenJDK/openjdk
brew reinstall adoptopenjdk8
brew tap aws/tap
brew reinstall awscli
brew reinstall aws-sam-cli
```

3. Clone the example repo and install it

```bash
git clone https://github.com/awslabs/aws-athena-query-federation
cd aws-athena-query-federation
git fetch --tags
git checkout -b < AWS_ATHENA_QUERY_FEDERATION_VERSION.txt
mvn clean install
cd ..
```

## Write Java 8 UDFs

Add [Uber H3](https://github.com/uber/h3-java/) to pom.xml

```xml
<dependency>
    <groupId>com.uber</groupId>
    <artifactId>h3</artifactId>
    <version>3.7.0</version> 
</dependency>
```

Add a geospatial method to the UDFHandler Class

- add as many geospatial functions as you want, actually :sunglasses:

```java
package com.amazonaws.athena.connectors.h3;

/**
 * ... base imports ...
 */
import com.uber.h3core.H3Core;

public class AthenaUDFHandler
        extends UserDefinedFunctionHandler
{
    /**
     * This method converts a lat / lng coordinate pair to an
     * Uber H3 address at a specified resolution.
     *
     * @param lat
     * @param lng
     * @param res
     * @return h3Address
     */
    public String geotoh3address(Double lat, Double lng, Integer res)
    {
        try {
            return H3Core.newInstance().geoToH3Address(lat, lng, res);
        }
        catch (IllegalArgumentException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
```

## Deployment

1. Build the project and send it to S3

```zsh
../tools/publish.sh \
  some-bucket-eu-west-1 \
  athena-udfs \
  eu-west-1
```

2. Deploy a Serverless App with Lambda Functions as a Cloudformation Stack

```zsh
sam deploy --template-file ./packaged.yaml --stack-name AthenaUDFSAM-01 \
  --parameter-overrides \
    'SecretNameOrPrefix=database-' \
    'LambdaFunctionName=athenaudfsam01' \
  --region eu-west-1 \
  --capabilities CAPABILITY_IAM
```

3. Run Athena UDF queries

```sql
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
```