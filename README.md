# aws-athena-udfs-h3

This connector extends Amazon Athena's capability by adding UDFs (via Lambda) for selected [h3-java](https://github.com/uber/h3-java) Java functions to support geospatial indexing and queries with Uber's [H3](https://h3geo.org/). A Maven Site hosted on GitHub Pages holds the [API documentation for this repository](https://daniel-cortez-stevenson.github.io/aws-athena-udfs-h3/).

## Deploy

### Option 1: Deploy the app with the AWS Console

1. Find the [App in the AWS Serverless Application Repository](https://console.aws.amazon.com/lambda/home?region=us-east-1#/create/app?applicationId=arn:aws:serverlessrepo:us-east-1:922535613973:applications/aws-athena-udfs-h3)
2. Click 'Deploy'

### Option 2: Deploy with the AWS SAM CLI

```bash
# build
mvn clean verify package -Dpublishing=true
# deploy
sam deploy \
  --resolve-s3 \
  --stack-name aws-athena-udfs-h3-stack \
  --template-file ./template.yaml \
  --capabilities CAPABILITY_IAM
```

### Option 3: Deploy as an AWS SAM Resource

In your AWS SAM `template.yaml` file:

```yaml
Resources:
  AwsAthenaUdfsH3:
    Type: AWS::Serverless::Application
    Properties:
    Location:
    ApplicationId: arn:aws:serverlessrepo:us-east-1:922535613973:applications/aws-athena-udfs-h3
    SemanticVersion: 1.0.0-rc7
    Parameters:
      # The name of Lambda function, which calls the H3AthenaUDFHandler
      # LambdaFunctionName: 'h3-athena-udf-handler' # Uncomment to override default value
      # Lambda memory in MB
      # LambdaMemory: '3008' # Uncomment to override default value
      # Maximum Lambda invocation runtime in seconds
      # LambdaTimeout: '300' # Uncomment to override default value
```

## Usage

The API is very similar to the h3-java API.

### Index coordinates

```sql
USING EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS BIGINT
LAMBDA 'h3-athena-udf-handler'
SELECT geo_to_h3(52.495999878401896, 13.414889023293945, 13) h3_index;
```

```text
|h3_index          |
|------------------|
|635554602371582271|
```

### Get the coordinates of an index

A `GeoCoord` in the h3-java API is represented as a well-known-text (WKT) point, which is compatible with [Athena geospatial functions](https://docs.aws.amazon.com/athena/latest/ug/geospatial-functions-list-v2.html).

```sql
USING EXTERNAL FUNCTION h3_to_geo(h3 BIGINT)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler'
select h3_to_geo(635554602371582271) wkt_point;
```

```text
|wkt_point                  |
|---------------------------|
|POINT (13.414849 52.496016)|
```

### Get the string representation of an index

```sql
USING EXTERNAL FUNCTION h3_to_string(h3 BIGINT)
RETURNS VARCHAR
LAMBDA 'h3-athena-udf-handler'
SELECT h3_to_string(635554602371582271) h3_address;
```

```text
h3_address     |
---------------+
8d1f18b25b9093f|
```

### More functions

See [Querying with User Defined Functions](https://docs.aws.amazon.com/athena/latest/ug/querying-udf.html)

In the AWS Athena Console with an Athena workgroup with Athena Query Engine 2 enabled, select a `udf_name` (any public method of the `H3AthenaUDFHandler`) and implement the function signature like so:

```sql
USING EXTERNAL FUNCTION udf_name(variable1 data_type[, variable2 data_type][,...])
RETURNS data_type
LAMBDA 'lambda-function-name'  -- the LambdaFunctionName of the serverless app.
SELECT  [...] udf_name(expression) [...]
```

## Known Limitations

Most h3-java API functions have an equivalent, snake-cased method in the `H3AthenaUDFHandler` API. Some do not.

- Functions returning lists of lists in the h3-java API are not supported. There is a limitation in the `UserDefinedFunctionHandler` that does not allow serialization of complex/nested types. These include:
  - `kRings`
  - `kRingDistances`
  - `hexRange`
- Experimental I, J coordinate h3-java API functions are not supported.
- The following UDFs do not work as expected, and should not be used:
  - `get_res_0_indexes() RETURNS ARRAY<BIGINT>`
    - Note: always throws `NullPointerException`
  - `get_res_0_indexes_addresses() RETURNS ARRAY<VARCHAR>`
    - Note: always throws `NullPointerException`

## Examples

### Data Sources

#### Open Street Maps

In the Athena console, run the query in [create_planet.sql](./src/main/resources/sql/create_planet.sql) to create some test data from the current [Open Street Maps](https://registry.opendata.aws/osm/) database.

Then run [test_udfs_planet.sql](./src/main/resources/sql/test_udfs_planet.sql) to test the H3 functions available via this application are registering and working correctly.

#### Facebook High Resolution Population Density Estimates

In the Athena console, run [create_hrsl.sql](./src/main/resources/sql/create_hrsl.sql), and then run [repair_hrsl.sql](./src/main/resources/sql/repair_hrsl.sql) to create some test data from the [Facebook Data For Good](https://dataforgood.fb.com/tools/population-density-maps/) Population Density dataset.

### Index Data Sources

In your SQL client, run the SQL script [create_hrsl_h3.sql](./src/main/resources/sql/create_hrsl_h3.sql) (or run each statement individually in the Athena console).

Then run [create_planet_h3.sql](./src/main/resources/sql/create_planet_h3.sql).

The created tables have an H3 index at resolution 15.

### Useful Example Query

Get restaurants per person in Germany at H3 resolution 7 and output H3 index string for mapping with tools like [Unfolded.ai](https://www.unfolded.ai) by running [restaurants_per_person.sql](./src/main/resources/sql/restaurants_per_person.sql).

[Go to the interactive map](https://studio.unfolded.ai/public/262d3af7-0857-4cb7-b134-f894558f9657/embed)

![Unfolded Map](./unfolded.png)

## Contributing

### Formatting

Format your Java contributions with the [spotless Maven plugin](https://github.com/diffplug/spotless/blob/main/plugin-maven/README.md). This is done automatically when running `mvn verify` or `mvn install`. Modify [pom.xml](./pom.xml) to change formatting rules.

```bash
mvn spotless:apply
```

### GitHub Pages Site

The [GitHub Pages Site](https://daniel-cortez-stevenson.github.io/aws-athena-udfs-h3/) is built with `mvn site` and is published manually. Change the contents of the site by modifying [pom.xml](./pom.xml) and [site.xml](site.xml).

Build the site locally.

```bash
mvn -Preporting site site:stage
# Open the built site in your browser
open ./target/site/index.html
```

Publish the site to GitHub Pages.

```bash
mvn scm-publish:publish-scm
```

### Publishing the UDFs to the AWS Serverless Application Repository

Publishing this code the the AWS Serverless Application Repository is done manually. New semantic versions should be published for new tagged commits in the `main` branch of this repository.

```bash
# build
mvn spotless:apply clean install -Dpublishing=true
# package
sam package \
  --resolve-s3 \
  --output-template-file ./target/packaged.yaml
# publish
sam publish \
  --template-file ./target/packaged.yaml \
  --semantic-version 1.0.0-rc7
```

## More Examples

See the AWS blog post [Translate and analyze text using SQL functions with Amazon Athena, Amazon Translate, and Amazon Comprehend](https://aws.amazon.com/blogs/machine-learning/translate-and-analyze-text-using-sql-functions-with-amazon-athena-amazon-translate-and-amazon-comprehend/)

## License

This project is licensed under the Apache-2.0 License.
