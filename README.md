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
    SemanticVersion: 1.0.0-rc2
    Parameters:
      # The name of Lambda function, which calls the H3AthenaUDFHandler
      # LambdaFunctionName: 'h3-athena-udf-handler' # Uncomment to override default value
      # Lambda memory in MB
      # LambdaMemory: '3008' # Uncomment to override default value
      # Maximum Lambda invocation runtime in seconds
      # LambdaTimeout: '300' # Uncomment to override default value
```

## Usage

See [Querying with User Defined Functions](https://docs.aws.amazon.com/athena/latest/ug/querying-udf.html)

In the AWS Athena Console with an Athena workgroup with Athena Query Engine 2 enabled, select a `UDF_name` (any method of the `H3AthenaUDFHandler`) and implement the function signature like so:

```sql
USING EXTERNAL FUNCTION udf_name(variable1 data_type[, variable2 data_type][,...])
RETURNS data_type
LAMBDA 'h3-athena-udf-handler'
SELECT  [...] udf_name(expression) [...]
```

## Known Limitations

The following UDFs do not work as expected, and should not be used:

```sql
k_rings(h3 BIGINT, k INTEGER) RETURNS ARRAY<ARRAY<BIGINT>> -- always returns NULL
k_rings(h3_address VARCHAR, k INTEGER) RETURNS ARRAY<ARRAY<VARCHAR>> -- always returns NULL
k_ring_distances(h3 BIGINT, k INTEGER) RETURNS ARRAY<ARRAY<BIGINT>> -- always returns NULL
k_ring_distances(h3_address VARCHAR, k INTEGER) RETURNS ARRAY<ARRAY<VARCHAR>> -- always returns NULL
hex_range(h3 BIGINT, k INTEGER) RETURNS ARRAY<ARRAY<BIGINT>> -- always returns NULL
hex_range(h3_address VARCHAR, k INTEGER) RETURNS ARRAY<ARRAY<VARCHAR>> -- always returns NULL
hex_ring(h3 BIGINT, k INTEGER) RETURNS ARRAY<ARRAY<BIGINT>> -- always returns NULL
hex_ring(h3_address VARCHAR, k INTEGER) RETURNS ARRAY<ARRAY<VARCHAR>> -- always returns NULL
exact_edge_length(edge BIGINT, unit VARCHAR) RETURNS DOUBLE -- always returns 0.0
exact_edge_length(edge_address VARCHAR, unit VARCHAR) RETURNS DOUBLE -- always returns 0.0
get_res_0_indexes() RETURNS ARRAY<BIGINT> -- always throws NullPointerException
get_res_0_indexes_addresses() RETURNS ARRAY<VARCHAR> -- always throws NullPointerException
```

## Test Data

### Open Street Maps

In the Athena console, run the query in [create_osm_planet_table.sql](./src/main/resources/sql/create_osm_planet_table.sql) to create some test data from the current [Open Street Maps](https://registry.opendata.aws/osm/) database and then run the query [test_functions_run.sql](./src/main/resources/sql/est_udfs_osm_planet.sql) to test drive some of the H3 functions available via this application.

### Facebook High Resolution Population Density Estimates

In the Athena console, run the query in [create_fb_population_table.sql](./src/main/resources/sql/create_fb_population_table.sql) and then run the query in [repair_fb_population_table.sql](./src/main/resources/sql/repair_fb_population_table.sql) to create some test data from the [Facebook Data For Good](https://dataforgood.fb.com/tools/population-density-maps/) Population Density dataset. You'll have to write your own Athena SQL queries for this data source.

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
  --semantic-version 1.0.0-rc2
```

## More Examples

See the AWS blog post [Translate and analyze text using SQL functions with Amazon Athena, Amazon Translate, and Amazon Comprehend](https://aws.amazon.com/blogs/machine-learning/translate-and-analyze-text-using-sql-functions-with-amazon-athena-amazon-translate-and-amazon-comprehend/)

## License

This project is licensed under the Apache-2.0 License.
