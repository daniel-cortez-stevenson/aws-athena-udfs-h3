# aws-athena-udfs-h3

This connector extends Amazon Athena's capability by adding UDFs (via Lambda) for selected [h3-java](https://github.com/uber/h3-java) Java functions to support geospatial indexing and queries with Uber's [H3](https://h3geo.org/)

## Deploy

### Option 1: Deploy the app with the AWS Console

1. Find the [App in the AWS Serverless Application Repository](https://console.aws.amazon.com/lambda/home?region=us-east-1#/create/app?applicationId=arn:aws:serverlessrepo:us-east-1:922535613973:applications/aws-athena-udfs-h3)
2. Click 'Deploy'

### Option 2: Deploy with the AWS SAM CLI

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
  --semantic-version 0.0.1
# deploy
sam deploy \
  --resolve-s3 \
  --stack-name aws-athena-udfs-h3-stack \
  --template-file ./target/packaged.yaml \
  --capabilities CAPABILITY_IAM # We don't need this, possible bug in aws-sam-cli
```

## Option 3: Deploy as an AWS SAM Resource

In your AWS SAM `template.yaml` file:

```yaml
Resources:
  AwsAthenaUdfsH3:
    Type: AWS::Serverless::Application
    Properties:
    Location:
      ApplicationId: arn:aws:serverlessrepo:us-east-1:922535613973:applications/aws-athena-udfs-h3
      SemanticVersion: 0.0.1
    Parameters:
      # The name of Lambda function, which calls the H3AthenaUDFHandler
      # LambdaFunctionName: 'h3-athena-udf-handler' # Uncomment to override default value
      # Lambda memory in MB
      # LambdaMemory: '3008' # Uncomment to override default value
      # Maximum Lambda invocation runtime in seconds
      # LambdaTimeout: '300' # Uncomment to override default value
```

## Contributing

Format your Java contributions with the *spotless* Maven plugin

```bash
mvn spotless:apply
```

## Publishing to the AWS Serverless Application Repository

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
  --semantic-version 0.0.1
```

## Usage

See [Querying with User Defined Functions](https://docs.aws.amazon.com/athena/latest/ug/querying-udf.html)

In the AWS Athena Console with an Athena workgroup with Athena Query Engine 2 enabled, select a `UDF_name` (any method of the `H3AthenaUDFHandler`) and implement the function signature accordingly:

```sql
USING EXTERNAL FUNCTION UDF_name(variable1 data_type[, variable2 data_type][,...])
```

See [create_osm_planet_table.sql](./sql/create_osm_planet_table.sql) to create some test data and [test_functions_run.sql](./sql/test_functions_run.sql) to run some of the methods.

## More Examples

See the AWS blog post [Translate and analyze text using SQL functions with Amazon Athena, Amazon Translate, and Amazon Comprehend](https://aws.amazon.com/blogs/machine-learning/translate-and-analyze-text-using-sql-functions-with-amazon-athena-amazon-translate-and-amazon-comprehend/)

## License

This project is licensed under the Apache-2.0 License.
