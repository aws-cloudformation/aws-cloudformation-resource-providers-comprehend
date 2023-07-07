## Cloud Formation Resource Provider Package for Comprehend

Amazon Comprehend uses natural language processing (NLP) to extract insights about the content of documents without the need of any special preprocessing. Amazon Comprehend processes any text files in UTF-8 format. It develops insights by recognizing the entities, key phrases, language, sentiments, and other common elements in a document. Use Amazon Comprehend to create new products based on understanding the structure of documents. With Amazon Comprehend you can search social networking feeds for mentions of products, scan an entire document repository for key phrases, or determine the topics contained in a set of documents.

This repository contains code to manage the following SageMaker resource providers:

- AWS::Comprehend::Flywheel


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

=======
# aws-cloudformation-resource-providers-comprehend
The CloudFormation Resource Provider Package For Amazon Comprehend

# License
This library is licensed under the Apache 2.0 License.

# Development

The RPDK will automatically generate the correct resource model from the schema whenever the project is built via Maven. You can also do this manually with the following command: `cfn generate`.

> Please don't modify files under `target/generated-sources/rpdk`, as they will be automatically overwritten.

The code uses [Lombok](https://projectlombok.org/), and [you may have to install IDE integrations](https://projectlombok.org/setup/overview) to enable auto-complete for Lombok-annotated classes.

# Testing

## Local SAM Testing
Follow the CloudFormation [instructions](https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-walkthrough.html#resource-type-walkthrough-test) for performing SAM tests locally.

Some simple example templates can be found under the `sam-test-templates` folder. You can create a `sam-tests` folder locally, then copy and adjust the template files accordingly for your own testing.

### Prerequisites
- AWS SAM CLI
- Docker

### Common Errors
- `HandlerWrapper` class not found
    - Make sure `mvn package` is run before running the test
- Out of memory or timeout
    - Increase the Lambda timeout and memory limit as appropriate in the `template.yml` file