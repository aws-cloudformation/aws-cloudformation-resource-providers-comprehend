package software.amazon.comprehend.documentclassifier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.CreateDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DeleteDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DeleteResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierDataFormat;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierInputDataConfig;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierMode;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierOutputDataConfig;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierProperties;
import software.amazon.awssdk.services.comprehend.model.InternalServerException;
import software.amazon.awssdk.services.comprehend.model.ListDocumentClassifiersRequest;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.ModelStatus;
import software.amazon.awssdk.services.comprehend.model.ResourceInUseException;
import software.amazon.awssdk.services.comprehend.model.ResourceLimitExceededException;
import software.amazon.awssdk.services.comprehend.model.ResourceNotFoundException;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.awssdk.services.comprehend.model.TagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.TooManyRequestsException;
import software.amazon.awssdk.services.comprehend.model.UntagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.VpcConfig;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class AbstractModelTestBase {
    protected static final Gson GSON = new Gson();
    protected static final String TEST_MODEL_NAME = "testModelName";
    protected static final String TEST_PARTITION = "aws";
    protected static final String TEST_REGION = "us-west-2";
    protected static final String TEST_ACCOUNT_ID = "123456789012";
    protected static final String TEST_OTHER_ACCOUNT_ID = "987654321012";
    protected static final String TEST_DOCUMENT_CLASSIFIER_ARN = String.format("arn:%s:comprehend:%s:%s:document-classifier/%s", TEST_PARTITION, TEST_REGION, TEST_ACCOUNT_ID, TEST_MODEL_NAME);
    protected static final String TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION = String.format("arn:%s:comprehend:%s:%s:document-classifier/%s/version/best-version", TEST_PARTITION, TEST_REGION, TEST_ACCOUNT_ID, TEST_MODEL_NAME);
    protected static final String TEST_DATA_ACCESS_ROLE_ARN = String.format("arn:aws:iam::%s:role/DataAccessRole", TEST_ACCOUNT_ID);
    protected static final String TEST_MODEL_KMS_KEY_ARN = String.format("arn:%s:kms:%s:%s:key/0987dcba-09fe-87dc-65ba-ab0987654321", TEST_PARTITION, TEST_REGION, TEST_ACCOUNT_ID);
    protected static final String TEST_VOLUME_KMS_KEY_ARN = String.format("arn:%s:kms:%s:%s:key/0987dcba-09fe-87dc-65ba-ab0987654320", TEST_PARTITION, TEST_REGION, TEST_ACCOUNT_ID);
    protected static final String TEST_OUTPUT_KMS_KEY_ARN = String.format("arn:%s:kms:%s:%s:key/0987dcba-09fe-87dc-65ba-ab0987654322", TEST_PARTITION, TEST_REGION, TEST_ACCOUNT_ID);
    protected static final String TEST_INPUT_S3_URI = "s3://some-bucket/input-location/";
    protected static final String TEST_TEST_S3_URI = "s3://some-bucket/test-location/";
    protected static final String TEST_INPUT_DOCUMENT_S3_URI = "s3://some-bucket/input-location/docs/";
    protected static final String TEST_TEST_DOCUMENT_S3_URI = "s3://some-bucket/test-location/docs/";
    protected static final String TEST_OUTPUT_S3_URI = "s3://some-bucket/output-location/";
    protected static final String ORIGINAL_OUTPUT_S3_URI = "s3://some-bucket/123456789012-CLR-7c5c6e452bde063102f5899e735a4649/output/output.tar.gz/document-classifer-output/";
    protected static final String DESCRIBED_OUTPUT_S3_URI = "s3://some-bucket/123456789012-CLR-7c5c6e452bde063102f5899e735a4649/output/output.tar.gz/document-classifer-output/123456789012-CLR-7c5c6e452bde063102f5899e735a4649/output/output.tar.gz";
    private static final DocumentClassifierOutputDataConfig TEST_OUTPUT_DATA_CONFIG = DocumentClassifierOutputDataConfig.builder()
            .kmsKeyId(TEST_OUTPUT_KMS_KEY_ARN)
            .s3Uri(TEST_OUTPUT_S3_URI)
            .build();
    protected static final String TEST_AUGMENTED_MANIFEST_ATTRIBUTE_NAME = "testAttributeName";
    protected static final String TEST_NEXT_TOKEN = "nextToken";
    protected static final String TEST_LOGICAL_RESOURCE_IDENTIFIER = "MyTestResource";
    protected static final String TEST_CLIENT_REQUEST_TOKEN = "12345678-a123-b123-c123-abc123456789";
    protected static final String TEST_LANGUAGE_CODE = "en";
    protected static final String TEST_VERSION_NAME = "1";
    protected static final String TEST_MODEL_POLICY = GSON.toJson(ImmutableMap.of(
            "Version", "2017-01-01",
            "Statement", ImmutableList.of(
                    ImmutableMap.of(
                            "Effect", "Allow",
                            "Action", "comprehend:ImportModel",
                            "Resource", ImmutableList.of(TEST_DOCUMENT_CLASSIFIER_ARN),
                            "Principal", ImmutableMap.of(
                                    "AWS", String.format("arn:%s:iam::%s:root", TEST_PARTITION, TEST_ACCOUNT_ID)
                            )
                    )
            )
    ));
    protected static final String TEST_MODEL_POLICY_UPDATED =  GSON.toJson(ImmutableMap.of(
            "Version", "2017-01-01",
            "Statement", ImmutableList.of(
                    ImmutableMap.of(
                            "Effect", "Allow",
                            "Action", "comprehend:ImportModel",
                            "Resource", ImmutableList.of(TEST_DOCUMENT_CLASSIFIER_ARN),
                            "Principal", ImmutableMap.of(
                                    "AWS", ImmutableList.of(
                                            String.format("arn:%s:iam::%s:root", TEST_PARTITION, TEST_ACCOUNT_ID),
                                            String.format("arn:%s:iam::%s:root", TEST_PARTITION, TEST_OTHER_ACCOUNT_ID)
                                    )
                            )
                    )
            )
    ));

    /* SDK OBJECTS */
    protected static final DocumentClassifierInputDataConfig TEST_INPUT_DATA_CONFIG = DocumentClassifierInputDataConfig.builder()
            .dataFormat(DocumentClassifierDataFormat.COMPREHEND_CSV)
            .s3Uri(TEST_INPUT_S3_URI)
            .build();
    
    protected static final VpcConfig TEST_VPC_CONFIG = VpcConfig.builder()
            .securityGroupIds("123")
            .subnets("456")
            .build();

    protected static final DocumentClassifierProperties TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING = DocumentClassifierProperties.builder()
            .documentClassifierArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .inputDataConfig(TEST_INPUT_DATA_CONFIG)
            .outputDataConfig(TEST_OUTPUT_DATA_CONFIG)
            .modelKmsKeyId(TEST_MODEL_KMS_KEY_ARN)
            .vpcConfig(TEST_VPC_CONFIG)
            .languageCode(TEST_LANGUAGE_CODE)
            .versionName(TEST_VERSION_NAME)
            .volumeKmsKeyId(TEST_VOLUME_KMS_KEY_ARN)
            .status(ModelStatus.TRAINING)
            .mode(DocumentClassifierMode.MULTI_CLASS)
            .build();
    protected static final DocumentClassifierProperties TEST_DOCUMENT_CLASSIFIER_PROPERTIES_UPDATING = DocumentClassifierProperties.builder()
            .documentClassifierArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .inputDataConfig(TEST_INPUT_DATA_CONFIG)
            .outputDataConfig(TEST_OUTPUT_DATA_CONFIG)
            .modelKmsKeyId(TEST_MODEL_KMS_KEY_ARN)
            .vpcConfig(TEST_VPC_CONFIG)
            .languageCode(TEST_LANGUAGE_CODE)
            .versionName(TEST_VERSION_NAME)
            .volumeKmsKeyId(TEST_VOLUME_KMS_KEY_ARN)
            .status(ModelStatus.TRAINED)
            .mode(DocumentClassifierMode.MULTI_CLASS)
            .build();
    protected static final DocumentClassifierProperties TEST_DOCUMENT_CLASSIFIER_PROPERTIES_DELETING = DocumentClassifierProperties.builder()
            .documentClassifierArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .inputDataConfig(TEST_INPUT_DATA_CONFIG)
            .outputDataConfig(TEST_OUTPUT_DATA_CONFIG)
            .modelKmsKeyId(TEST_MODEL_KMS_KEY_ARN)
            .vpcConfig(TEST_VPC_CONFIG)
            .languageCode(TEST_LANGUAGE_CODE)
            .versionName(TEST_VERSION_NAME)
            .volumeKmsKeyId(TEST_VOLUME_KMS_KEY_ARN)
            .status(ModelStatus.DELETING)
            .mode(DocumentClassifierMode.MULTI_CLASS)
            .build();
    protected static final DocumentClassifierProperties TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED = DocumentClassifierProperties.builder()
            .documentClassifierArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .inputDataConfig(TEST_INPUT_DATA_CONFIG)
            .outputDataConfig(TEST_OUTPUT_DATA_CONFIG)
            .modelKmsKeyId(TEST_MODEL_KMS_KEY_ARN)
            .vpcConfig(TEST_VPC_CONFIG)
            .languageCode(TEST_LANGUAGE_CODE)
            .versionName(TEST_VERSION_NAME)
            .volumeKmsKeyId(TEST_VOLUME_KMS_KEY_ARN)
            .status(ModelStatus.TRAINED)
            .mode(DocumentClassifierMode.MULTI_CLASS)
            .build();
    protected static final DocumentClassifierProperties TEST_DOCUMENT_CLASSIFIER_PROPERTIES_FAILED = DocumentClassifierProperties.builder()
            .documentClassifierArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .inputDataConfig(TEST_INPUT_DATA_CONFIG)
            .outputDataConfig(TEST_OUTPUT_DATA_CONFIG)
            .modelKmsKeyId(TEST_MODEL_KMS_KEY_ARN)
            .vpcConfig(TEST_VPC_CONFIG)
            .languageCode(TEST_LANGUAGE_CODE)
            .versionName(TEST_VERSION_NAME)
            .volumeKmsKeyId(TEST_VOLUME_KMS_KEY_ARN)
            .status(ModelStatus.IN_ERROR)
            .mode(DocumentClassifierMode.MULTI_CLASS)
            .build();

    protected static final List<DocumentClassifierProperties> TEST_DOCUMENT_CLASSIFIER_PROPERTIES_LIST = Arrays.asList(
            TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING,
            TEST_DOCUMENT_CLASSIFIER_PROPERTIES_UPDATING,
            TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED,
            TEST_DOCUMENT_CLASSIFIER_PROPERTIES_DELETING,
            TEST_DOCUMENT_CLASSIFIER_PROPERTIES_FAILED
    );
    protected static final List<DocumentClassifierProperties> TEST_EMPTY_DOCUMENT_CLASSIFIER_PROPERTIES_LIST = Collections.emptyList();

    protected static List<Tag> RESOURCE_TAGS = Arrays.asList(
            Tag.builder().key("key1").value("value1").build(),
            Tag.builder().key("key2").value("value2").build(),
            Tag.builder().key("key3").value("value3").build());
    protected static List<Tag> RESOURCE_TAGS_UPDATED = Arrays.asList(
            Tag.builder().key("key1").value("newValue1").build(),
            Tag.builder().key("key2").value("value2").build(),
            Tag.builder().key("key4").value("value4").build());
    protected static List<Tag> STACK_LEVEL_TAGS = Arrays.asList(
            Tag.builder().key("stackKey1").value("stackValue1").build(),
            Tag.builder().key("stackKey2").value("stackValue2").build());
    protected static List<Tag> SYSTEM_TAGS = Arrays.asList(
            Tag.builder().key("aws:cloudformation:logical-id").value("testLogicalId").build(),
            Tag.builder().key("aws:cloudformation:stack-id").value("testStackId").build(),
            Tag.builder().key("aws:cloudformation:stack-name").value("testStackName").build());
    protected static List<Tag> SYSTEM_TAGS_UPDATED = Arrays.asList(
            Tag.builder().key("aws:cloudformation:logical-id").value("testLogicalId2").build(),
            Tag.builder().key("aws:cloudformation:stack-id").value("testStackId2").build(),
            Tag.builder().key("aws:cloudformation:stack-name").value("testStackName2").build());
    protected static List<Tag> RESOURCE_TAGS_WITH_SYSTEM_TAGS =
            Stream.of(RESOURCE_TAGS, SYSTEM_TAGS)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
    protected static List<Tag> USER_TAGS_WITH_SYSTEM_TAGS =
            Stream.of(RESOURCE_TAGS, STACK_LEVEL_TAGS, SYSTEM_TAGS)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
    protected static List<Tag> UPDATED_USER_TAGS_WITH_SYSTEM_TAGS =
            Stream.of(RESOURCE_TAGS_UPDATED, STACK_LEVEL_TAGS, SYSTEM_TAGS)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
    protected static List<Tag> UPDATED_USER_TAGS_WITH_UPDATED_SYSTEM_TAGS =
            Stream.of(RESOURCE_TAGS_UPDATED, STACK_LEVEL_TAGS, SYSTEM_TAGS_UPDATED)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
    protected static Map<String, String> SYSTEM_TAGS_MAP = TagHelper.convertSdkTagsToKeyValueMap(SYSTEM_TAGS);
    protected static Map<String, String> SYSTEM_TAGS_UPDATED_MAP = TagHelper.convertSdkTagsToKeyValueMap(SYSTEM_TAGS_UPDATED);
    protected static Map<String, String> STACK_TAGS_MAP = TagHelper.convertSdkTagsToKeyValueMap(STACK_LEVEL_TAGS);

    /* CFN RESOURCE MODELS */
    protected static ResourceModel TEST_RESOURCE_MODEL = buildResourceModel(RESOURCE_TAGS, TEST_MODEL_POLICY);
    protected static ResourceModel TEST_RESOURCE_MODEL_UPDATED = buildResourceModel(RESOURCE_TAGS_UPDATED, TEST_MODEL_POLICY_UPDATED);
    protected static ResourceModel TEST_RESOURCE_MODEL_NO_POLICY = buildResourceModelWithNoPolicy(RESOURCE_TAGS);
    protected static ResourceModel TEST_RESOURCE_MODEL_WITH_SYSTEM_TAGS = buildResourceModel(USER_TAGS_WITH_SYSTEM_TAGS, TEST_MODEL_POLICY);
    protected static ResourceModel TEST_RESOURCE_MODEL_WITH_SYSTEM_TAGS_UPDATED = buildResourceModel(
            UPDATED_USER_TAGS_WITH_UPDATED_SYSTEM_TAGS, TEST_MODEL_POLICY_UPDATED);
    protected static ResourceModel TEST_RESOURCE_MODEL_NO_ARN = buildResourceModelNoArn(RESOURCE_TAGS);

    /* RESOURCE HANDLER REQUESTS */
    protected static final ResourceHandlerRequest<ResourceModel> TEST_RESOURCE_HANDLER_REQUEST =
            buildResourceHandlerRequest(TEST_RESOURCE_MODEL, SYSTEM_TAGS_MAP, SYSTEM_TAGS_MAP);
    protected static final ResourceHandlerRequest<ResourceModel> TEST_RESOURCE_HANDLER_REQUEST_NO_ARN =
            buildResourceHandlerRequest(TEST_RESOURCE_MODEL_NO_ARN, SYSTEM_TAGS_MAP, SYSTEM_TAGS_MAP);
    protected static final ResourceHandlerRequest<ResourceModel> TEST_RESOURCE_HANDLER_REQUEST_WITH_NEXT_TOKEN =
            buildResourceHandlerRequestWithNextToken(TEST_RESOURCE_MODEL, SYSTEM_TAGS_MAP, SYSTEM_TAGS_MAP);

    /* SDK REQUESTS */
    protected static final CreateDocumentClassifierRequest EXPECTED_CREATE_DOCUMENT_CLASSIFIER_REQUEST = Translator.translateToCreateRequest(
            TEST_RESOURCE_HANDLER_REQUEST_NO_ARN.getDesiredResourceState(),
            TEST_RESOURCE_HANDLER_REQUEST_NO_ARN.getClientRequestToken(),
            TagHelper.getDesiredTags(TEST_RESOURCE_HANDLER_REQUEST_NO_ARN)
    );
    protected static final DescribeDocumentClassifierRequest EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST = Translator.translateToReadRequest(
            TEST_RESOURCE_HANDLER_REQUEST.getDesiredResourceState());
    protected static final DescribeResourcePolicyRequest EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST = Translator.translateToDescribeResourcePolicyRequest(
            TEST_RESOURCE_HANDLER_REQUEST.getDesiredResourceState());
    protected static final DeleteResourcePolicyRequest EXPECTED_DELETE_RESOURCE_POLICY_REQUEST = Translator.translateToDeleteResourcePolicyRequest(
            TEST_RESOURCE_HANDLER_REQUEST.getDesiredResourceState());
    protected static final ListTagsForResourceRequest EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST = Translator.translateToListTagsRequest(
            TEST_RESOURCE_HANDLER_REQUEST.getDesiredResourceState());
    protected static final DeleteDocumentClassifierRequest EXPECTED_DELETE_DOCUMENT_CLASSIFIER_REQUEST = Translator.translateToDeleteRequest(
            TEST_RESOURCE_HANDLER_REQUEST.getDesiredResourceState());
    protected static final ListDocumentClassifiersRequest EXPECTED_LIST_DOCUMENT_CLASSIFIERS_REQUEST = Translator.translateToListRequest(
            null);
    protected static final ListDocumentClassifiersRequest EXPECTED_LIST_DOCUMENT_CLASSIFIERS_REQUEST_WITH_NEXT_TOKEN = Translator.translateToListRequest(
            TEST_NEXT_TOKEN);

    /* EXCEPTIONS */
    protected static final ResourceNotFoundException RESOURCE_NOT_FOUND_EXCEPTION = ResourceNotFoundException.builder()
            .message("resource not found")
            .build();
    protected static final InternalServerException INTERNAL_SERVER_EXCEPTION = InternalServerException.builder()
            .message("internal server error")
            .build();
    protected static final ResourceInUseException RESOURCE_IN_USE_EXCEPTION = ResourceInUseException.builder()
            .message("resource is in use/already exists")
            .build();
    protected static final ResourceLimitExceededException RESOURCE_LIMIT_EXCEEDED_EXCEPTION = ResourceLimitExceededException.builder()
            .message("resource limit has been exceeded")
            .build();
    protected static final TooManyRequestsException TOO_MANY_REQUESTS_EXCEPTION = TooManyRequestsException.builder()
            .message("too many requests")
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("Throttling").build())
            .build();

    protected static final Credentials MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
    protected static final LoggerProxy LOGGER = new LoggerProxy();

    protected static <T> ProxyClient<T> MOCK_PROXY(
            final AmazonWebServicesClientProxy proxy,
            final T sdkClient) {
        return new ProxyClient<T>() {
            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseT
            injectCredentialsAndInvokeV2(RequestT request, Function<RequestT, ResponseT> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> CompletableFuture<ResponseT>
            injectCredentialsAndInvokeV2Async(RequestT request, Function<RequestT, CompletableFuture<ResponseT>> requestFunction) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>>
            IterableT
            injectCredentialsAndInvokeIterableV2(RequestT request, Function<RequestT, IterableT> requestFunction) {
                return proxy.injectCredentialsAndInvokeIterableV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseInputStream<ResponseT>
            injectCredentialsAndInvokeV2InputStream(RequestT requestT, Function<RequestT, ResponseInputStream<ResponseT>> function) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseBytes<ResponseT>
            injectCredentialsAndInvokeV2Bytes(RequestT requestT, Function<RequestT, ResponseBytes<ResponseT>> function) {
                throw new UnsupportedOperationException();
            }

            @Override
            public T client() {
                return sdkClient;
            }
        };
    }

    protected ProgressEvent<ResourceModel, CallbackContext> invokeHandleRequestAndReturnFinalProgressEvent(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<ComprehendClient> proxyClient,
            final LoggerProxy logger,
            final AbstractModelHandler handler
    ) {
        ProgressEvent<ResourceModel, CallbackContext> progressResponse = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        while (progressResponse.canContinueProgress()) {
            progressResponse = handler.handleRequest(proxy, request, progressResponse.getCallbackContext(), proxyClient, logger);
        }

        return progressResponse;
    }

    protected static ResourceModel buildResourceModelNoArn(List<Tag> sdkTags) {
        return ResourceModel.builder()
                .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
                .inputDataConfig(Translator.fromSdkInputDataConfig(TEST_INPUT_DATA_CONFIG))
                .outputDataConfig(Translator.fromSdkOutputDataConfig(TEST_OUTPUT_DATA_CONFIG))
                .mode(DocumentClassifierMode.MULTI_CLASS.toString())
                .documentClassifierName(TEST_MODEL_NAME)
                .modelKmsKeyId(TEST_MODEL_KMS_KEY_ARN)
                .vpcConfig(Translator.fromSdkVpcConfig(TEST_VPC_CONFIG))
                .languageCode(TEST_LANGUAGE_CODE)
                .versionName(TEST_VERSION_NAME)
                .volumeKmsKeyId(TEST_VOLUME_KMS_KEY_ARN)
                .tags(Translator.fromSdkTags(sdkTags))
                .build();
    }

    protected static ResourceModel buildResourceModel(List<Tag> sdkTags, String modelPolicy) {
        ResourceModel resourceModel = buildResourceModelNoArn(sdkTags);
        resourceModel.setArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION);
        resourceModel.setModelPolicy(modelPolicy);
        return resourceModel;
    }

    protected static ResourceModel buildResourceModelWithNoPolicy(List<Tag> sdkTags) {
        ResourceModel resourceModel = buildResourceModelNoArn(sdkTags);
        resourceModel.setArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION);
        return resourceModel;
    }

    protected static ResourceHandlerRequest<ResourceModel> buildResourceHandlerRequest(
            ResourceModel resourceModel, Map<String, String> previousSystemTagsMap, Map<String, String> systemTagsMap
    ) {

        return ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TEST_CLIENT_REQUEST_TOKEN)
                .desiredResourceState(resourceModel)
                .desiredResourceTags(STACK_TAGS_MAP)
                .awsPartition(TEST_PARTITION)
                .awsAccountId(TEST_ACCOUNT_ID)
                .region(TEST_REGION)
                .logicalResourceIdentifier(TEST_LOGICAL_RESOURCE_IDENTIFIER)
                .previousSystemTags(previousSystemTagsMap)
                .systemTags(systemTagsMap)
                .build();
    }

    protected static ResourceHandlerRequest<ResourceModel> buildResourceHandlerRequestWithNextToken(
            ResourceModel resourceModel, Map<String, String> previousSystemTagsMap, Map<String, String> systemTagsMap
    ) {
        ResourceHandlerRequest<ResourceModel> request = buildResourceHandlerRequest(
                resourceModel, previousSystemTagsMap, systemTagsMap);
        request.setNextToken(TEST_NEXT_TOKEN);
        return request;
    }

    protected static void assertTagResourceRequestsEqual(
            final TagResourceRequest expectedTagResourceRequest,
            final TagResourceRequest actualTagResourceRequest
    ) {
        assertThat(expectedTagResourceRequest.resourceArn()).isEqualTo(actualTagResourceRequest.resourceArn());
        assertThat(new HashSet<>(expectedTagResourceRequest.tags())).isEqualTo(
                new HashSet<>(actualTagResourceRequest.tags()));
    }

    protected static void assertUntagResourceRequestsEqual(
            final UntagResourceRequest expectedUntagResourceRequest,
            final UntagResourceRequest actualUntagResourceRequest
    ) {
        assertThat(expectedUntagResourceRequest.resourceArn()).isEqualTo(expectedUntagResourceRequest.resourceArn());
        assertThat(new HashSet<>(actualUntagResourceRequest.tagKeys())).isEqualTo(
                new HashSet<>(actualUntagResourceRequest.tagKeys()));
    }

}
