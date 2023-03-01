package software.amazon.comprehend.flywheel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.CreateFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DataSecurityConfig;
import software.amazon.awssdk.services.comprehend.model.DeleteFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierMode;
import software.amazon.awssdk.services.comprehend.model.DocumentClassificationConfig;
import software.amazon.awssdk.services.comprehend.model.EntityRecognitionConfig;
import software.amazon.awssdk.services.comprehend.model.EntityTypesListItem;
import software.amazon.awssdk.services.comprehend.model.FlywheelProperties;
import software.amazon.awssdk.services.comprehend.model.FlywheelStatus;
import software.amazon.awssdk.services.comprehend.model.FlywheelSummary;
import software.amazon.awssdk.services.comprehend.model.InternalServerException;
import software.amazon.awssdk.services.comprehend.model.ListFlywheelsRequest;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.ModelType;
import software.amazon.awssdk.services.comprehend.model.ResourceInUseException;
import software.amazon.awssdk.services.comprehend.model.ResourceLimitExceededException;
import software.amazon.awssdk.services.comprehend.model.TooManyRequestsException;
import software.amazon.awssdk.services.comprehend.model.ResourceNotFoundException;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.awssdk.services.comprehend.model.TagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.TaskConfig;
import software.amazon.awssdk.services.comprehend.model.UntagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.UpdateFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.VpcConfig;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.assertj.core.api.Assertions.assertThat;


public class AbstractTestBase {

  protected static final String TEST_FLYWHEEL_NAME = "niceFlywheel";
  protected static final String TEST_FLYWHEEL_NAME_2 = "nicerFlywheel";
  protected static final String TEST_PARTITION = "aws";
  protected static final String TEST_REGION = "us-west-2";
  protected static final String TEST_ACCOUNT_ID = "123456789012";
  protected static final String TEST_FLYWHEEL_ARN = String.format("arn:%s:comprehend:%s:%s:flywheel/%s", TEST_PARTITION, TEST_REGION, TEST_ACCOUNT_ID, TEST_FLYWHEEL_NAME);
  protected static final String TEST_FLYWHEEL_ARN_2 = String.format("arn:%s:comprehend:%s:%s:flywheel/%s", TEST_PARTITION, TEST_REGION, TEST_ACCOUNT_ID, TEST_FLYWHEEL_NAME_2);
  protected static final String TEST_ACTIVE_MODEL_ARN = String.format("arn:%s:comprehend:%s:%s:document-classifier/testDocumentClassifier", TEST_PARTITION, TEST_REGION, TEST_ACCOUNT_ID);
  protected static final String TEST_ACTIVE_MODEL_ARN_ER = String.format("arn:%s:comprehend:%s:%s:entity-recognizer/testEntityRecognizer", TEST_PARTITION, TEST_REGION, TEST_ACCOUNT_ID);
  protected static final String TEST_DATA_ACCESS_ROLE_ARN = String.format("arn:aws:iam::%s:role/DataAccessRole", TEST_ACCOUNT_ID);
  protected static final String TEST_DATA_LAKE_KMS_KEY_ARN = String.format("arn:%s:kms:%s:%s:key/0987dcba-09fe-87dc-65ba-ab0987654322", TEST_PARTITION, TEST_REGION, TEST_ACCOUNT_ID);
  protected static final String TEST_MODEL_KMS_KEY_ARN = String.format("arn:%s:kms:%s:%s:key/0987dcba-09fe-87dc-65ba-ab0987654321", TEST_PARTITION, TEST_REGION, TEST_ACCOUNT_ID);
  protected static final String TEST_VOLUME_KMS_KEY_ARN = String.format("arn:%s:kms:%s:%s:key/0987dcba-09fe-87dc-65ba-ab0987654320", TEST_PARTITION, TEST_REGION, TEST_ACCOUNT_ID);
  protected static final String TEST_DATA_LAKE_S3_URI = "s3://test-data-lake-location/";
  protected static final String TEST_OUTPUT_DATA_LAKE_S3_URI = "s3://test-data-lake-location/" + TEST_FLYWHEEL_NAME + "/schemaVersion=1/20220816T200755Z";
  protected static final String TEST_NEXT_TOKEN = "nextToken";
  protected static final String TEST_LANGUAGE_CODE = "en";
  protected static final String TEST_LOGICAL_RESOURCE_IDENTIFIER = "MyTestResource";
  protected static final String TEST_CLIENT_REQUEST_TOKEN = "12345678-a123-b123-c123-abc123456789";


  /* SDK OBJECTS */
  protected static final DataSecurityConfig SDK_DATA_SECURITY_CONFIG = DataSecurityConfig.builder()
          .vpcConfig(VpcConfig.builder()
                  .securityGroupIds("group-1", "group-2", "group-N")
                  .subnets("subnet-1", "subnet-2", "subnet-N")
                  .build())
          .modelKmsKeyId(TEST_MODEL_KMS_KEY_ARN)
          .volumeKmsKeyId(TEST_VOLUME_KMS_KEY_ARN)
          .dataLakeKmsKeyId(TEST_DATA_LAKE_KMS_KEY_ARN)
          .build();

  protected static final DocumentClassificationConfig SDK_CLR_CONFIG_MODE_LABELS = DocumentClassificationConfig.builder()
          .mode("MULTI_LABEL")
          .labels("a", "b", "c")
          .build();
  protected static final TaskConfig SDK_CLR_TASK_CONFIG = TaskConfig.builder()
          .languageCode(TEST_LANGUAGE_CODE)
          .documentClassificationConfig(SDK_CLR_CONFIG_MODE_LABELS)
          .build();
  protected static final EntityRecognitionConfig SDK_ER_CONFIG = EntityRecognitionConfig.builder()
          .entityTypes(
                  EntityTypesListItem.builder().type("a").build(),
                  EntityTypesListItem.builder().type("b").build(),
                  EntityTypesListItem.builder().type("c").build())
          .build();
  protected static final TaskConfig SDK_ER_TASK_CONFIG = TaskConfig.builder()
          .languageCode(TEST_LANGUAGE_CODE)
          .entityRecognitionConfig(SDK_ER_CONFIG)
          .build();

  protected static final FlywheelProperties TEST_FLYWHEEL_PROPERTIES_CREATING = buildFlywheelProperties(
          FlywheelStatus.CREATING, SDK_CLR_TASK_CONFIG, ModelType.DOCUMENT_CLASSIFIER, TEST_ACTIVE_MODEL_ARN,
          TEST_DATA_LAKE_S3_URI);
  protected static final FlywheelProperties TEST_FLYWHEEL_PROPERTIES_ACTIVE = buildFlywheelProperties(
          FlywheelStatus.ACTIVE, SDK_CLR_TASK_CONFIG, ModelType.DOCUMENT_CLASSIFIER, TEST_ACTIVE_MODEL_ARN,
          TEST_DATA_LAKE_S3_URI);
  protected static final FlywheelProperties TEST_FLYWHEEL_PROPERTIES_DELETING = buildFlywheelProperties(
          FlywheelStatus.DELETING, SDK_CLR_TASK_CONFIG, ModelType.DOCUMENT_CLASSIFIER, TEST_ACTIVE_MODEL_ARN,
          TEST_DATA_LAKE_S3_URI);
  protected static final FlywheelProperties TEST_FLYWHEEL_PROPERTIES_FAILED = buildFlywheelProperties(
          FlywheelStatus.FAILED, SDK_CLR_TASK_CONFIG, ModelType.DOCUMENT_CLASSIFIER, TEST_ACTIVE_MODEL_ARN,
          TEST_DATA_LAKE_S3_URI);
  protected static final FlywheelProperties TEST_FLYWHEEL_PROPERTIES_ACTIVE_ER = buildFlywheelProperties(
          FlywheelStatus.ACTIVE, SDK_ER_TASK_CONFIG, ModelType.ENTITY_RECOGNIZER, TEST_ACTIVE_MODEL_ARN_ER,
          TEST_DATA_LAKE_S3_URI);
  protected static final FlywheelProperties TEST_FLYWHEEL_PROPERTIES_TRANSFORMED_DATALAKE_S3_URI = buildFlywheelProperties(
          FlywheelStatus.ACTIVE, SDK_CLR_TASK_CONFIG, ModelType.DOCUMENT_CLASSIFIER, TEST_ACTIVE_MODEL_ARN,
          TEST_OUTPUT_DATA_LAKE_S3_URI);

  protected static final FlywheelSummary TEST_FLYWHEEL_SUMMARY = FlywheelSummary.builder()
          .flywheelArn(TEST_FLYWHEEL_ARN)
          .build();
  protected static final FlywheelSummary TEST_FLYWHEEL_SUMMARY_2 = FlywheelSummary.builder()
          .flywheelArn(TEST_FLYWHEEL_ARN_2)
          .build();
  protected static final List<FlywheelSummary> TEST_FLYWHEEL_SUMMARY_LIST = Arrays.asList(
          TEST_FLYWHEEL_SUMMARY,
          TEST_FLYWHEEL_SUMMARY_2
  );
  protected static final List<FlywheelSummary> TEST_EMPTY_FLYWHEEL_SUMMARY_LIST = Collections.emptyList();

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
  protected static ResourceModel TEST_RESOURCE_MODEL = buildResourceModel(
          TEST_ACTIVE_MODEL_ARN, ModelType.DOCUMENT_CLASSIFIER, SDK_CLR_TASK_CONFIG, RESOURCE_TAGS);
  protected static ResourceModel TEST_RESOURCE_MODEL_UPDATED = buildResourceModel(
          TEST_ACTIVE_MODEL_ARN, ModelType.DOCUMENT_CLASSIFIER, SDK_CLR_TASK_CONFIG, RESOURCE_TAGS_UPDATED);
  protected static ResourceModel TEST_RESOURCE_MODEL_WITH_SYSTEM_TAGS = buildResourceModel(
          TEST_ACTIVE_MODEL_ARN, ModelType.DOCUMENT_CLASSIFIER, SDK_CLR_TASK_CONFIG, USER_TAGS_WITH_SYSTEM_TAGS);
  protected static ResourceModel TEST_RESOURCE_MODEL_WITH_SYSTEM_TAGS_UPDATED = buildResourceModel(
          TEST_ACTIVE_MODEL_ARN, ModelType.DOCUMENT_CLASSIFIER, SDK_CLR_TASK_CONFIG, UPDATED_USER_TAGS_WITH_UPDATED_SYSTEM_TAGS);
  protected static ResourceModel TEST_RESOURCE_MODEL_NO_ARN = buildResourceModelNoArn(
          TEST_ACTIVE_MODEL_ARN, ModelType.DOCUMENT_CLASSIFIER, SDK_CLR_TASK_CONFIG, RESOURCE_TAGS);
  protected static ResourceModel TEST_RESOURCE_MODEL_ER = buildResourceModel(
          TEST_ACTIVE_MODEL_ARN_ER, ModelType.ENTITY_RECOGNIZER, SDK_ER_TASK_CONFIG, RESOURCE_TAGS);

  /* RESOURCE HANDLER REQUESTS */
  protected static final ResourceHandlerRequest<ResourceModel> TEST_RESOURCE_HANDLER_REQUEST =
          buildResourceHandlerRequest(TEST_RESOURCE_MODEL, SYSTEM_TAGS_MAP, SYSTEM_TAGS_MAP);
  protected static final ResourceHandlerRequest<ResourceModel> TEST_RESOURCE_HANDLER_REQUEST_NO_ARN =
          buildResourceHandlerRequest(TEST_RESOURCE_MODEL_NO_ARN, SYSTEM_TAGS_MAP, SYSTEM_TAGS_MAP);
  protected static final ResourceHandlerRequest<ResourceModel> TEST_RESOURCE_HANDLER_REQUEST_WITH_NEXT_TOKEN =
          buildResourceHandlerRequestWithNextToken(TEST_RESOURCE_MODEL, SYSTEM_TAGS_MAP, SYSTEM_TAGS_MAP);
  protected static final ResourceHandlerRequest<ResourceModel> TEST_RESOURCE_HANDLER_REQUEST_ER =
          buildResourceHandlerRequest(TEST_RESOURCE_MODEL_ER, SYSTEM_TAGS_MAP, SYSTEM_TAGS_MAP);


  /* SDK REQUESTS */
  protected static final CreateFlywheelRequest EXPECTED_CREATE_FLYWHEEL_REQUEST = Translator.translateToCreateRequest(
          TEST_RESOURCE_HANDLER_REQUEST_NO_ARN.getDesiredResourceState(),
          TEST_RESOURCE_HANDLER_REQUEST_NO_ARN.getClientRequestToken(),
          TagHelper.getDesiredTags(TEST_RESOURCE_HANDLER_REQUEST_NO_ARN)
  );
  protected static final DescribeFlywheelRequest EXPECTED_DESCRIBE_FLYWHEEL_REQUEST = Translator.translateToReadRequest(
          TEST_RESOURCE_HANDLER_REQUEST.getDesiredResourceState());
  protected static final ListTagsForResourceRequest EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST = Translator.translateToListTagsRequest(
          TEST_RESOURCE_HANDLER_REQUEST.getDesiredResourceState());
  protected static final DeleteFlywheelRequest EXPECTED_DELETE_FLYWHEEL_REQUEST = Translator.translateToDeleteRequest(
          TEST_RESOURCE_HANDLER_REQUEST.getDesiredResourceState());
  protected static final UpdateFlywheelRequest EXPECTED_UPDATE_FLYWHEEL_REQUEST = Translator.translateToUpdateRequest(
          TEST_RESOURCE_HANDLER_REQUEST.getDesiredResourceState());
  protected static final ListFlywheelsRequest EXPECTED_LIST_FLYWHEEL_REQUEST = Translator.translateToListRequest(
          null);
  protected static final ListFlywheelsRequest EXPECTED_LIST_FLYWHEEL_REQUEST_WITH_NEXT_TOKEN = Translator.translateToListRequest(
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

  static ProxyClient<ComprehendClient> MOCK_PROXY(
    final AmazonWebServicesClientProxy proxy,
    final ComprehendClient sdkClient) {
    return new ProxyClient<ComprehendClient>() {
      @Override
      public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseT
      injectCredentialsAndInvokeV2(RequestT request, Function<RequestT, ResponseT> requestFunction) {
        return proxy.injectCredentialsAndInvokeV2(request, requestFunction);
      }

      @Override
      public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
      CompletableFuture<ResponseT>
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
      public ComprehendClient client() {
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
          final AbstractFlywheelHandler handler
  ) {
    ProgressEvent<ResourceModel, CallbackContext> progressResponse = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

    while (progressResponse.canContinueProgress()) {
      progressResponse = handler.handleRequest(proxy, request, progressResponse.getCallbackContext(), proxyClient, logger);
    }

    return progressResponse;
  }

  private static FlywheelProperties buildFlywheelProperties(
          FlywheelStatus status, TaskConfig taskConfig, ModelType modelType, String activeModelArn,
          String dataLakeS3Uri) {
    return FlywheelProperties.builder()
            .activeModelArn(activeModelArn)
            .flywheelArn(TEST_FLYWHEEL_ARN)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .dataLakeS3Uri(dataLakeS3Uri)
            .dataSecurityConfig(SDK_DATA_SECURITY_CONFIG)
            .taskConfig(taskConfig)
            .modelType(modelType)
            .status(status)
            .build();
  }

  protected static ResourceModel buildResourceModelNoArn(
          String activeModelArn, ModelType modelType, TaskConfig sdkTaskConfig, List<Tag> sdkTags
  ) {
    return ResourceModel.builder()
            .activeModelArn(activeModelArn)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .dataLakeS3Uri(TEST_DATA_LAKE_S3_URI)
            .dataSecurityConfig(Translator.fromSdkDataSecurityConfig(SDK_DATA_SECURITY_CONFIG))
            .flywheelName(TEST_FLYWHEEL_NAME)
            .modelType(modelType.toString())
            .taskConfig(Translator.fromSdkTaskConfig(sdkTaskConfig))
            .tags(Translator.fromSdkTags(sdkTags))
            .build();
  }

  protected static ResourceModel buildResourceModel(
          String activeModelArn, ModelType modelType, TaskConfig sdkTaskConfig, List<Tag> sdkTags
  ) {
    ResourceModel resourceModel = buildResourceModelNoArn(activeModelArn, modelType, sdkTaskConfig, sdkTags);
    resourceModel.setArn(TEST_FLYWHEEL_ARN);
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
