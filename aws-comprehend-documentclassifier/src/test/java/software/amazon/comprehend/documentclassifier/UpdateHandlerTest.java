package software.amazon.comprehend.documentclassifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.DeleteResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.DeleteResourcePolicyResponse;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierResponse;
import software.amazon.awssdk.services.comprehend.model.DescribeResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeResourcePolicyResponse;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.comprehend.model.PutResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.PutResourcePolicyResponse;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.awssdk.services.comprehend.model.TagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.TagResourceResponse;
import software.amazon.awssdk.services.comprehend.model.UntagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.UntagResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractModelTestBase {

    protected static final ResourceHandlerRequest<ResourceModel> TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY =
            ResourceHandlerRequest.<ResourceModel>builder()
                    .clientRequestToken(TEST_CLIENT_REQUEST_TOKEN)
                    .desiredResourceState(TEST_RESOURCE_MODEL_UPDATED)
                    .awsPartition(TEST_PARTITION)
                    .awsAccountId(TEST_ACCOUNT_ID)
                    .region(TEST_REGION)
                    .logicalResourceIdentifier(TEST_LOGICAL_RESOURCE_IDENTIFIER)
                    .build();

    protected static final ResourceHandlerRequest<ResourceModel> TEST_RESOURCE_HANDLER_REQUEST_NO_MODEL_POLICY =
            ResourceHandlerRequest.<ResourceModel>builder()
                    .clientRequestToken(TEST_CLIENT_REQUEST_TOKEN)
                    .desiredResourceState(TEST_RESOURCE_MODEL_NO_POLICY)
                    .awsPartition(TEST_PARTITION)
                    .awsAccountId(TEST_ACCOUNT_ID)
                    .region(TEST_REGION)
                    .logicalResourceIdentifier(TEST_LOGICAL_RESOURCE_IDENTIFIER)
                    .build();

    protected static final ResourceHandlerRequest<ResourceModel> TEST_RESOURCE_HANDLER_REQUEST_WITH_STACK_TAGS =
            ResourceHandlerRequest.<ResourceModel>builder()
                    .clientRequestToken(TEST_CLIENT_REQUEST_TOKEN)
                    .desiredResourceState(TEST_RESOURCE_MODEL_UPDATED)
                    .desiredResourceTags(STACK_TAGS_MAP)
                    .awsPartition(TEST_PARTITION)
                    .awsAccountId(TEST_ACCOUNT_ID)
                    .region(TEST_REGION)
                    .previousSystemTags(SYSTEM_TAGS_MAP)
                    .systemTags(SYSTEM_TAGS_UPDATED_MAP)
                    .logicalResourceIdentifier(TEST_LOGICAL_RESOURCE_IDENTIFIER)
                    .build();

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<ComprehendClient> proxyClient;

    @Mock
    private ComprehendClient comprehendClient;
    
    @Captor
    ArgumentCaptor<DescribeDocumentClassifierRequest> describeDocumentClassifierRequestArgumentCaptor;

    @Captor
    ArgumentCaptor<DescribeResourcePolicyRequest> describeResourcePolicyRequestArgumentCaptor;

    @Captor
    ArgumentCaptor<PutResourcePolicyRequest> putResourcePolicyRequestArgumentCaptor;

    @Captor
    ArgumentCaptor<DeleteResourcePolicyRequest> deleteResourcePolicyRequestArgumentCaptor;

    @Captor
    ArgumentCaptor<ListTagsForResourceRequest> listTagsForResourceRequestArgumentCaptor;

    @Captor
    ArgumentCaptor<UntagResourceRequest> untagResourceRequestArgumentCaptor;

    @Captor
    ArgumentCaptor<TagResourceRequest> tagResourceRequestArgumentCaptor;

    private UpdateHandler handler;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(LOGGER, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = MOCK_PROXY(proxy, comprehendClient);
        handler = new UpdateHandler();
    }

    @Test
    public void handleRequest_Success_WhenStabilizationSucceeds() {
        List<Tag> expectedTagsPostUntagging = Arrays.asList(
                Tag.builder().key("key1").value("value1").build(),
                Tag.builder().key("key2").value("value2").build());
        Collection<Tag> expectedTagsPostTagging = Translator.toSdkTags(TEST_RESOURCE_MODEL_UPDATED.getTags());

        // Set up mock behavior
        // These empty responses aren't needed, but makeServiceCall is repeated if response is null
        when(comprehendClient.putResourcePolicy(any(PutResourcePolicyRequest.class)))
                .thenReturn(PutResourcePolicyResponse.builder().build());
        when(comprehendClient.untagResource(any(UntagResourceRequest.class)))
                .thenReturn(UntagResourceResponse.builder().build());
        when(comprehendClient.tagResource(any(TagResourceRequest.class)))
                .thenReturn(TagResourceResponse.builder().build());

        when(comprehendClient.describeResourcePolicy(any(DescribeResourcePolicyRequest.class)))
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY)  // original model policy
                        .build())
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY_UPDATED)  // new model policy
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(RESOURCE_TAGS)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(RESOURCE_TAGS)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(expectedTagsPostUntagging)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(expectedTagsPostUntagging)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(expectedTagsPostTagging)
                        .build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isEqualTo(TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY.getDesiredResourceState());
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isNull();
        assertThat(finalResponse.getErrorCode()).isNull();
        assertThat(finalResponse.getCallbackContext().getTagsToAdd()).isEqualTo(new HashSet<>(Arrays.asList(
                Tag.builder().key("key1").value("newValue1").build(),
                Tag.builder().key("key4").value("value4").build()
        )));
        assertThat(finalResponse.getCallbackContext().getTagKeysToRemove()).isEqualTo(
                new HashSet<>(Collections.singletonList("key3")));

        verify(comprehendClient, times(2)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).deleteResourcePolicy(any(DeleteResourcePolicyRequest.class));

        verify(comprehendClient, times(1)).putResourcePolicy(putResourcePolicyRequestArgumentCaptor.capture());
        PutResourcePolicyRequest expectedPutResourcePolicyRequest = PutResourcePolicyRequest.builder()
                .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                .resourcePolicy(TEST_MODEL_POLICY_UPDATED)
                .build();
        expectedPutResourcePolicyRequest.equalsBySdkFields(putResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(6)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).untagResource(untagResourceRequestArgumentCaptor.capture());
        UntagResourceRequest expectedUntagResourceRequest = UntagResourceRequest.builder()
                .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                .tagKeys(Arrays.asList("key3"))
                .build();
        assertUntagResourceRequestsEqual(expectedUntagResourceRequest, untagResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).tagResource(tagResourceRequestArgumentCaptor.capture());
        TagResourceRequest expectedTagResourceRequest = TagResourceRequest.builder()
                .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                .tags(new HashSet<>(Arrays.asList(
                        Tag.builder().key("key1").value("newValue1").build(),
                        Tag.builder().key("key4").value("value4").build())))
                .build();
        assertTagResourceRequestsEqual(expectedTagResourceRequest, tagResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenListTagsErrorsDuringStabilization() {
        // Set up mock behavior
        // These empty responses aren't needed, but makeServiceCall is repeated if response is null
        when(comprehendClient.putResourcePolicy(any(PutResourcePolicyRequest.class)))
                .thenReturn(PutResourcePolicyResponse.builder().build());
        when(comprehendClient.untagResource(any(UntagResourceRequest.class)))
                .thenReturn(UntagResourceResponse.builder().build());

        when(comprehendClient.describeResourcePolicy(any(DescribeResourcePolicyRequest.class)))
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY)  // original model policy
                        .build())
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY_UPDATED)  // new model policy
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(RESOURCE_TAGS)
                        .build())
                .thenThrow(RESOURCE_NOT_FOUND_EXCEPTION);


        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isEqualTo(RESOURCE_NOT_FOUND_EXCEPTION.getMessage());
        assertThat(finalResponse.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
        assertThat(finalResponse.getCallbackContext()).isNull();

        verify(comprehendClient, times(1)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).deleteResourcePolicy(any(DeleteResourcePolicyRequest.class));

        verify(comprehendClient, times(1)).putResourcePolicy(putResourcePolicyRequestArgumentCaptor.capture());
        PutResourcePolicyRequest expectedPutResourcePolicyRequest = PutResourcePolicyRequest.builder()
                .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                .resourcePolicy(TEST_MODEL_POLICY_UPDATED)
                .build();
        expectedPutResourcePolicyRequest.equalsBySdkFields(putResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(2)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).untagResource(untagResourceRequestArgumentCaptor.capture());
        UntagResourceRequest expectedUntagResourceRequest = UntagResourceRequest.builder()
                .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                .tagKeys(Arrays.asList("key3"))
                .build();
        assertUntagResourceRequestsEqual(expectedUntagResourceRequest, untagResourceRequestArgumentCaptor.getValue());
    }


    @Test
    public void handleRequest_NoMutatingCalls_WhenNoChangeInTagsAndModelPolicy() {
        // Set up mock behavior
        when(comprehendClient.describeResourcePolicy(any(DescribeResourcePolicyRequest.class)))
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY_UPDATED)
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(Translator.toSdkTags(TEST_RESOURCE_MODEL_UPDATED.getTags()))
                        .build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isEqualTo(
                TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY.getDesiredResourceState());
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isNull();
        assertThat(finalResponse.getErrorCode()).isNull();
        assertThat(finalResponse.getCallbackContext().getTagsToAdd()).isEmpty();
        assertThat(finalResponse.getCallbackContext().getTagKeysToRemove()).isEmpty();

        verify(comprehendClient, times(2)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).deleteResourcePolicy(any(DeleteResourcePolicyRequest.class));
        verify(comprehendClient, times(0)).putResourcePolicy(any(PutResourcePolicyRequest.class));

        verify(comprehendClient, times(2)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).untagResource(any(UntagResourceRequest.class));
        verify(comprehendClient, times(0)).tagResource(any(TagResourceRequest.class));

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_DeleteResourcePolicy_WhenModelPolicyRemoved() {
        // Set up mock behavior
        // These empty responses aren't needed, but makeServiceCall is repeated if response is null
        when(comprehendClient.deleteResourcePolicy(any(DeleteResourcePolicyRequest.class)))
                .thenReturn(DeleteResourcePolicyResponse.builder().build());

        when(comprehendClient.describeResourcePolicy(any(DescribeResourcePolicyRequest.class)))
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY)
                        .build())
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(Translator.toSdkTags(TEST_RESOURCE_MODEL_NO_POLICY.getTags()))
                        .build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_MODEL_POLICY, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isEqualTo(
                TEST_RESOURCE_HANDLER_REQUEST_NO_MODEL_POLICY.getDesiredResourceState());
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isNull();
        assertThat(finalResponse.getErrorCode()).isNull();
        assertThat(finalResponse.getCallbackContext().getTagsToAdd()).isEmpty();
        assertThat(finalResponse.getCallbackContext().getTagKeysToRemove()).isEmpty();

        verify(comprehendClient, times(2)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).deleteResourcePolicy(deleteResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DELETE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(deleteResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).putResourcePolicy(any(PutResourcePolicyRequest.class));

        verify(comprehendClient, times(2)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).untagResource(any(UntagResourceRequest.class));
        verify(comprehendClient, times(0)).tagResource(any(TagResourceRequest.class));

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenInternalServerException() {
        // Set up mock behavior
        when(comprehendClient.describeResourcePolicy(any(DescribeResourcePolicyRequest.class)))
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY)
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(Translator.toSdkTags(TEST_RESOURCE_MODEL_UPDATED.getTags()))
                        .build());
        doThrow(INTERNAL_SERVER_EXCEPTION).when(comprehendClient).deleteResourcePolicy(any(DeleteResourcePolicyRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_MODEL_POLICY, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.ServiceInternalError);
        assertThat(finalResponse.getMessage()).isEqualTo(INTERNAL_SERVER_EXCEPTION.getMessage());
        assertThat(finalResponse.getCallbackContext()).isNull();

        verify(comprehendClient, times(1)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).deleteResourcePolicy(deleteResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DELETE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(deleteResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).putResourcePolicy(any(PutResourcePolicyRequest.class));

        verify(comprehendClient, times(1)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).untagResource(any(UntagResourceRequest.class));
        verify(comprehendClient, times(0)).tagResource(any(TagResourceRequest.class));
        verify(comprehendClient, times(0)).describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenResourceNotFound() {
        // Set up mock behavior
        // These empty responses aren't needed, but makeServiceCall is repeated if response is null
        when(comprehendClient.putResourcePolicy(any(PutResourcePolicyRequest.class)))
                .thenReturn(PutResourcePolicyResponse.builder().build());

        when(comprehendClient.describeResourcePolicy(any(DescribeResourcePolicyRequest.class)))
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY)
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(RESOURCE_TAGS)
                        .build());
        doThrow(RESOURCE_NOT_FOUND_EXCEPTION).when(comprehendClient).untagResource(any(UntagResourceRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse).isNotNull();
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.NotFound);
        assertThat(finalResponse.getMessage()).isEqualTo(RESOURCE_NOT_FOUND_EXCEPTION.getMessage());
        assertThat(finalResponse.getCallbackContext()).isNull();

        verify(comprehendClient, times(1)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).deleteResourcePolicy(any(DeleteResourcePolicyRequest.class));

        verify(comprehendClient, times(1)).putResourcePolicy(putResourcePolicyRequestArgumentCaptor.capture());
        PutResourcePolicyRequest expectedPutResourcePolicyRequest = PutResourcePolicyRequest.builder()
                .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                .resourcePolicy(TEST_MODEL_POLICY_UPDATED)
                .build();
        expectedPutResourcePolicyRequest.equalsBySdkFields(putResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).untagResource(untagResourceRequestArgumentCaptor.capture());
        UntagResourceRequest expectedUntagResourceRequest = UntagResourceRequest.builder()
                .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                .tagKeys(Arrays.asList("key3"))
                .build();
        assertUntagResourceRequestsEqual(expectedUntagResourceRequest, untagResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).tagResource(any(TagResourceRequest.class));
        verify(comprehendClient, times(0)).describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class));
    }

    @Test
    public void handleRequest_RetriesServiceCall_WhenThrottled() {
        // Set up mock behavior
        // These empty responses aren't needed, but makeServiceCall is repeated if response is null
        when(comprehendClient.putResourcePolicy(any(PutResourcePolicyRequest.class)))
                .thenThrow(TOO_MANY_REQUESTS_EXCEPTION)
                .thenReturn(PutResourcePolicyResponse.builder().build());

        when(comprehendClient.describeResourcePolicy(any(DescribeResourcePolicyRequest.class)))
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY)  // original model policy
                        .build())
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY_UPDATED)  // new model policy
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(Translator.toSdkTags(TEST_RESOURCE_MODEL_UPDATED.getTags()))
                        .build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isEqualTo(TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY.getDesiredResourceState());
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isNull();
        assertThat(finalResponse.getErrorCode()).isNull();
        assertThat(finalResponse.getCallbackContext().getTagsToAdd()).isEmpty();
        assertThat(finalResponse.getCallbackContext().getTagKeysToRemove()).isEmpty();

        verify(comprehendClient, times(2)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).deleteResourcePolicy(any(DeleteResourcePolicyRequest.class));

        verify(comprehendClient, times(2)).putResourcePolicy(putResourcePolicyRequestArgumentCaptor.capture());
        PutResourcePolicyRequest expectedPutResourcePolicyRequest = PutResourcePolicyRequest.builder()
                .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                .resourcePolicy(TEST_MODEL_POLICY_UPDATED)
                .build();
        expectedPutResourcePolicyRequest.equalsBySdkFields(putResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(2)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).untagResource(any(UntagResourceRequest.class));
        verify(comprehendClient, times(0)).tagResource(any(TagResourceRequest.class));

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_UserTagsAndSystemTagsAdded_DuringUpdate() {
        List<Tag> expectedTagsPostUntagging = Arrays.asList(
                Tag.builder().key("key1").value("value1").build(),
                Tag.builder().key("key2").value("value2").build(),
                Tag.builder().key("aws:cloudformation:logical-id").value("testLogicalId").build(),
                Tag.builder().key("aws:cloudformation:stack-id").value("testStackId").build(),
                Tag.builder().key("aws:cloudformation:stack-name").value("testStackName").build());

        // Set up mock behavior
        // These tag & untag responses aren't needed, but makeServiceCall is repeated if response is null
        when(comprehendClient.untagResource(any(UntagResourceRequest.class)))
                .thenReturn(UntagResourceResponse.builder().build());
        when(comprehendClient.tagResource(any(TagResourceRequest.class)))
                .thenReturn(TagResourceResponse.builder().build());

        when(comprehendClient.describeResourcePolicy(any(DescribeResourcePolicyRequest.class)))
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY_UPDATED)
                        .build());
        // Resource tags are updated, and stack tags are added
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(RESOURCE_TAGS_WITH_SYSTEM_TAGS)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(expectedTagsPostUntagging)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(UPDATED_USER_TAGS_WITH_SYSTEM_TAGS)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                        .tags(UPDATED_USER_TAGS_WITH_UPDATED_SYSTEM_TAGS)
                        .build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_WITH_STACK_TAGS, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isEqualTo(TEST_RESOURCE_MODEL_WITH_SYSTEM_TAGS_UPDATED);
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isNull();
        assertThat(finalResponse.getErrorCode()).isNull();

        Set<Tag> expectedTagsToAdd = new HashSet<>(Arrays.asList(
                Tag.builder().key("key1").value("newValue1").build(),
                Tag.builder().key("key4").value("value4").build(),
                Tag.builder().key("stackKey1").value("stackValue1").build(),
                Tag.builder().key("stackKey2").value("stackValue2").build()));
        expectedTagsToAdd.addAll(SYSTEM_TAGS_UPDATED);
        Set<String> expectedTagKeysToRemove = new HashSet<>(Collections.singletonList("key3"));
        assertThat(finalResponse.getCallbackContext().getTagsToAdd()).isEqualTo(expectedTagsToAdd);
        assertThat(finalResponse.getCallbackContext().getTagKeysToRemove()).isEqualTo(expectedTagKeysToRemove);

        verify(comprehendClient, times(2)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).deleteResourcePolicy(any(DeleteResourcePolicyRequest.class));
        verify(comprehendClient, times(0)).putResourcePolicy(any(PutResourcePolicyRequest.class));

        verify(comprehendClient, times(5)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).untagResource(untagResourceRequestArgumentCaptor.capture());
        UntagResourceRequest expectedUntagResourceRequest = UntagResourceRequest.builder()
                .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                .tagKeys(expectedTagKeysToRemove)
                .build();
        assertUntagResourceRequestsEqual(expectedUntagResourceRequest, untagResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).tagResource(tagResourceRequestArgumentCaptor.capture());
        TagResourceRequest expectedTagResourceRequest = TagResourceRequest.builder()
                .resourceArn(TEST_DOCUMENT_CLASSIFIER_ARN_WITH_VERSION)
                .tags(expectedTagsToAdd)
                .build();
        assertTagResourceRequestsEqual(expectedTagResourceRequest, tagResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());
    }

}
