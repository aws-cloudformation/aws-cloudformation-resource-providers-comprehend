package software.amazon.comprehend.flywheel;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.CreateFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.CreateFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.InternalServerException;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.awssdk.services.comprehend.model.TagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.TagResourceResponse;
import software.amazon.awssdk.services.comprehend.model.UntagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.UntagResourceResponse;
import software.amazon.awssdk.services.comprehend.model.UpdateFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.UpdateFlywheelResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    protected static final ResourceHandlerRequest<ResourceModel> TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY =
            ResourceHandlerRequest.<ResourceModel>builder()
                    .clientRequestToken(TEST_CLIENT_REQUEST_TOKEN)
                    .desiredResourceState(TEST_RESOURCE_MODEL_UPDATED)
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
    ArgumentCaptor<UpdateFlywheelRequest> updateFlywheelRequestArgumentCaptor;

    @Captor
    ArgumentCaptor<DescribeFlywheelRequest> describeFlywheelRequestArgumentCaptor;

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
        // These tag & untag responses aren't needed, but makeServiceCall is repeated if response is null
        when(comprehendClient.untagResource(any(UntagResourceRequest.class)))
                .thenReturn(UntagResourceResponse.builder().build());
        when(comprehendClient.tagResource(any(TagResourceRequest.class)))
                .thenReturn(TagResourceResponse.builder().build());

        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(RESOURCE_TAGS)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(RESOURCE_TAGS)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(expectedTagsPostUntagging)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(expectedTagsPostUntagging)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(expectedTagsPostTagging)
                        .build());
        when(comprehendClient.updateFlywheel(any(UpdateFlywheelRequest.class)))
                .thenReturn(UpdateFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
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

        verify(comprehendClient, times(6)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).updateFlywheel(updateFlywheelRequestArgumentCaptor.capture());
        EXPECTED_UPDATE_FLYWHEEL_REQUEST.equalsBySdkFields(updateFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).untagResource(untagResourceRequestArgumentCaptor.capture());
        UntagResourceRequest expectedUntagResourceRequest = UntagResourceRequest.builder()
                .resourceArn(TEST_FLYWHEEL_ARN)
                .tagKeys(Arrays.asList("key3"))
                .build();
        assertUntagResourceRequestsEqual(expectedUntagResourceRequest, untagResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).tagResource(tagResourceRequestArgumentCaptor.capture());
        TagResourceRequest expectedTagResourceRequest = TagResourceRequest.builder()
                .resourceArn(TEST_FLYWHEEL_ARN)
                .tags(new HashSet<>(Arrays.asList(
                        Tag.builder().key("key1").value("newValue1").build(),
                        Tag.builder().key("key4").value("value4").build())))
                .build();
        assertTagResourceRequestsEqual(expectedTagResourceRequest, tagResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenListTagsErrorsDuringStabilization() {
        // Set up mock behavior
        // These tag & untag responses aren't needed, but makeServiceCall is repeated if response is null
        when(comprehendClient.untagResource(any(UntagResourceRequest.class)))
                .thenReturn(UntagResourceResponse.builder().build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(RESOURCE_TAGS)
                        .build())
                .thenThrow(RESOURCE_NOT_FOUND_EXCEPTION);
        when(comprehendClient.updateFlywheel(any(UpdateFlywheelRequest.class)))
                .thenReturn(UpdateFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build());

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

        verify(comprehendClient, times(2)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).updateFlywheel(updateFlywheelRequestArgumentCaptor.capture());
        EXPECTED_UPDATE_FLYWHEEL_REQUEST.equalsBySdkFields(updateFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).untagResource(untagResourceRequestArgumentCaptor.capture());
        UntagResourceRequest expectedUntagResourceRequest = UntagResourceRequest.builder()
                .resourceArn(TEST_FLYWHEEL_ARN)
                .tagKeys(Arrays.asList("key3"))
                .build();
        assertUntagResourceRequestsEqual(expectedUntagResourceRequest, untagResourceRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_NoCallsToTagAPIs_WhenNoChangeInTags() {
        // Set up mock behavior
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(Translator.toSdkTags(TEST_RESOURCE_MODEL_UPDATED.getTags()))
                        .build());
        when(comprehendClient.updateFlywheel(any(UpdateFlywheelRequest.class)))
                .thenReturn(UpdateFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build());

        // Invoke Handler
        CallbackContext callbackContext = new CallbackContext();
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY, callbackContext, proxyClient, LOGGER, handler);

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

        verify(comprehendClient, times(2)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).updateFlywheel(updateFlywheelRequestArgumentCaptor.capture());
        EXPECTED_UPDATE_FLYWHEEL_REQUEST.equalsBySdkFields(updateFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).untagResource(any(UntagResourceRequest.class));
        verify(comprehendClient, times(0)).tagResource(any(TagResourceRequest.class));

        verify(comprehendClient, times(1)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenInternalServerException() {
        // Set up mock behavior
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(Translator.toSdkTags(TEST_RESOURCE_MODEL_UPDATED.getTags()))
                        .build());
        doThrow(INTERNAL_SERVER_EXCEPTION).when(comprehendClient).updateFlywheel(any(UpdateFlywheelRequest.class));

        // Invoke Handler
        ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.ServiceInternalError);
        assertThat(finalResponse.getMessage()).isEqualTo(INTERNAL_SERVER_EXCEPTION.getMessage());
        assertThat(finalResponse.getCallbackContext()).isNull();

        verify(comprehendClient, times(1)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).updateFlywheel(updateFlywheelRequestArgumentCaptor.capture());
        EXPECTED_UPDATE_FLYWHEEL_REQUEST.equalsBySdkFields(updateFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).untagResource(any(UntagResourceRequest.class));
        verify(comprehendClient, times(0)).tagResource(any(TagResourceRequest.class));
        verify(comprehendClient, times(0)).describeFlywheel(any(DescribeFlywheelRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenResourceNotFound() {
        // Set up mock behavior
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(RESOURCE_TAGS)
                        .build());
        when(comprehendClient.updateFlywheel(any(UpdateFlywheelRequest.class)))
                .thenReturn(UpdateFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build());
        doThrow(RESOURCE_NOT_FOUND_EXCEPTION).when(comprehendClient).untagResource(any(UntagResourceRequest.class));

        // Invoke Handler
        ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
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

        verify(comprehendClient, times(1)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).updateFlywheel(updateFlywheelRequestArgumentCaptor.capture());
        EXPECTED_UPDATE_FLYWHEEL_REQUEST.equalsBySdkFields(updateFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).untagResource(untagResourceRequestArgumentCaptor.capture());
        UntagResourceRequest expectedUntagResourceRequest = UntagResourceRequest.builder()
                .resourceArn(TEST_FLYWHEEL_ARN)
                .tagKeys(Arrays.asList("key3"))
                .build();
        assertUntagResourceRequestsEqual(expectedUntagResourceRequest, untagResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).tagResource(any(TagResourceRequest.class));
        verify(comprehendClient, times(0)).describeFlywheel(any(DescribeFlywheelRequest.class));
    }

    @Test
    public void handleRequest_RetriesServiceCall_WhenThrottled() {
        // Set up mock behavior
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(Translator.toSdkTags(TEST_RESOURCE_MODEL_UPDATED.getTags()))
                        .build());
        when(comprehendClient.updateFlywheel(any(UpdateFlywheelRequest.class)))
                .thenThrow(TOO_MANY_REQUESTS_EXCEPTION)
                .thenReturn(UpdateFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build());

        // Invoke Handler
        CallbackContext callbackContext = new CallbackContext();
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY, callbackContext, proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isEqualTo(TEST_RESOURCE_HANDLER_REQUEST_RESOURCE_TAGS_ONLY.getDesiredResourceState());
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isNull();
        assertThat(finalResponse.getErrorCode()).isNull();
        assertThat(finalResponse.getCallbackContext().getTagsToAdd()).isEmpty();
        assertThat(finalResponse.getCallbackContext().getTagKeysToRemove()).isEmpty();

        verify(comprehendClient, times(2)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(2)).updateFlywheel(updateFlywheelRequestArgumentCaptor.capture());
        EXPECTED_UPDATE_FLYWHEEL_REQUEST.equalsBySdkFields(updateFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).untagResource(any(UntagResourceRequest.class));
        verify(comprehendClient, times(0)).tagResource(any(TagResourceRequest.class));

        verify(comprehendClient, times(1)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());
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

        // Resource tags are updated, and stack tags are added
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(RESOURCE_TAGS_WITH_SYSTEM_TAGS)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(expectedTagsPostUntagging)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(UPDATED_USER_TAGS_WITH_SYSTEM_TAGS)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .resourceArn(TEST_FLYWHEEL_ARN)
                        .tags(UPDATED_USER_TAGS_WITH_UPDATED_SYSTEM_TAGS)
                        .build());
        when(comprehendClient.updateFlywheel(any(UpdateFlywheelRequest.class)))
                .thenReturn(UpdateFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
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

        verify(comprehendClient, times(5)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).updateFlywheel(updateFlywheelRequestArgumentCaptor.capture());
        EXPECTED_UPDATE_FLYWHEEL_REQUEST.equalsBySdkFields(updateFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).untagResource(untagResourceRequestArgumentCaptor.capture());
        UntagResourceRequest expectedUntagResourceRequest = UntagResourceRequest.builder()
                .resourceArn(TEST_FLYWHEEL_ARN)
                .tagKeys(expectedTagKeysToRemove)
                .build();
        assertUntagResourceRequestsEqual(expectedUntagResourceRequest, untagResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).tagResource(tagResourceRequestArgumentCaptor.capture());
        TagResourceRequest expectedTagResourceRequest = TagResourceRequest.builder()
                .resourceArn(TEST_FLYWHEEL_ARN)
                .tags(expectedTagsToAdd)
                .build();
        assertTagResourceRequestsEqual(expectedTagResourceRequest, tagResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());
    }

}
