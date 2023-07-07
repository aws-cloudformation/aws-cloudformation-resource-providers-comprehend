package software.amazon.comprehend.documentclassifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierResponse;
import software.amazon.awssdk.services.comprehend.model.DescribeResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeResourcePolicyResponse;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ReadHandlerTest extends AbstractModelTestBase {
    private final ResourceModel resourceModel = ResourceModel.builder()
            .arn(TEST_DOCUMENT_CLASSIFIER_ARN)
            .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .clientRequestToken(TEST_CLIENT_REQUEST_TOKEN)
            .desiredResourceState(resourceModel)
            .awsPartition(TEST_PARTITION)
            .awsAccountId(TEST_ACCOUNT_ID)
            .region(TEST_REGION)
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
    ArgumentCaptor<ListTagsForResourceRequest> listTagsForResourceRequestArgumentCaptor;

    private ReadHandler handler;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(LOGGER, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = MOCK_PROXY(proxy, comprehendClient);
        handler = new ReadHandler();
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        // Set up mock behavior
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build());
        when(comprehendClient.describeResourcePolicy(any(DescribeResourcePolicyRequest.class)))
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY)
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(RESOURCE_TAGS)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> response = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);;

        // Validate handler behavior
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TEST_RESOURCE_HANDLER_REQUEST.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());
    }

    /**
     * A read handler MUST return FAILED with a NotFound error code if the resource doesn't exist.
     */
    @Test
    public void handleRequest_Fail_WhenResourceNotFound() {
        // Set up mock behavior
        doThrow(RESOURCE_NOT_FOUND_EXCEPTION)
                .when(comprehendClient).describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> response = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);;

        // Validate handler behavior
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(RESOURCE_NOT_FOUND_EXCEPTION.getMessage());
        assertThat(response.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.NotFound);

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeResourcePolicy(any(DescribeResourcePolicyRequest.class));
        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    /**
     * A read handler MUST return FAILED with a NotFound error code if the resource doesn't exist.
     */
    @Test
    public void handleRequest_Fail_WhenResourceNotFoundDuringDescribeResourcePolicy() {
        // Set up mock behavior
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build());
        doThrow(RESOURCE_NOT_FOUND_EXCEPTION)
                .when(comprehendClient).describeResourcePolicy(any(DescribeResourcePolicyRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> response = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);;

        // Validate handler behavior
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(RESOURCE_NOT_FOUND_EXCEPTION.getMessage());
        assertThat(response.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.NotFound);

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    /**
     * A read handler MUST return FAILED with a NotFound error code if the resource doesn't exist.
     */
    @Test
    public void handleRequest_Fail_WhenTagResourceNotFound() {
        // Set up mock behavior
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build());
        when(comprehendClient.describeResourcePolicy(any(DescribeResourcePolicyRequest.class)))
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY)
                        .build());
        doThrow(RESOURCE_NOT_FOUND_EXCEPTION)
                .when(comprehendClient).listTagsForResource(any(ListTagsForResourceRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> response = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);;

        // Validate handler behavior
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(RESOURCE_NOT_FOUND_EXCEPTION.getMessage());
        assertThat(response.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.NotFound);

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());
    }
}
