package software.amazon.comprehend.documentclassifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.CreateDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.CreateDocumentClassifierResponse;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierResponse;
import software.amazon.awssdk.services.comprehend.model.DescribeResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeResourcePolicyResponse;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractModelTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<ComprehendClient> proxyClient;

    @Mock
    private ComprehendClient comprehendClient;

    @Captor
    private ArgumentCaptor<CreateDocumentClassifierRequest> createDocumentClassifierRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<DescribeDocumentClassifierRequest> describeDocumentClassifierRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<DescribeResourcePolicyRequest> describeResourcePolicyRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<ListTagsForResourceRequest> listTagsForResourceRequestArgumentCaptor;

    private CreateHandler handler;

    private CallbackContext callbackContext;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(LOGGER, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = MOCK_PROXY(proxy, comprehendClient);
        handler = new CreateHandler();
        callbackContext = new CallbackContext();
    }

    @Test
    public void handleRequest_Success_WhenStabilizationSucceeds() {
        // Set up mock behavior
        when(comprehendClient.createDocumentClassifier(any(CreateDocumentClassifierRequest.class)))
                .thenReturn(CreateDocumentClassifierResponse.builder()
                        .documentClassifierArn(TEST_DOCUMENT_CLASSIFIER_ARN)
                        .build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING)
                        .build())
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(Collections.emptyList())
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(USER_TAGS_WITH_SYSTEM_TAGS)
                        .build());
        when(comprehendClient.describeResourcePolicy(any(DescribeResourcePolicyRequest.class)))
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isEqualTo(TEST_RESOURCE_MODEL_WITH_SYSTEM_TAGS);
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isNull();
        assertThat(finalResponse.getErrorCode()).isNull();
        assertThat(finalResponse.getCallbackContext().getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_ARN);
        assertEquals(finalResponse.getCallbackContext().getTagsToAdd(),
                new HashSet<>(Translator.toSdkTags(TEST_RESOURCE_MODEL_WITH_SYSTEM_TAGS.getTags())));
        assertThat(finalResponse.getCallbackContext().getTagKeysToRemove()).isNull();

        verify(comprehendClient, times(1)).createDocumentClassifier(createDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_CREATE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(createDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenStabilizationFails() {
        // Set up mock behavior
        when(comprehendClient.createDocumentClassifier(any(CreateDocumentClassifierRequest.class)))
                .thenReturn(CreateDocumentClassifierResponse.builder()
                        .documentClassifierArn(TEST_DOCUMENT_CLASSIFIER_ARN)
                        .build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING)
                        .build())
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING)
                        .build())
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_FAILED)
                        .build());

        // Invoke Handler
        assertThrows(CfnGeneralServiceException.class,
                () -> invokeHandleRequestAndReturnFinalProgressEvent(
                        proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler));

        // Validate handler behavior
        assertThat(callbackContext.getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_ARN);

        verify(comprehendClient, times(1)).createDocumentClassifier(createDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_CREATE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(createDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(comprehendClient, times(0)).describeResourcePolicy(any(DescribeResourcePolicyRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenDescribeErrorsDuringStabilization() {
        // Set up mock behavior
        when(comprehendClient.createDocumentClassifier(any(CreateDocumentClassifierRequest.class)))
                .thenReturn(CreateDocumentClassifierResponse.builder()
                        .documentClassifierArn(TEST_DOCUMENT_CLASSIFIER_ARN)
                        .build());
        doThrow(RESOURCE_NOT_FOUND_EXCEPTION).when(comprehendClient)
                .describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isEqualTo(RESOURCE_NOT_FOUND_EXCEPTION.getMessage());
        assertThat(finalResponse.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
        assertThat(finalResponse.getCallbackContext()).isNull();

        assertThat(callbackContext.getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_ARN);

        verify(comprehendClient, times(1)).createDocumentClassifier(createDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_CREATE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(createDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(comprehendClient, times(0)).describeResourcePolicy(any(DescribeResourcePolicyRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenListTagsErrorsDuringStabilization() {
        // Set up mock behavior
        when(comprehendClient.createDocumentClassifier(any(CreateDocumentClassifierRequest.class)))
                .thenReturn(CreateDocumentClassifierResponse.builder()
                        .documentClassifierArn(TEST_DOCUMENT_CLASSIFIER_ARN)
                        .build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build());
        doThrow(INTERNAL_SERVER_EXCEPTION).when(comprehendClient)
                .listTagsForResource(any(ListTagsForResourceRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isEqualTo(INTERNAL_SERVER_EXCEPTION.getMessage());
        assertThat(finalResponse.getErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);
        assertThat(finalResponse.getCallbackContext()).isNull();

        assertThat(callbackContext.getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_ARN);

        verify(comprehendClient, times(1)).createDocumentClassifier(createDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_CREATE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(createDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeResourcePolicy(any(DescribeResourcePolicyRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenDocumentClassifierInUnexpectedStateDuringStabilization() {
        // Set up mock behavior
        when(comprehendClient.createDocumentClassifier(any(CreateDocumentClassifierRequest.class)))
                .thenReturn(CreateDocumentClassifierResponse.builder()
                        .documentClassifierArn(TEST_DOCUMENT_CLASSIFIER_ARN)
                        .build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING)
                        .build())
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING)
                        .build())
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_DELETING)
                        .build());

        // Invoke Handler
        assertThrows(CfnNotStabilizedException.class,
                () -> invokeHandleRequestAndReturnFinalProgressEvent(
                        proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler));

        // Validate handler behavior
        assertThat(callbackContext.getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_ARN);

        verify(comprehendClient, times(1)).createDocumentClassifier(createDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_CREATE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(createDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(comprehendClient, times(0)).describeResourcePolicy(any(DescribeResourcePolicyRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenDocumentClassifierAlreadyExists() {
        // Set up mock behavior
        doThrow(RESOURCE_IN_USE_EXCEPTION).when(comprehendClient).createDocumentClassifier(any(CreateDocumentClassifierRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.AlreadyExists);
        assertThat(finalResponse.getMessage()).isEqualTo(RESOURCE_IN_USE_EXCEPTION.getMessage());
        assertThat(finalResponse.getCallbackContext()).isNull();

        verify(comprehendClient, times(1)).createDocumentClassifier(createDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_CREATE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(createDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class));
        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(comprehendClient, times(0)).describeResourcePolicy(any(DescribeResourcePolicyRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenResourceLimitExceeded() {
        // Set up mock behavior
        doThrow(RESOURCE_LIMIT_EXCEEDED_EXCEPTION).when(comprehendClient).createDocumentClassifier(any(CreateDocumentClassifierRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.InvalidRequest);
        assertThat(finalResponse.getMessage()).isEqualTo(RESOURCE_LIMIT_EXCEEDED_EXCEPTION.getMessage());
        assertThat(finalResponse.getCallbackContext()).isNull();

        verify(comprehendClient, times(1)).createDocumentClassifier(createDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_CREATE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(createDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class));
        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(comprehendClient, times(0)).describeResourcePolicy(any(DescribeResourcePolicyRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenInternalServerException() {
        // Set up mock behavior
        doThrow(INTERNAL_SERVER_EXCEPTION).when(comprehendClient).createDocumentClassifier(any(CreateDocumentClassifierRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse).isNotNull();
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.ServiceInternalError);
        assertThat(finalResponse.getMessage()).isEqualTo(INTERNAL_SERVER_EXCEPTION.getMessage());
        assertThat(finalResponse.getCallbackContext()).isNull();

        verify(comprehendClient, times(1)).createDocumentClassifier(createDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_CREATE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(createDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class));
        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(comprehendClient, times(0)).describeResourcePolicy(any(DescribeResourcePolicyRequest.class));
    }

    @Test
    public void handleRequest_RetriesServiceCall_WhenThrottled() {
        // Set up mock behavior
        when(comprehendClient.createDocumentClassifier(any(CreateDocumentClassifierRequest.class)))
                .thenReturn(CreateDocumentClassifierResponse.builder()
                        .documentClassifierArn(TEST_DOCUMENT_CLASSIFIER_ARN)
                        .build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING)
                        .build())
                .thenReturn(DescribeDocumentClassifierResponse.builder()
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenThrow(TOO_MANY_REQUESTS_EXCEPTION)
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(Collections.emptyList())
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(USER_TAGS_WITH_SYSTEM_TAGS)
                        .build());
        when(comprehendClient.describeResourcePolicy(any(DescribeResourcePolicyRequest.class)))
                .thenReturn(DescribeResourcePolicyResponse.builder()
                        .resourcePolicy(TEST_MODEL_POLICY)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isEqualTo(TEST_RESOURCE_MODEL_WITH_SYSTEM_TAGS);
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isNull();
        assertThat(finalResponse.getErrorCode()).isNull();
        assertThat(finalResponse.getCallbackContext().getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_ARN);
        assertEquals(finalResponse.getCallbackContext().getTagsToAdd(),
                new HashSet<>(Translator.toSdkTags(TEST_RESOURCE_MODEL_WITH_SYSTEM_TAGS.getTags())));
        assertThat(finalResponse.getCallbackContext().getTagKeysToRemove()).isNull();

        verify(comprehendClient, times(1)).createDocumentClassifier(createDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_CREATE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(createDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(4)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeResourcePolicy(describeResourcePolicyRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_RESOURCE_POLICY_REQUEST.equalsBySdkFields(describeResourcePolicyRequestArgumentCaptor.getValue());
    }

}
