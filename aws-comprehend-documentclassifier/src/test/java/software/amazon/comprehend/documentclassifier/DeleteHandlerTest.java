package software.amazon.comprehend.documentclassifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.DeleteDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DeleteDocumentClassifierResponse;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractModelTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<ComprehendClient> proxyClient;

    @Mock
    private ComprehendClient comprehendClient;

    @Captor
    private ArgumentCaptor<DeleteDocumentClassifierRequest> deleteDocumentClassifierRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<DescribeDocumentClassifierRequest> describeDocumentClassifierRequestArgumentCaptor;

    private DeleteHandler handler;

//    private final ResourceModel resourceModel = ResourceModel.builder()
//            .arn(TEST_DOCUMENT_CLASSIFIER_ARN)
//            .build();
//
//    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
//            .desiredResourceState(resourceModel)
//            .awsPartition(TEST_PARTITION)
//            .awsAccountId(TEST_ACCOUNT_ID)
//            .region(TEST_REGION)
//            .logicalResourceIdentifier(TEST_LOGICAL_RESOURCE_IDENTIFIER)
//            .build();

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(LOGGER, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = MOCK_PROXY(proxy, comprehendClient);
        handler = new DeleteHandler();
    }

    @Test
    public void handleRequest_Success_WhenStabilizationSucceeds() {
        // Set up mock behavior
        when(comprehendClient.deleteDocumentClassifier(any(DeleteDocumentClassifierRequest.class)))
                .thenReturn(DeleteDocumentClassifierResponse.builder().build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))  // Stabilization calls
                .thenReturn(DescribeDocumentClassifierResponse.builder()  // First call trained
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build())
                .thenReturn(DescribeDocumentClassifierResponse.builder()  // Second call deleting
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_DELETING)
                        .build())
                .thenThrow(RESOURCE_NOT_FOUND_EXCEPTION);  // Third call deleted

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isNull();
        assertThat(finalResponse.getErrorCode()).isNull();

        verify(comprehendClient, times(1)).deleteDocumentClassifier(deleteDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DELETE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(deleteDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenStabilizationFails() {
        // Set up mock behavior
        when(comprehendClient.deleteDocumentClassifier(any(DeleteDocumentClassifierRequest.class)))
                .thenReturn(DeleteDocumentClassifierResponse.builder().build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))  // Stabilization calls
                .thenReturn(DescribeDocumentClassifierResponse.builder()  // First call trained
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build())
                .thenReturn(DescribeDocumentClassifierResponse.builder()  // Second call deleting
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_DELETING)
                        .build())
                .thenReturn(DescribeDocumentClassifierResponse.builder()  // Third call failed
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_FAILED)
                        .build());

        // Invoke Handler
        assertThrows(CfnGeneralServiceException.class,
                () -> invokeHandleRequestAndReturnFinalProgressEvent(
                        proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler));

        // Validate handler behavior
        verify(comprehendClient, times(1)).deleteDocumentClassifier(deleteDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DELETE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(deleteDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenDescribeErrorsDuringStabilization() {
        // Set up mock behavior
        when(comprehendClient.deleteDocumentClassifier(any(DeleteDocumentClassifierRequest.class)))
                .thenReturn(DeleteDocumentClassifierResponse.builder().build());
        doThrow(INTERNAL_SERVER_EXCEPTION).when(comprehendClient)
                .describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isEqualTo(INTERNAL_SERVER_EXCEPTION.getMessage());
        assertThat(finalResponse.getErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);
        assertThat(finalResponse.getCallbackContext()).isNull();

        // Validate handler behavior
        verify(comprehendClient, times(1)).deleteDocumentClassifier(deleteDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DELETE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(deleteDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());
    }


    @Test
    public void handleRequest_Fails_WhenDocumentClassifierInUnexpectedStateDuringStabilization() {
        // Set up mock behavior
        when(comprehendClient.deleteDocumentClassifier(any(DeleteDocumentClassifierRequest.class)))
                .thenReturn(DeleteDocumentClassifierResponse.builder().build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))  // Stabilization calls
                .thenReturn(DescribeDocumentClassifierResponse.builder()  // First call trained
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINED)
                        .build())
                .thenReturn(DescribeDocumentClassifierResponse.builder()  // Second call deleting
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_DELETING)
                        .build())
                .thenReturn(DescribeDocumentClassifierResponse.builder()  // Third call failed
                        .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING)
                        .build());

        // Invoke Handler
        assertThrows(CfnNotStabilizedException.class,
                () -> invokeHandleRequestAndReturnFinalProgressEvent(
                        proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler));

        // Validate handler behavior
        verify(comprehendClient, times(1)).deleteDocumentClassifier(deleteDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DELETE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(deleteDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenInternalServerException() {
        // Set up mock behavior
        doThrow(INTERNAL_SERVER_EXCEPTION).when(comprehendClient).deleteDocumentClassifier(any(DeleteDocumentClassifierRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.ServiceInternalError);
        assertThat(finalResponse.getMessage()).isEqualTo(INTERNAL_SERVER_EXCEPTION.getMessage());

        verify(comprehendClient, times(1)).deleteDocumentClassifier(deleteDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DELETE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(deleteDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class));
    }

    /**
     * A delete handler MUST return FAILED with a NotFound error code if the resource didn't exist before the delete request.
     */
    @Test
    public void handleRequest_Fails_WhenResourceNotFound() {
        // Set up mock behavior
        doThrow(RESOURCE_NOT_FOUND_EXCEPTION).when(comprehendClient).deleteDocumentClassifier(any(DeleteDocumentClassifierRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.NotFound);
        assertThat(finalResponse.getMessage()).isEqualTo(RESOURCE_NOT_FOUND_EXCEPTION.getMessage());

        verify(comprehendClient, times(1)).deleteDocumentClassifier(deleteDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DELETE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(deleteDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenDocumentClassifierInUse() {
        // Set up mock behavior
        doThrow(RESOURCE_IN_USE_EXCEPTION).when(comprehendClient).deleteDocumentClassifier(any(DeleteDocumentClassifierRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.ResourceConflict);
        assertThat(finalResponse.getMessage()).isEqualTo(RESOURCE_IN_USE_EXCEPTION.getMessage());

        verify(comprehendClient, times(1)).deleteDocumentClassifier(deleteDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DELETE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(deleteDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class));
    }

    @Test
    public void handleRequest_RetriesServiceCall_WhenThrottled() {
        // Set up mock behavior
        when(comprehendClient.deleteDocumentClassifier(any(DeleteDocumentClassifierRequest.class)))
                .thenThrow(TOO_MANY_REQUESTS_EXCEPTION)
                .thenReturn(DeleteDocumentClassifierResponse.builder().build());
        when(comprehendClient.describeDocumentClassifier(any(DescribeDocumentClassifierRequest.class)))  // Stabilization calls
                .thenThrow(RESOURCE_NOT_FOUND_EXCEPTION);  // First call deleted

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isNull();
        assertThat(finalResponse.getErrorCode()).isNull();

        // Validate handler behavior
        verify(comprehendClient, times(2)).deleteDocumentClassifier(deleteDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DELETE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(deleteDocumentClassifierRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeDocumentClassifier(describeDocumentClassifierRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_DOCUMENT_CLASSIFIER_REQUEST.equalsBySdkFields(describeDocumentClassifierRequestArgumentCaptor.getValue());
    }

}
