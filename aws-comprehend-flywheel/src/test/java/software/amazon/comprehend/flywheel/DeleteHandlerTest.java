package software.amazon.comprehend.flywheel;

import java.time.Duration;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.CreateFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.CreateFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.DeleteFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DeleteFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<ComprehendClient> proxyClient;

    @Mock
    private ComprehendClient comprehendClient;

    @Captor
    private ArgumentCaptor<DeleteFlywheelRequest> deleteFlywheelRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<DescribeFlywheelRequest> describeFlywheelRequestArgumentCaptor;

    private DeleteHandler handler;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(LOGGER, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = MOCK_PROXY(proxy, comprehendClient);
        handler = new DeleteHandler();
    }

    @Test
    public void handleRequest_Success_WhenStabilizationSucceeds() {
        // Set up mock behavior
        when(comprehendClient.deleteFlywheel(any(DeleteFlywheelRequest.class)))
                .thenReturn(DeleteFlywheelResponse.builder().build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))  // Stabilization calls
                .thenReturn(DescribeFlywheelResponse.builder()  // First call active
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build())
                .thenReturn(DescribeFlywheelResponse.builder()  // Second call deleting
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_DELETING)
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

        verify(comprehendClient, times(1)).deleteFlywheel(deleteFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DELETE_FLYWHEEL_REQUEST.equalsBySdkFields(deleteFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenStabilizationFails() {
        // Set up mock behavior
        when(comprehendClient.deleteFlywheel(any(DeleteFlywheelRequest.class)))
                .thenReturn(DeleteFlywheelResponse.builder().build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))  // Stabilization calls
                .thenReturn(DescribeFlywheelResponse.builder()  // First call active
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build())
                .thenReturn(DescribeFlywheelResponse.builder()  // Second call deleting
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_DELETING)
                        .build())
                .thenReturn(DescribeFlywheelResponse.builder()  // Third call failed
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_FAILED)
                        .build());

        // Invoke Handler
        assertThrows(CfnGeneralServiceException.class,
                () -> invokeHandleRequestAndReturnFinalProgressEvent(
                        proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler));

        // Validate handler behavior
        verify(comprehendClient, times(1)).deleteFlywheel(deleteFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DELETE_FLYWHEEL_REQUEST.equalsBySdkFields(deleteFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenDescribeErrorsDuringStabilization() {
        // Set up mock behavior
        when(comprehendClient.deleteFlywheel(any(DeleteFlywheelRequest.class)))
                .thenReturn(DeleteFlywheelResponse.builder().build());
        doThrow(INTERNAL_SERVER_EXCEPTION).when(comprehendClient)
                .describeFlywheel(any(DescribeFlywheelRequest.class));

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
        verify(comprehendClient, times(1)).deleteFlywheel(deleteFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DELETE_FLYWHEEL_REQUEST.equalsBySdkFields(deleteFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenFlywheelInUnexpectedStateDuringStabilization() {
        // Set up mock behavior
        when(comprehendClient.deleteFlywheel(any(DeleteFlywheelRequest.class)))
                .thenReturn(DeleteFlywheelResponse.builder().build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))  // Stabilization calls
                .thenReturn(DescribeFlywheelResponse.builder()  // First call active
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build())
                .thenReturn(DescribeFlywheelResponse.builder()  // Second call deleting
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_DELETING)
                        .build())
                .thenReturn(DescribeFlywheelResponse.builder()  // Third call updating
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_CREATING)
                        .build());

        // Invoke Handler
        assertThrows(CfnNotStabilizedException.class,
                () -> invokeHandleRequestAndReturnFinalProgressEvent(
                        proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler));

        // Validate handler behavior
        verify(comprehendClient, times(1)).deleteFlywheel(deleteFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DELETE_FLYWHEEL_REQUEST.equalsBySdkFields(deleteFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenInternalServerException() {
        // Set up mock behavior
        doThrow(INTERNAL_SERVER_EXCEPTION).when(comprehendClient).deleteFlywheel(any(DeleteFlywheelRequest.class));

        // Invoke Handler
        ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.ServiceInternalError);
        assertThat(finalResponse.getMessage()).isEqualTo(INTERNAL_SERVER_EXCEPTION.getMessage());

        verify(comprehendClient, times(1)).deleteFlywheel(deleteFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DELETE_FLYWHEEL_REQUEST.equalsBySdkFields(deleteFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeFlywheel(any(DescribeFlywheelRequest.class));
    }

    /**
     * A delete handler MUST return FAILED with a NotFound error code if the resource didn't exist before the delete request.
     */
    @Test
    public void handleRequest_Fails_WhenResourceNotFound() {
        // Set up mock behavior
        doThrow(RESOURCE_NOT_FOUND_EXCEPTION).when(comprehendClient).deleteFlywheel(any(DeleteFlywheelRequest.class));

        // Invoke Handler
        ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.NotFound);
        assertThat(finalResponse.getMessage()).isEqualTo(RESOURCE_NOT_FOUND_EXCEPTION.getMessage());

        verify(comprehendClient, times(1)).deleteFlywheel(deleteFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DELETE_FLYWHEEL_REQUEST.equalsBySdkFields(deleteFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeFlywheel(any(DescribeFlywheelRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenFlywheelInUse() {
        // Set up mock behavior
        doThrow(RESOURCE_IN_USE_EXCEPTION).when(comprehendClient).deleteFlywheel(any(DeleteFlywheelRequest.class));

        // Invoke Handler
        ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.ResourceConflict);
        assertThat(finalResponse.getMessage()).isEqualTo(RESOURCE_IN_USE_EXCEPTION.getMessage());

        verify(comprehendClient, times(1)).deleteFlywheel(deleteFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DELETE_FLYWHEEL_REQUEST.equalsBySdkFields(deleteFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeFlywheel(any(DescribeFlywheelRequest.class));
    }

    @Test
    public void handleRequest_RetriesServiceCall_WhenThrottled() {
        // Set up mock behavior
        when(comprehendClient.deleteFlywheel(any(DeleteFlywheelRequest.class)))
                .thenThrow(TOO_MANY_REQUESTS_EXCEPTION)
                .thenReturn(DeleteFlywheelResponse.builder().build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))  // Stabilization calls
                .thenThrow(RESOURCE_NOT_FOUND_EXCEPTION);  // First call deleted

        final ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(finalResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(finalResponse.getResourceModel()).isNull();
        assertThat(finalResponse.getResourceModels()).isNull();
        assertThat(finalResponse.getMessage()).isNull();
        assertThat(finalResponse.getErrorCode()).isNull();

        verify(comprehendClient, times(2)).deleteFlywheel(deleteFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DELETE_FLYWHEEL_REQUEST.equalsBySdkFields(deleteFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());
    }

}
