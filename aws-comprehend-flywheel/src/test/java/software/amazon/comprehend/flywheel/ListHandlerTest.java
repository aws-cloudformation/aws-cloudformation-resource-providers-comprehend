package software.amazon.comprehend.flywheel;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.DeleteFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.ListFlywheelsRequest;
import software.amazon.awssdk.services.comprehend.model.ListFlywheelsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<ComprehendClient> proxyClient;

    @Mock
    private ComprehendClient comprehendClient;

    @Captor
    ArgumentCaptor<ListFlywheelsRequest> listFlywheelsRequestArgumentCaptor;

    private ListHandler handler;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(LOGGER, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = MOCK_PROXY(proxy, comprehendClient);
        handler = new ListHandler();
    }

    /**
     * A list handler MUST return an array of primary identifiers.
     */
    @Test
    public void handleRequest_SimpleSuccess() {
        // Set up mock behavior
        when(comprehendClient.listFlywheels(any(ListFlywheelsRequest.class)))
                .thenReturn(ListFlywheelsResponse.builder()
                        .flywheelSummaryList(TEST_FLYWHEEL_SUMMARY_LIST)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> response = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();

        assertThat(response.getResourceModels().size()).isEqualTo(2);
        assertThat(response.getResourceModels().get(0).getArn()).isEqualTo(TEST_FLYWHEEL_SUMMARY.flywheelArn());
        assertThat(response.getResourceModels().get(1).getArn()).isEqualTo(TEST_FLYWHEEL_SUMMARY_2.flywheelArn());

        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(comprehendClient, times(1)).listFlywheels(listFlywheelsRequestArgumentCaptor.capture());

        EXPECTED_LIST_FLYWHEEL_REQUEST.equalsBySdkFields(listFlywheelsRequestArgumentCaptor.getValue());
    }

    /**
     * A list handler MUST return an array of primary identifiers.
     */
    @Test
    public void handleRequest_SimpleSuccess_WithNextToken() {
        // Set up mock behavior
        when(comprehendClient.listFlywheels(any(ListFlywheelsRequest.class)))
                .thenReturn(ListFlywheelsResponse.builder()
                        .flywheelSummaryList(TEST_FLYWHEEL_SUMMARY_LIST)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> response = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_WITH_NEXT_TOKEN, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();

        assertThat(response.getResourceModels().size()).isEqualTo(2);
        assertThat(response.getResourceModels().get(0).getArn()).isEqualTo(TEST_FLYWHEEL_SUMMARY.flywheelArn());
        assertThat(response.getResourceModels().get(1).getArn()).isEqualTo(TEST_FLYWHEEL_SUMMARY_2.flywheelArn());

        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(comprehendClient, times(1)).listFlywheels(listFlywheelsRequestArgumentCaptor.capture());
        EXPECTED_LIST_FLYWHEEL_REQUEST_WITH_NEXT_TOKEN.equalsBySdkFields(listFlywheelsRequestArgumentCaptor.getValue());
    }

    /**
     * A list request MUST return an empty array if there are no resources found.
     */
    @Test
    public void handleRequest_Success_WhenNoFlywheelsReturned() {
        // Set up mock behavior
        when(comprehendClient.listFlywheels(any(ListFlywheelsRequest.class)))
                .thenReturn(ListFlywheelsResponse.builder()
                        .flywheelSummaryList(TEST_EMPTY_FLYWHEEL_SUMMARY_LIST)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> response = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels().size()).isEqualTo(0);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(comprehendClient, times(1)).listFlywheels(listFlywheelsRequestArgumentCaptor.capture());
        EXPECTED_LIST_FLYWHEEL_REQUEST_WITH_NEXT_TOKEN.equalsBySdkFields(listFlywheelsRequestArgumentCaptor.getValue());
    }

}
