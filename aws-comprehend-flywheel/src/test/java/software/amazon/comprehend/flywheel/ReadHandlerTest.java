package software.amazon.comprehend.flywheel;

import java.time.Duration;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceResponse;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;


@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<ComprehendClient> proxyClient;

    @Mock
    private ComprehendClient comprehendClient;

    @Captor
    ArgumentCaptor<DescribeFlywheelRequest> describeFlywheelRequestArgumentCaptor;

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
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_TRANSFORMED_DATALAKE_S3_URI)
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

        verify(comprehendClient, times(1)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_SimpleSuccess_ER() {
        // Set up mock behavior
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE_ER)
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(RESOURCE_TAGS)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> response = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_ER, new CallbackContext(), proxyClient, LOGGER, handler);;

        // Validate handler behavior
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(TEST_RESOURCE_HANDLER_REQUEST_ER.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(comprehendClient, times(1)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());

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
                .when(comprehendClient).describeFlywheel(any(DescribeFlywheelRequest.class));

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> response = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);;

        // Validate handler behavior
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getMessage()).isEqualTo(RESOURCE_NOT_FOUND_EXCEPTION.getMessage());
        assertThat(response.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.NotFound);

        verify(comprehendClient, times(1)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    /**
     * A read handler MUST return FAILED with a NotFound error code if the resource doesn't exist.
     */
    @Test
    public void handleRequest_Fail_WhenTagResourceNotFound() {
        // Set up mock behavior
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
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

        verify(comprehendClient, times(1)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());
    }

}
