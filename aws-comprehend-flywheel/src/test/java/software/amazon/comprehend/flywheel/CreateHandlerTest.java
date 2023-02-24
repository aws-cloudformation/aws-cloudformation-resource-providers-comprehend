package software.amazon.comprehend.flywheel;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.CreateFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.CreateFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<ComprehendClient> proxyClient;

    @Mock
    private ComprehendClient comprehendClient;

    @Captor
    private ArgumentCaptor<CreateFlywheelRequest> createFlywheelRequestArgumentCaptor;

    @Captor
    private ArgumentCaptor<DescribeFlywheelRequest> describeFlywheelRequestArgumentCaptor;

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
        when(comprehendClient.createFlywheel(any(CreateFlywheelRequest.class)))
                .thenReturn(CreateFlywheelResponse.builder()
                        .flywheelArn(TEST_FLYWHEEL_ARN)
                        .build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_CREATING)
                        .build())
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(Collections.emptyList())
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(USER_TAGS_WITH_SYSTEM_TAGS)
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
        assertThat(finalResponse.getCallbackContext().getFlywheelArn()).isEqualTo(TEST_FLYWHEEL_ARN);
        assertEquals(finalResponse.getCallbackContext().getTagsToAdd(),
                new HashSet<>(Translator.toSdkTags(TEST_RESOURCE_MODEL_WITH_SYSTEM_TAGS.getTags())));
        assertThat(finalResponse.getCallbackContext().getTagKeysToRemove()).isNull();

        verify(comprehendClient, times(1)).createFlywheel(createFlywheelRequestArgumentCaptor.capture());
        EXPECTED_CREATE_FLYWHEEL_REQUEST.equalsBySdkFields(createFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenStabilizationFails() {
        // Set up mock behavior
        when(comprehendClient.createFlywheel(any(CreateFlywheelRequest.class)))
                .thenReturn(CreateFlywheelResponse.builder()
                        .flywheelArn(TEST_FLYWHEEL_ARN)
                        .build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_CREATING)
                        .build())
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_CREATING)
                        .build())
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_FAILED)
                        .build());

        // Invoke Handler
        assertThrows(CfnGeneralServiceException.class,
                () -> invokeHandleRequestAndReturnFinalProgressEvent(
                        proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler));

        // Validate handler behavior
        assertThat(callbackContext.getFlywheelArn()).isEqualTo(TEST_FLYWHEEL_ARN);

        verify(comprehendClient, times(1)).createFlywheel(createFlywheelRequestArgumentCaptor.capture()); // Using eq() argument matcher didn't work
        EXPECTED_CREATE_FLYWHEEL_REQUEST.equalsBySdkFields(createFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenDescribeErrorsDuringStabilization() {
        // Set up mock behavior
        when(comprehendClient.createFlywheel(any(CreateFlywheelRequest.class)))
                .thenReturn(CreateFlywheelResponse.builder()
                        .flywheelArn(TEST_FLYWHEEL_ARN)
                        .build());
        doThrow(RESOURCE_NOT_FOUND_EXCEPTION).when(comprehendClient)
                .describeFlywheel(any(DescribeFlywheelRequest.class));

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

        assertThat(callbackContext.getFlywheelArn()).isEqualTo(TEST_FLYWHEEL_ARN);

        verify(comprehendClient, times(1)).createFlywheel(createFlywheelRequestArgumentCaptor.capture()); // Using eq() argument matcher didn't work
        EXPECTED_CREATE_FLYWHEEL_REQUEST.equalsBySdkFields(createFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenListTagsErrorsDuringStabilization() {
        // Set up mock behavior
        when(comprehendClient.createFlywheel(any(CreateFlywheelRequest.class)))
                .thenReturn(CreateFlywheelResponse.builder()
                        .flywheelArn(TEST_FLYWHEEL_ARN)
                        .build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
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

        assertThat(callbackContext.getFlywheelArn()).isEqualTo(TEST_FLYWHEEL_ARN);

        verify(comprehendClient, times(1)).createFlywheel(createFlywheelRequestArgumentCaptor.capture()); // Using eq() argument matcher didn't work
        EXPECTED_CREATE_FLYWHEEL_REQUEST.equalsBySdkFields(createFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(1)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());
    }

    @Test
    public void handleRequest_Fails_WhenFlywheelInUnexpectedStateDuringStabilization() {
        // Set up mock behavior
        when(comprehendClient.createFlywheel(any(CreateFlywheelRequest.class)))
                .thenReturn(CreateFlywheelResponse.builder()
                        .flywheelArn(TEST_FLYWHEEL_ARN)
                        .build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))  // Stabilization calls
                .thenReturn(DescribeFlywheelResponse.builder()  // First call active
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_CREATING)
                        .build())
                .thenReturn(DescribeFlywheelResponse.builder()  // Second call deleting
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_CREATING)
                        .build())
                .thenReturn(DescribeFlywheelResponse.builder()  // Third call updating
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_DELETING)
                        .build());

        // Invoke Handler
        assertThrows(CfnNotStabilizedException.class,
                () -> invokeHandleRequestAndReturnFinalProgressEvent(
                        proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler));

        // Validate handler behavior
        assertThat(callbackContext.getFlywheelArn()).isEqualTo(TEST_FLYWHEEL_ARN);

        verify(comprehendClient, times(1)).createFlywheel(createFlywheelRequestArgumentCaptor.capture()); // Using eq() argument matcher didn't work
        EXPECTED_CREATE_FLYWHEEL_REQUEST.equalsBySdkFields(createFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenFlywheelAlreadyExists() {
        // Set up mock behavior
        doThrow(RESOURCE_IN_USE_EXCEPTION).when(comprehendClient).createFlywheel(any(CreateFlywheelRequest.class));

        // Invoke Handler
        ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                        proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.AlreadyExists);
        assertThat(finalResponse.getMessage()).isEqualTo(RESOURCE_IN_USE_EXCEPTION.getMessage());
        assertThat(finalResponse.getCallbackContext()).isNull();

        verify(comprehendClient, times(1)).createFlywheel(createFlywheelRequestArgumentCaptor.capture()); // Using eq() argument matcher didn't work
        EXPECTED_CREATE_FLYWHEEL_REQUEST.equalsBySdkFields(createFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeFlywheel(any(DescribeFlywheelRequest.class));
        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenResourceLimitExceeded() {
        // Set up mock behavior
        doThrow(RESOURCE_LIMIT_EXCEEDED_EXCEPTION).when(comprehendClient).createFlywheel(any(CreateFlywheelRequest.class));

        // Invoke Handler
        ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.InvalidRequest);
        assertThat(finalResponse.getMessage()).isEqualTo(RESOURCE_LIMIT_EXCEEDED_EXCEPTION.getMessage());
        assertThat(finalResponse.getCallbackContext()).isNull();

        verify(comprehendClient, times(1)).createFlywheel(createFlywheelRequestArgumentCaptor.capture()); // Using eq() argument matcher didn't work
        EXPECTED_CREATE_FLYWHEEL_REQUEST.equalsBySdkFields(createFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeFlywheel(any(DescribeFlywheelRequest.class));
        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_Fails_WhenInternalServerException() {
        // Set up mock behavior
        doThrow(INTERNAL_SERVER_EXCEPTION).when(comprehendClient).createFlywheel(any(CreateFlywheelRequest.class));

        // Invoke Handler
        ProgressEvent<ResourceModel, CallbackContext> finalResponse = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_NO_ARN, callbackContext, proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(finalResponse).isNotNull();
        assertThat(finalResponse.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(finalResponse.getErrorCode()).isEqualByComparingTo(HandlerErrorCode.ServiceInternalError);
        assertThat(finalResponse.getMessage()).isEqualTo(INTERNAL_SERVER_EXCEPTION.getMessage());
        assertThat(finalResponse.getCallbackContext()).isNull();

        verify(comprehendClient, times(1)).createFlywheel(createFlywheelRequestArgumentCaptor.capture()); // Using eq() argument matcher didn't work
        EXPECTED_CREATE_FLYWHEEL_REQUEST.equalsBySdkFields(createFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(0)).describeFlywheel(any(DescribeFlywheelRequest.class));
        verify(comprehendClient, times(0)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_RetriesServiceCall_WhenThrottled() {
        // Set up mock behavior
        when(comprehendClient.createFlywheel(any(CreateFlywheelRequest.class)))
                .thenReturn(CreateFlywheelResponse.builder()
                        .flywheelArn(TEST_FLYWHEEL_ARN)
                        .build());
        when(comprehendClient.describeFlywheel(any(DescribeFlywheelRequest.class)))
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_CREATING)
                        .build())
                .thenReturn(DescribeFlywheelResponse.builder()
                        .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_ACTIVE)
                        .build());
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenThrow(TOO_MANY_REQUESTS_EXCEPTION)
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(Collections.emptyList())
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(USER_TAGS_WITH_SYSTEM_TAGS)
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
        assertThat(finalResponse.getCallbackContext().getFlywheelArn()).isEqualTo(TEST_FLYWHEEL_ARN);
        assertEquals(finalResponse.getCallbackContext().getTagsToAdd(),
                new HashSet<>(Translator.toSdkTags(TEST_RESOURCE_MODEL_WITH_SYSTEM_TAGS.getTags())));
        assertThat(finalResponse.getCallbackContext().getTagKeysToRemove()).isNull();

        verify(comprehendClient, times(1)).createFlywheel(createFlywheelRequestArgumentCaptor.capture()); // Using eq() argument matcher didn't work
        EXPECTED_CREATE_FLYWHEEL_REQUEST.equalsBySdkFields(createFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(3)).describeFlywheel(describeFlywheelRequestArgumentCaptor.capture());
        EXPECTED_DESCRIBE_FLYWHEEL_REQUEST.equalsBySdkFields(describeFlywheelRequestArgumentCaptor.getValue());

        verify(comprehendClient, times(4)).listTagsForResource(listTagsForResourceRequestArgumentCaptor.capture());
        EXPECTED_LIST_TAGS_FOR_RESOURCE_REQUEST.equalsBySdkFields(listTagsForResourceRequestArgumentCaptor.getValue());
    }

}
