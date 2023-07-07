package software.amazon.comprehend.documentclassifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.ListDocumentClassifiersRequest;
import software.amazon.awssdk.services.comprehend.model.ListDocumentClassifiersResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractModelTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<ComprehendClient> proxyClient;

    @Mock
    private ComprehendClient comprehendClient;

    @Captor
    ArgumentCaptor<ListDocumentClassifiersRequest> listDocumentClassifiersRequestArgumentCaptor;

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
        when(comprehendClient.listDocumentClassifiers(any(ListDocumentClassifiersRequest.class)))
                .thenReturn(ListDocumentClassifiersResponse.builder()
                        .documentClassifierPropertiesList(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_LIST)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> response = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();

        int expectedNumberOfModels = 5;
        assertThat(response.getResourceModels().size()).isEqualTo(expectedNumberOfModels);
        for (int i = 0; i < expectedNumberOfModels; i++) {
            assertThat(response.getResourceModels().get(i).getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_LIST.get(i).documentClassifierArn());
        }

        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(comprehendClient, times(1)).listDocumentClassifiers(listDocumentClassifiersRequestArgumentCaptor.capture());
        EXPECTED_LIST_DOCUMENT_CLASSIFIERS_REQUEST.equalsBySdkFields(listDocumentClassifiersRequestArgumentCaptor.getValue());
    }

    /**
     * A list handler MUST return an array of primary identifiers.
     */
    @Test
    public void handleRequest_SimpleSuccess_WithNextToken() {
        // Set up mock behavior
        when(comprehendClient.listDocumentClassifiers(any(ListDocumentClassifiersRequest.class)))
                .thenReturn(ListDocumentClassifiersResponse.builder()
                        .documentClassifierPropertiesList(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_LIST)
                        .build());

        // Invoke Handler
        final ProgressEvent<ResourceModel, CallbackContext> response = invokeHandleRequestAndReturnFinalProgressEvent(
                proxy, TEST_RESOURCE_HANDLER_REQUEST_WITH_NEXT_TOKEN, new CallbackContext(), proxyClient, LOGGER, handler);

        // Validate handler behavior
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();

        int expectedNumberOfModels = 5;
        assertThat(response.getResourceModels().size()).isEqualTo(expectedNumberOfModels);
        for (int i = 0; i < expectedNumberOfModels; i++) {
            assertThat(response.getResourceModels().get(i).getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_LIST.get(i).documentClassifierArn());
        }

        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(comprehendClient, times(1)).listDocumentClassifiers(listDocumentClassifiersRequestArgumentCaptor.capture());
        EXPECTED_LIST_DOCUMENT_CLASSIFIERS_REQUEST_WITH_NEXT_TOKEN.equalsBySdkFields(listDocumentClassifiersRequestArgumentCaptor.getValue());
    }

    /**
     * A list request MUST return an empty array if there are no resources found.
     */
    @Test
    public void handleRequest_Success_WhenNoDocumentClassifiersReturned() {
        // Set up mock behavior
        when(comprehendClient.listDocumentClassifiers(any(ListDocumentClassifiersRequest.class)))
                .thenReturn(ListDocumentClassifiersResponse.builder()
                        .documentClassifierPropertiesList(TEST_EMPTY_DOCUMENT_CLASSIFIER_PROPERTIES_LIST)
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

        verify(comprehendClient, times(1)).listDocumentClassifiers(listDocumentClassifiersRequestArgumentCaptor.capture());
        EXPECTED_LIST_DOCUMENT_CLASSIFIERS_REQUEST_WITH_NEXT_TOKEN.equalsBySdkFields(listDocumentClassifiersRequestArgumentCaptor.getValue());
    }

}
