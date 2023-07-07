package software.amazon.comprehend.documentclassifier;

import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.ListDocumentClassifiersRequest;
import software.amazon.awssdk.services.comprehend.model.ListDocumentClassifiersResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ListHandler extends AbstractModelHandler {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<ComprehendClient> proxyClient,
            final Logger logger) {

        this.logger = logger;

        ListDocumentClassifiersRequest listDocumentClassifiersRequest = Translator.translateToListRequest(request.getNextToken());
        ListDocumentClassifiersResponse listDocumentClassifierResponse = proxy.injectCredentialsAndInvokeV2(listDocumentClassifiersRequest, proxyClient.client()::listDocumentClassifiers);

        logger.log(String.format("Successfully listed %d document classifiers.", listDocumentClassifierResponse.documentClassifierPropertiesList().size()));
        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(Translator.translateFromListResponse(listDocumentClassifierResponse))
                .nextToken(listDocumentClassifierResponse.nextToken())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
