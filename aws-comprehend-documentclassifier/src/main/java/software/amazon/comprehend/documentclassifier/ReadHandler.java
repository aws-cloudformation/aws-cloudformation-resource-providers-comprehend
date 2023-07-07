package software.amazon.comprehend.documentclassifier;

import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.*;
import software.amazon.cloudformation.proxy.*;

public class ReadHandler extends AbstractModelHandler {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                       final ResourceHandlerRequest<ResourceModel> request,
                                                                       final CallbackContext callbackContext,
                                                                       final ProxyClient<ComprehendClient> proxyClient,
                                                                       final Logger logger) {

        this.logger = logger;

        final ResourceModel documentClassifierModel = request.getDesiredResourceState();

        DescribeDocumentClassifierRequest describeDocumentClassifierRequest = Translator.translateToReadRequest(documentClassifierModel);
        ListTagsForResourceRequest listTagsForResourceRequest = Translator.translateToListTagsRequest(documentClassifierModel);
        DescribeResourcePolicyRequest describeResourcePolicyRequest = Translator.translateToDescribeResourcePolicyRequest(documentClassifierModel);

        try {
            DescribeDocumentClassifierResponse describeDocumentClassifierResponse = proxy.injectCredentialsAndInvokeV2(describeDocumentClassifierRequest,
                    proxyClient.client()::describeDocumentClassifier);
            logger.log(String.format("Successfully described document classifier [%s].", documentClassifierModel.getArn()));

            DescribeResourcePolicyResponse describeResourcePolicyResponse = proxy.injectCredentialsAndInvokeV2(describeResourcePolicyRequest,
                    proxyClient.client()::describeResourcePolicy);
            logger.log(String.format("Successfully described resource policy for document classifier [%s].", documentClassifierModel.getArn()));

            ListTagsForResourceResponse listTagsForResourceResponse = proxy.injectCredentialsAndInvokeV2(listTagsForResourceRequest,
                    proxyClient.client()::listTagsForResource);
            logger.log(String.format("Successfully listed %d tags for document classifier [%s].",
                    listTagsForResourceResponse.tags().size(), documentClassifierModel.getArn()));

            ResourceModel newModel = Translator.translateFromReadResponse(describeDocumentClassifierResponse);
            newModel.setModelPolicy(describeResourcePolicyResponse.resourcePolicy());
            newModel.setTags(Translator.fromSdkTags(listTagsForResourceResponse.tags()));

            return ProgressEvent.success(newModel, callbackContext);
        } catch (final ResourceNotFoundException e) {
            logger.log(String.format("Read operation failed for document classifier [%s] with error: %s",
                    documentClassifierModel.getArn(), e));
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
        }
    }
}
