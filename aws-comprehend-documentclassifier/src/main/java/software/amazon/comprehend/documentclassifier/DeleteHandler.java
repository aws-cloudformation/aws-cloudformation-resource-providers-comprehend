package software.amazon.comprehend.documentclassifier;

import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.DeleteDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DeleteDocumentClassifierResponse;
import software.amazon.awssdk.services.comprehend.model.ModelStatus;
import software.amazon.awssdk.services.comprehend.model.ResourceNotFoundException;
import software.amazon.cloudformation.Action;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

import java.time.Duration;

public class DeleteHandler extends AbstractModelHandler {

    protected static final Constant DELETE_BACKOFF_STRATEGY = Constant.of().delay(Duration.ofSeconds(5)).timeout(Duration.ofMinutes(60)).build();

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<ComprehendClient> proxyClient,
            final Logger logger) {

        this.logger = logger;

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                // Progress chain to delete document classifier and stabilize
                .then(progress ->
                        proxy.initiate("AWS-Comprehend-DocumentClassifier::Delete", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                                .translateToServiceRequest(Translator::translateToDeleteRequest)
                                .backoffDelay(DELETE_BACKOFF_STRATEGY)
                                .makeServiceCall(this::deleteDocumentClassifier)
                                .stabilize(this::deleteStabilize)
                                .handleError(this::handleError)
                                .progress()
                )
                // Return the successful progress event without resource model
                .then(progress -> ProgressEvent.defaultSuccessHandler(null));
    }


    /**
     * Deletes document classifier.
     */
    private DeleteDocumentClassifierResponse deleteDocumentClassifier(
            final DeleteDocumentClassifierRequest deleteDocumentClassifierRequest,
            final ProxyClient<ComprehendClient> proxyClient
    ) {
        DeleteDocumentClassifierResponse deleteDocumentClassifierResponse = proxyClient.injectCredentialsAndInvokeV2(
                deleteDocumentClassifierRequest, proxyClient.client()::deleteDocumentClassifier);

        logger.log(String.format("DocumentClassifier [%s] deletion call successful.", deleteDocumentClassifierRequest.documentClassifierArn()));
        return deleteDocumentClassifierResponse;
    }

    /**
     * Verifies document classifier deletion has stabilized by checking if document classifier is not found.
     */
    private boolean deleteStabilize(
            final DeleteDocumentClassifierRequest request,
            final DeleteDocumentClassifierResponse response,
            final ProxyClient<ComprehendClient> proxyClient,
            final ResourceModel documentClassifierModel,
            final CallbackContext callbackContext) {
        if (documentClassifierModel.getArn() == null) documentClassifierModel.setArn(callbackContext.getArn());
        try {
            final ModelStatus currentModelStatus = getDocumentClassifierStatus(proxyClient, documentClassifierModel.getArn(), logger);
            switch(currentModelStatus) {
                case TRAINED:
                case TRAINED_WITH_WARNING:
                case DELETING:
                    logger.log(String.format("DocumentClassifier [%s] deletion has not stabilized.", documentClassifierModel.getPrimaryIdentifier()));
                    return false;
                case IN_ERROR:
                    throw new CfnGeneralServiceException(Action.DELETE.toString(), new Throwable(String.format(
                            "DocumentClassifier [%s] reached an IN_ERROR state while attempting to delete.", documentClassifierModel.getPrimaryIdentifier())));
                case TRAINING:
                case UNKNOWN_TO_SDK_VERSION:
                default:
                    throw new CfnNotStabilizedException(ResourceModel.TYPE_NAME, documentClassifierModel.getDocumentClassifierName());
            }
        } catch (ResourceNotFoundException e) {
            logger.log(String.format("DocumentClassifier [%s] deletion has stabilized.", documentClassifierModel.getPrimaryIdentifier()));
            return true;
        }
    }
}
