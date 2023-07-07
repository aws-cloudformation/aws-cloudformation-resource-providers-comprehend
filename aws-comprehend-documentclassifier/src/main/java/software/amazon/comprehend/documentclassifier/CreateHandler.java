package software.amazon.comprehend.documentclassifier;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.CreateDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.CreateDocumentClassifierResponse;
import software.amazon.awssdk.services.comprehend.model.ModelStatus;
import software.amazon.awssdk.services.comprehend.model.ResourceInUseException;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.cloudformation.Action;
import software.amazon.cloudformation.exceptions.BaseHandlerException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;


public class CreateHandler extends AbstractModelHandler {

    private static final Constant CREATE_BACKOFF_STRATEGY = Constant.of().delay(Duration.ofSeconds(15)).timeout(Duration.ofDays(2)).build();

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<ComprehendClient> proxyClient,
            final Logger logger
    ) {

        this.logger = logger;
        final ResourceModel documentClassifierModel = request.getDesiredResourceState();

        final Set<Tag> desiredTags = TagHelper.getDesiredTags(request);
        callbackContext.setTagsToAdd(desiredTags);

        return ProgressEvent.progress(documentClassifierModel, callbackContext)
                // Progress chain to create document classifier and stabilize
                .then(progress -> proxy
                        .initiate("AWS-Comprehend-DocumentClassifier::Create", proxyClient, documentClassifierModel, callbackContext)
                        .translateToServiceRequest(resourceModel -> Translator.translateToCreateRequest(
                                resourceModel,
                                request.getClientRequestToken(),
                                desiredTags))
                        .backoffDelay(CREATE_BACKOFF_STRATEGY)
                        .makeServiceCall((awsRequest, client) -> createDocumentClassifierAndUpdateResourceModel(awsRequest, client, documentClassifierModel, callbackContext))
                        .stabilize(this::createStabilize)
                        .handleError(this::handleError)
                        .progress()
                )
                // Progress chain to describe document classifier and return result
                .then(progress -> {
                    if (documentClassifierModel.getArn() == null) documentClassifierModel.setArn(callbackContext.getArn());
                    return new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger);
                });
    }

    /**
     * Creates document classifier and obtain document classifier arn from the response to set the resource model arn.
     */
    private CreateDocumentClassifierResponse createDocumentClassifierAndUpdateResourceModel(
            CreateDocumentClassifierRequest createDocumentClassifierRequest,
            final ProxyClient<ComprehendClient> proxyClient,
            final ResourceModel documentClassifierModel,
            final CallbackContext callbackContext
    ) {
        CreateDocumentClassifierResponse createDocumentClassifierResponse = proxyClient.injectCredentialsAndInvokeV2(
                createDocumentClassifierRequest, proxyClient.client()::createDocumentClassifier);

        documentClassifierModel.setArn(createDocumentClassifierResponse.documentClassifierArn());
        callbackContext.setArn(createDocumentClassifierResponse.documentClassifierArn());
        logger.log(String.format("DocumentClassifier [%s] creation call successful.", documentClassifierModel.getArn()));

        return createDocumentClassifierResponse;
    }

    /**
     * Verifies document classifier creation has stabilized by checking if model status is TRAINED and the desired tags
     * are present.
     */
    private boolean createStabilize(
            final CreateDocumentClassifierRequest request,
            final CreateDocumentClassifierResponse response,
            final ProxyClient<ComprehendClient> proxyClient,
            final ResourceModel documentClassifierModel,
            final CallbackContext callbackContext) {
        if (documentClassifierModel.getArn() == null) documentClassifierModel.setArn(callbackContext.getArn());
        return (callbackContext.getModelStatus() == ModelStatus.TRAINED ||  callbackContext.getModelStatus() == ModelStatus.TRAINED_WITH_WARNING
                || isDocumentClassifierTrained(proxyClient, documentClassifierModel, callbackContext))
                && isTaggingComplete(proxyClient, documentClassifierModel, callbackContext.getTagsToAdd());
    }

    /**
     * Returns whether document classifier has active status.
     */
    private boolean isDocumentClassifierTrained(final ProxyClient<ComprehendClient> proxyClient,
                                               final ResourceModel documentClassifierModel,
                                               final CallbackContext callbackContext) {
        final ModelStatus modelStatus = getDocumentClassifierStatus(proxyClient, documentClassifierModel.getArn(), logger);
        callbackContext.setModelStatus(modelStatus);
        switch (modelStatus) {
            case TRAINED:
            case TRAINED_WITH_WARNING:
                logger.log(String.format("DocumentClassifier [%s] has reached TRAINED state.", documentClassifierModel.getPrimaryIdentifier()));
                return true;
            case TRAINING:
            case SUBMITTED:
                logger.log(String.format("DocumentClassifier [%s] has not reached TRAINED state.", documentClassifierModel.getPrimaryIdentifier()));
                return false;
            case IN_ERROR:
                throw new CfnGeneralServiceException(Action.CREATE.toString(), new Throwable(String.format(
                        "DocumentClassifier [%s] reached a FAILED state while attempting to create.", documentClassifierModel.getPrimaryIdentifier())));
            case DELETING:
            case UNKNOWN_TO_SDK_VERSION:
            default:
                throw new CfnNotStabilizedException(ResourceModel.TYPE_NAME, documentClassifierModel.getDocumentClassifierName());
        }
    }

    /**
     * Returns whether document classifier resource has the desired tags.
     */
    private boolean isTaggingComplete(final ProxyClient<ComprehendClient> proxyClient,
                                      final ResourceModel documentClassifierModel,
                                      final Set<Tag> desiredTags) {

        if (desiredTags.isEmpty()) return true;

        final Set<Tag> documentClassifierCurrentTags = new HashSet<>(TagHelper.getCurrentTags(proxyClient, documentClassifierModel));
        boolean taggingStabilized = desiredTags.equals(documentClassifierCurrentTags);
        logger.log(String.format("DocumentClassifier [%s] tagging stabilization status: %s.",
                documentClassifierModel.getPrimaryIdentifier(), taggingStabilized));

        return taggingStabilized;
    }


    /**
     * CreateHandler maps service's ResourceInUseException to CloudFormation's CfnAlreadyExistsException, instead of
     * mapping to CfnResourceConflictException like for the other handlers
     */
    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleError(
            final AwsRequest request,
            final Exception exception,
            final ProxyClient<ComprehendClient> proxyClient,
            final ResourceModel documentClassifierModel,
            final CallbackContext callbackContext) {

        if (exception instanceof ResourceInUseException) {
            BaseHandlerException handlerException = new CfnAlreadyExistsException(exception);
            return ProgressEvent.defaultFailureHandler(handlerException, handlerException.getErrorCode());
        }

        return super.handleError(request, exception, proxyClient, documentClassifierModel, callbackContext);
    }
}
