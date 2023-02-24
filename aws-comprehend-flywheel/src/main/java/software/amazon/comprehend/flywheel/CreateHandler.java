package software.amazon.comprehend.flywheel;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.CreateFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.CreateFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.FlywheelStatus;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class CreateHandler extends AbstractFlywheelHandler {

    protected static final Constant CREATE_BACKOFF_STRATEGY = Constant.of().delay(Duration.ofSeconds(15)).timeout(Duration.ofDays(2)).build();

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<ComprehendClient> proxyClient,
        final Logger logger) {

        this.logger = logger;
        final ResourceModel flywheelModel = request.getDesiredResourceState();

        final Set<Tag> desiredTags = TagHelper.getDesiredTags(request);
        callbackContext.setTagsToAdd(desiredTags);

        return ProgressEvent.progress(flywheelModel, callbackContext)
            // Progress chain to create flywheel and stabilize
            .then(progress ->
                proxy.initiate("AWS-Comprehend-Flywheel::Create", proxyClient, flywheelModel, callbackContext)
                    .translateToServiceRequest(resourceModel -> Translator.translateToCreateRequest(
                            resourceModel,
                            request.getClientRequestToken(),
                            desiredTags))
                    .backoffDelay(CREATE_BACKOFF_STRATEGY)
                    .makeServiceCall((awsRequest, client) -> createFlywheelAndUpdateResourceModel(awsRequest, client, flywheelModel, callbackContext))
                    .stabilize(this::createStabilize)
                    .handleError(this::handleError)
                    .progress()
            )
            // Progress chain to describe flywheel and return result
            .then(progress -> {
                if (flywheelModel.getArn() == null) flywheelModel.setArn(callbackContext.getFlywheelArn());
                return new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger);
            });
    }

    /**
     * Creates flywheel and obtain flywheel arn from the response to set the resource model arn.
     */
    private CreateFlywheelResponse createFlywheelAndUpdateResourceModel(
            CreateFlywheelRequest createFlywheelRequest,
            final ProxyClient<ComprehendClient> proxyClient,
            final ResourceModel flywheelModel,
            final CallbackContext callbackContext
    ) {
        CreateFlywheelResponse createFlywheelResponse = proxyClient.injectCredentialsAndInvokeV2(
                createFlywheelRequest, proxyClient.client()::createFlywheel);

        flywheelModel.setArn(createFlywheelResponse.flywheelArn());
        callbackContext.setFlywheelArn(createFlywheelResponse.flywheelArn());
        logger.log(String.format("Flywheel [%s] creation call successful.", flywheelModel.getArn()));

        return createFlywheelResponse;
    }

    /**
     * Verifies flywheel creation has stabilized by checking if flywheel status is ACTIVE and the desired tags are
     * present.
     */
    private boolean createStabilize(
            final CreateFlywheelRequest request,
            final CreateFlywheelResponse response,
            final ProxyClient<ComprehendClient> proxyClient,
            final ResourceModel flywheelModel,
            final CallbackContext callbackContext) {
        return (callbackContext.getFlywheelStatus() == FlywheelStatus.ACTIVE || isFlywheelActive(proxyClient, flywheelModel, callbackContext))
                && isTaggingComplete(proxyClient, flywheelModel, callbackContext.getTagsToAdd());
    }

    /**
     * Returns whether flywheel has active status.
     */
    private boolean isFlywheelActive(final ProxyClient<ComprehendClient> proxyClient,
                                     final ResourceModel flywheelModel,
                                     final CallbackContext callbackContext) {
        final FlywheelStatus flywheelStatus = getFlywheelStatus(proxyClient, flywheelModel.getArn(), logger);
        callbackContext.setFlywheelStatus(flywheelStatus);
        switch (flywheelStatus) {
            case ACTIVE:
                logger.log(String.format("Flywheel [%s] has reached ACTIVE state.", flywheelModel.getPrimaryIdentifier()));
                return true;
            case CREATING:
                logger.log(String.format("Flywheel [%s] has not reached ACTIVE state.", flywheelModel.getPrimaryIdentifier()));
                return false;
            case FAILED:
                throw new CfnGeneralServiceException(Action.CREATE.toString(), new Throwable(String.format(
                        "Flywheel [%s] reached a FAILED state while attempting to create.", flywheelModel.getPrimaryIdentifier())));
            case UPDATING:
            case DELETING:
            case UNKNOWN_TO_SDK_VERSION:
            default:
                throw new CfnNotStabilizedException(ResourceModel.TYPE_NAME, flywheelModel.getFlywheelName());
        }
    }

    /**
     * Returns whether flywheel resource has the desired tags.
     */
    private boolean isTaggingComplete(final ProxyClient<ComprehendClient> proxyClient,
                                      final ResourceModel flywheelModel,
                                      final Set<Tag> desiredTags) {

        if (desiredTags.isEmpty()) return true;

        final Set<Tag> flywheelCurrentTags = new HashSet<>(TagHelper.getCurrentTags(proxyClient, flywheelModel));
        boolean taggingStabilized = desiredTags.equals(flywheelCurrentTags);
        logger.log(String.format("Flywheel [%s] tagging stabilization status: %s.",
                flywheelModel.getPrimaryIdentifier(), taggingStabilized));

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
            final ResourceModel flywheelModel,
            final CallbackContext callbackContext) {

        if (exception instanceof ResourceInUseException) {
            BaseHandlerException handlerException = new CfnAlreadyExistsException(exception);
            return ProgressEvent.defaultFailureHandler(handlerException, handlerException.getErrorCode());
        }

        return super.handleError(request, exception, proxyClient, flywheelModel, callbackContext);
    }

}
