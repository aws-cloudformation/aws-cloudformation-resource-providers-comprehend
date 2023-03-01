package software.amazon.comprehend.flywheel;

import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.DeleteFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DeleteFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.FlywheelStatus;
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

public class DeleteHandler extends AbstractFlywheelHandler {

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
            // Progress chain to delete flywheel and stabilize
            .then(progress ->
                 proxy.initiate("AWS-Comprehend-Flywheel::Delete", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                    .translateToServiceRequest(Translator::translateToDeleteRequest)
                    .backoffDelay(DELETE_BACKOFF_STRATEGY)
                    .makeServiceCall(this::deleteFlywheel)
                    .stabilize(this::deleteStabilize)
                    .handleError(this::handleError)
                    .progress()
            )
            // Return the successful progress event without resource model
            .then(progress -> ProgressEvent.defaultSuccessHandler(null));
    }

    /**
     * Deletes flywheel.
     */
    private DeleteFlywheelResponse deleteFlywheel(
            DeleteFlywheelRequest deleteFlywheelRequest,
            final ProxyClient<ComprehendClient> proxyClient
    ) {
        DeleteFlywheelResponse deleteFlywheelResponse = proxyClient.injectCredentialsAndInvokeV2(
                deleteFlywheelRequest, proxyClient.client()::deleteFlywheel);

        logger.log(String.format("Flywheel [%s] deletion call successful.", deleteFlywheelRequest.flywheelArn()));
        return deleteFlywheelResponse;
    }

    /**
     * Verifies flywheel deletion has stabilized by checking if flywheel is not found.
     */
    private boolean deleteStabilize(
            final DeleteFlywheelRequest request,
            final DeleteFlywheelResponse response,
            final ProxyClient<ComprehendClient> proxyClient,
            final ResourceModel flywheelModel,
            final CallbackContext callbackContext) {
        try {
            final FlywheelStatus flywheelStatus = getFlywheelStatus(proxyClient, flywheelModel.getArn(), logger);
            switch(flywheelStatus) {
                case ACTIVE:
                case DELETING:
                    logger.log(String.format("Flywheel [%s] deletion has not stabilized.", flywheelModel.getPrimaryIdentifier()));
                    return false;
                case FAILED:
                    throw new CfnGeneralServiceException(Action.CREATE.toString(), new Throwable(String.format(
                            "Flywheel [%s] reached a FAILED state while attempting to delete.", flywheelModel.getPrimaryIdentifier())));
                case CREATING:
                case UPDATING:
                case UNKNOWN_TO_SDK_VERSION:
                default:
                    throw new CfnNotStabilizedException(ResourceModel.TYPE_NAME, flywheelModel.getFlywheelName());
            }
        } catch (ResourceNotFoundException e) {
            logger.log(String.format("Flywheel [%s] deletion has stabilized.", flywheelModel.getPrimaryIdentifier()));
            return true;
        }
    }
}
