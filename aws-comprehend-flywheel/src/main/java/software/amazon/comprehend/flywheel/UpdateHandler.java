package software.amazon.comprehend.flywheel;

import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.awssdk.services.comprehend.model.FlywheelStatus;
import software.amazon.awssdk.services.comprehend.model.TagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.TagResourceResponse;
import software.amazon.awssdk.services.comprehend.model.UntagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.UntagResourceResponse;
import software.amazon.awssdk.services.comprehend.model.UpdateFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.UpdateFlywheelResponse;
import software.amazon.cloudformation.Action;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UpdateHandler extends AbstractFlywheelHandler {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<ComprehendClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        final ResourceModel flywheelModel = request.getDesiredResourceState();

        final Set<Tag> currentTags = TagHelper.getCurrentTags(proxyClient, request.getDesiredResourceState());
        final Set<Tag> desiredTags = TagHelper.getDesiredTags(request);
        UntagResourceRequest untagResourceRequest = Translator.translateToUntagResourceRequest(flywheelModel, currentTags, desiredTags);
        TagResourceRequest tagResourceRequest = Translator.translateToTagResourceRequest(flywheelModel, currentTags, desiredTags);

        callbackContext.setTagKeysToRemove(new HashSet<>(untagResourceRequest.tagKeys()));
        callbackContext.setTagsToAdd(new HashSet<>(tagResourceRequest.tags()));

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                // Progress chain to update flywheel (no stabilization as UpdateFlywheel is synchronous)
                .then(progress ->
                    proxy.initiate("AWS-Comprehend-Flywheel::Update::Flywheel", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(Translator::translateToUpdateRequest)
                        .makeServiceCall(this::updateFlywheel)
                        .handleError(this::handleError)
                        .progress())
                // Progress chain to untag if necessary and stabilize
                .then(progress ->
                    proxy.initiate("AWS-Comprehend-Flywheel::Update::Untag", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(resourceModel -> untagResourceRequest)
                        .makeServiceCall(this::untagFlywheel)
                        .stabilize(this::untagStabilize)
                        .handleError(this::handleError)
                        .progress())
                // Progress chain to tag if necessary and stabilize
                .then(progress ->
                    proxy.initiate("AWS-Comprehend-Flywheel::Update::Tag", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(resourceModel -> tagResourceRequest)
                        .makeServiceCall(this::tagFlywheel)
                        .stabilize(this::tagStabilize)
                        .handleError(this::handleError)
                        .progress())
                // Progress chain to describe flywheel and return the resource model
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    /**
     * Updates flywheel.
     */
    private UpdateFlywheelResponse updateFlywheel(
            UpdateFlywheelRequest updateFlywheelRequest,
            final ProxyClient<ComprehendClient> proxyClient
    ) {
        UpdateFlywheelResponse updateFlywheelResponse = proxyClient.injectCredentialsAndInvokeV2(
                updateFlywheelRequest, proxyClient.client()::updateFlywheel);

        logger.log(String.format("Flywheel [%s] update call successful.", updateFlywheelResponse.flywheelProperties().flywheelArn()));
        return updateFlywheelResponse;
    }

    /**
     * Untags flywheel if necessary.
     */
    private UntagResourceResponse untagFlywheel(
            UntagResourceRequest untagResourceRequest,
            final ProxyClient<ComprehendClient> proxyClient
    ) {
        if (untagResourceRequest.tagKeys().isEmpty()) return null;

        UntagResourceResponse untagResourceResponse = proxyClient.injectCredentialsAndInvokeV2(
                untagResourceRequest, proxyClient.client()::untagResource);

        logger.log(String.format("Flywheel [%s] untag call for removing %d tags successful.",
                untagResourceRequest.resourceArn(), untagResourceRequest.tagKeys().size()));

        return untagResourceResponse;
    }

    /**
     * Tags flywheel if necessary.
     */
    private TagResourceResponse tagFlywheel(
            TagResourceRequest tagResourceRequest,
            final ProxyClient<ComprehendClient> proxyClient
    ) {
        if (tagResourceRequest.tags().isEmpty()) return null;

        TagResourceResponse tagResourceResponse = proxyClient.injectCredentialsAndInvokeV2(
                tagResourceRequest, proxyClient.client()::tagResource);

        logger.log(String.format("Flywheel [%s] tag call for adding %d tags successful.",
                tagResourceRequest.resourceArn(), tagResourceRequest.tags().size()));

        return tagResourceResponse;
    }

    /**
     * Verifies flywheel untagging has stabilized by checking no tag keys to remove is present in current flywheel tag keys.
     */
    private boolean untagStabilize(
            final UntagResourceRequest request,
            final UntagResourceResponse response,
            final ProxyClient<ComprehendClient> proxyClient,
            final ResourceModel flywheelModel,
            final CallbackContext callbackContext) {

        if (callbackContext.getTagKeysToRemove().isEmpty()) return true;

        final Set<String> flywheelCurrentTagKeys = TagHelper.getCurrentTags(proxyClient, flywheelModel)
                .stream().map(Tag::key).collect(Collectors.toSet());

        boolean untagStabilized = Collections.disjoint(flywheelCurrentTagKeys, callbackContext.getTagKeysToRemove());

        logger.log(String.format("Flywheel [%s] untagging stabilization status: %s.",
                flywheelModel.getPrimaryIdentifier(), untagStabilized));

        return untagStabilized;
    }

    /**
     * Verifies flywheel tagging has stabilized by checking current flywheel tags contain all tags to add.
     */
    private boolean tagStabilize(
            final TagResourceRequest request,
            final TagResourceResponse response,
            final ProxyClient<ComprehendClient> proxyClient,
            final ResourceModel flywheelModel,
            final CallbackContext callbackContext) {


        if (callbackContext.getTagsToAdd().isEmpty()) return true;

        final Set<Tag> flywheelCurrentTags = new HashSet<>(TagHelper.getCurrentTags(proxyClient, flywheelModel));
        boolean tagStabilized = flywheelCurrentTags.containsAll(callbackContext.getTagsToAdd());
        logger.log(String.format("Flywheel [%s] untagging stabilization status: %s.",
                flywheelModel.getPrimaryIdentifier(), tagStabilized));

        return tagStabilized;
    }

}
