package software.amazon.comprehend.documentclassifier;

import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.DeleteResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.DeleteResourcePolicyResponse;
import software.amazon.awssdk.services.comprehend.model.PutResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.PutResourcePolicyResponse;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.awssdk.services.comprehend.model.TagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.TagResourceResponse;
import software.amazon.awssdk.services.comprehend.model.UntagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.UntagResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class UpdateHandler extends AbstractModelHandler {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                       final ResourceHandlerRequest<ResourceModel> request,
                                                                       final CallbackContext callbackContext,
                                                                       final ProxyClient<ComprehendClient> proxyClient,
                                                                       final Logger logger) {
        this.logger = logger;

        final ResourceModel documentClassifierModel = request.getDesiredResourceState();

        final String currentResourcePolicy = getDocumentClassifierResourcePolicy(
                proxyClient, documentClassifierModel.getArn(), logger);
        
        final Set<Tag> currentTags = TagHelper.getCurrentTags(proxyClient, request.getDesiredResourceState());
        final Set<Tag> desiredTags = TagHelper.getDesiredTags(request);
        UntagResourceRequest untagResourceRequest = Translator.translateToUntagResourceRequest(documentClassifierModel, currentTags, desiredTags);
        TagResourceRequest tagResourceRequest = Translator.translateToTagResourceRequest(documentClassifierModel, currentTags, desiredTags);

        callbackContext.setTagKeysToRemove(new HashSet<>(untagResourceRequest.tagKeys()));
        callbackContext.setTagsToAdd(new HashSet<>(tagResourceRequest.tags()));

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                // Progress chain to delete model policy if necessary
                .then(progress ->
                        proxy.initiate("AWS-Comprehend-DocumentClassifier::Update::DeleteResourcePolicy", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                                .translateToServiceRequest(Translator::translateToDeleteResourcePolicyRequest)
                                .makeServiceCall((awsRequest, client) -> deleteResourcePolicy(awsRequest, client, documentClassifierModel, currentResourcePolicy))
                                .handleError(this::handleError)
                                .progress())
                // Progress chain to update model policy if necessary
                .then(progress ->
                        proxy.initiate("AWS-Comprehend-DocumentClassifier::Update::PutResourcePolicy", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                                .translateToServiceRequest(Translator::translateToPutResourcePolicyRequest)
                                .makeServiceCall((awsRequest, client) -> putResourcePolicy(awsRequest, client, documentClassifierModel, currentResourcePolicy))
                                .handleError(this::handleError)
                                .progress())
                // Progress chain to untag if necessary and stabilize
                .then(progress ->
                        proxy.initiate("AWS-Comprehend-DocumentClassifier::Update::Untag", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                                .translateToServiceRequest(resourceModel -> untagResourceRequest)
                                .makeServiceCall(this::untagDocumentClassifier)
                                .stabilize(this::untagStabilize)
                                .handleError(this::handleError)
                                .progress())
                // Progress chain to tag if necessary and stabilize
                .then(progress ->
                        proxy.initiate("AWS-Comprehend-DocumentClassifier::Update::Tag", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                                .translateToServiceRequest(resourceModel -> tagResourceRequest)
                                .makeServiceCall(this::tagDocumentClassifier)
                                .stabilize(this::tagStabilize)
                                .handleError(this::handleError)
                                .progress())
                // Progress chain to describe document classifier and return the resource model
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    /**
     * Deletes resource policy for document classifier if necessary.
     */
    private DeleteResourcePolicyResponse deleteResourcePolicy(
            DeleteResourcePolicyRequest deleteResourcePolicyRequest,
            final ProxyClient<ComprehendClient> proxyClient,
            ResourceModel documentClassifierModel,
            String currentResourcePolicy
    ) {
        if (documentClassifierModel.getModelPolicy() != null || currentResourcePolicy == null) return null;
        
        DeleteResourcePolicyResponse deleteResourcePolicyResponse = proxyClient.injectCredentialsAndInvokeV2(
                deleteResourcePolicyRequest, proxyClient.client()::deleteResourcePolicy);

        logger.log(String.format("DDocumentClassifier [%s] delete resource policy call successful.", deleteResourcePolicyRequest.resourceArn()));
        return deleteResourcePolicyResponse;
    }

    /**
     * Puts resource policy for document classifier if necessary.
     */
    private PutResourcePolicyResponse putResourcePolicy(
            PutResourcePolicyRequest putResourcePolicyRequest,
            final ProxyClient<ComprehendClient> proxyClient,
            ResourceModel documentClassifierModel,
            String currentResourcePolicy
    ) {
        if (documentClassifierModel.getModelPolicy() == null ||
                documentClassifierModel.getModelPolicy().equals(currentResourcePolicy)) return null;

        PutResourcePolicyResponse putResourcePolicyResponse = proxyClient.injectCredentialsAndInvokeV2(
                putResourcePolicyRequest, proxyClient.client()::putResourcePolicy);

        logger.log(String.format("DocumentClassifier [%s] put resource policy call successful.", putResourcePolicyRequest.resourceArn()));
        return putResourcePolicyResponse;
    }

    /**
     * Untags document classifier if necessary.
     */
    private UntagResourceResponse untagDocumentClassifier(
            UntagResourceRequest untagResourceRequest,
            final ProxyClient<ComprehendClient> proxyClient
    ) {
        if (untagResourceRequest.tagKeys().isEmpty()) return null;

        UntagResourceResponse untagResourceResponse = proxyClient.injectCredentialsAndInvokeV2(
                untagResourceRequest, proxyClient.client()::untagResource);

        logger.log(String.format("DocumentClassifier [%s] untag call for removing %d tags successful.",
                untagResourceRequest.resourceArn(), untagResourceRequest.tagKeys().size()));

        return untagResourceResponse;
    }

    /**
     * Tags document classifier if necessary.
     */
    private TagResourceResponse tagDocumentClassifier(
            TagResourceRequest tagResourceRequest,
            final ProxyClient<ComprehendClient> proxyClient
    ) {
        if (tagResourceRequest.tags().isEmpty()) return null;

        TagResourceResponse tagResourceResponse = proxyClient.injectCredentialsAndInvokeV2(
                tagResourceRequest, proxyClient.client()::tagResource);

        logger.log(String.format("DocumentClassifier [%s] tag call for adding %d tags successful.",
                tagResourceRequest.resourceArn(), tagResourceRequest.tags().size()));

        return tagResourceResponse;
    }

    /**
     * Verifies document classifier untagging has stabilized by checking no tag keys to remove is present in current document classifier tag keys.
     */
    private boolean untagStabilize(
            final UntagResourceRequest request,
            final UntagResourceResponse response,
            final ProxyClient<ComprehendClient> proxyClient,
            final ResourceModel documentClassifierModel,
            final CallbackContext callbackContext) {

        if (callbackContext.getTagKeysToRemove().isEmpty()) return true;
        if (documentClassifierModel.getArn() == null) documentClassifierModel.setArn(callbackContext.getArn());

        final Set<String> currentTagKeys = TagHelper.getCurrentTags(proxyClient, documentClassifierModel)
                .stream().map(Tag::key).collect(Collectors.toSet());

        boolean untagStabilized = Collections.disjoint(currentTagKeys, callbackContext.getTagKeysToRemove());

        logger.log(String.format("DocumentClassifier [%s] untagging stabilization status: %s.",
                documentClassifierModel.getPrimaryIdentifier(), untagStabilized));

        return untagStabilized;
    }

    /**
     * Verifies document classifier tagging has stabilized by checking current document classifier tags contain all tags to add.
     */
    private boolean tagStabilize(
            final TagResourceRequest request,
            final TagResourceResponse response,
            final ProxyClient<ComprehendClient> proxyClient,
            final ResourceModel documentClassifierModel,
            final CallbackContext callbackContext) {

        if (callbackContext.getTagsToAdd().isEmpty()) return true;
        if (documentClassifierModel.getArn() == null) documentClassifierModel.setArn(callbackContext.getArn());

        final Set<Tag> currentTags = new HashSet<>(TagHelper.getCurrentTags(proxyClient, documentClassifierModel));
        boolean tagStabilized = currentTags.containsAll(callbackContext.getTagsToAdd());
        logger.log(String.format("DocumentClassifier [%s] untagging stabilization status: %s.",
                documentClassifierModel.getPrimaryIdentifier(), tagStabilized));

        return tagStabilized;
    }
}
