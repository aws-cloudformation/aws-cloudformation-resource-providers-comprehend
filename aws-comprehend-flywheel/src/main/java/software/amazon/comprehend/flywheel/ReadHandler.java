package software.amazon.comprehend.flywheel;

import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.comprehend.model.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;


public class ReadHandler extends AbstractFlywheelHandler {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<ComprehendClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        final ResourceModel flywheelModel = request.getDesiredResourceState();

        DescribeFlywheelRequest describeFlywheelRequest = Translator.translateToReadRequest(flywheelModel);
        ListTagsForResourceRequest listTagsForResourceRequest = Translator.translateToListTagsRequest(flywheelModel);

        try {
            DescribeFlywheelResponse describeFlywheelResponse = proxy.injectCredentialsAndInvokeV2(describeFlywheelRequest, proxyClient.client()::describeFlywheel);
            logger.log(String.format("Successfully described flywheel [%s].", flywheelModel.getArn()));

            ListTagsForResourceResponse listTagsForResourceResponse = proxy.injectCredentialsAndInvokeV2(listTagsForResourceRequest, proxyClient.client()::listTagsForResource);
            logger.log(String.format("Successfully listed %d tags for flywheel [%s].",
                    listTagsForResourceResponse.tags().size(), flywheelModel.getArn()));

            ResourceModel newModel = Translator.translateFromReadResponse(describeFlywheelResponse);
            newModel.setTags(Translator.fromSdkTags(listTagsForResourceResponse.tags()));

            return ProgressEvent.success(newModel, callbackContext);
        } catch (final ResourceNotFoundException e) {
            logger.log(String.format("Read operation failed for flywheel [%s] with error: %s",
                    flywheelModel.getArn(), e));
            return ProgressEvent.defaultFailureHandler(e, HandlerErrorCode.NotFound);
        }

    }

}
