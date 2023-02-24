package software.amazon.comprehend.flywheel;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.ListFlywheelsRequest;
import software.amazon.awssdk.services.comprehend.model.ListFlywheelsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.ArrayList;
import java.util.List;

public class ListHandler extends AbstractFlywheelHandler {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<ComprehendClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        final ListFlywheelsRequest listFlywheelsRequest = Translator.translateToListRequest(request.getNextToken());
        ListFlywheelsResponse listFlywheelsResponse = proxy.injectCredentialsAndInvokeV2(listFlywheelsRequest, proxyClient.client()::listFlywheels);

        logger.log(String.format("Successfully listed %d flywheels.", listFlywheelsResponse.flywheelSummaryList().size()));
        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(Translator.translateFromListResponse(listFlywheelsResponse))
                .nextToken(listFlywheelsResponse.nextToken())
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
