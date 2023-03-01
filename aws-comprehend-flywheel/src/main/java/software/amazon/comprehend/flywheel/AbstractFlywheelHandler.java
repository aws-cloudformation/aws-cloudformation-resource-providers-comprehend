package software.amazon.comprehend.flywheel;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.ComprehendException;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.FlywheelStatus;
import software.amazon.awssdk.services.comprehend.model.InternalServerException;
import software.amazon.awssdk.services.comprehend.model.InvalidRequestException;
import software.amazon.awssdk.services.comprehend.model.KmsKeyValidationException;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.comprehend.model.ResourceInUseException;
import software.amazon.awssdk.services.comprehend.model.ResourceLimitExceededException;
import software.amazon.awssdk.services.comprehend.model.ResourceNotFoundException;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.awssdk.services.comprehend.model.TooManyTagsException;
import software.amazon.awssdk.services.comprehend.model.UnsupportedLanguageException;
import software.amazon.cloudformation.exceptions.BaseHandlerException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.exceptions.CfnServiceInternalErrorException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;


/**
 * Functionality shared across Create/Read/Update/Delete/List Handlers
 */
public abstract class AbstractFlywheelHandler extends BaseHandler<CallbackContext> {

  protected Logger logger;

  public abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
    final AmazonWebServicesClientProxy proxy,
    final ResourceHandlerRequest<ResourceModel> request,
    final CallbackContext callbackContext,
    final ProxyClient<ComprehendClient> proxyClient,
    final Logger logger);

  @Override
  public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
          AmazonWebServicesClientProxy proxy,
          ResourceHandlerRequest<ResourceModel> request,
          CallbackContext callbackContext,
          Logger logger) {
    return handleRequest(
            proxy,
            request,
            callbackContext == null ? CallbackContext.builder().build(): callbackContext,
            proxy.newProxy(() -> ComprehendClient.builder().build()),
            logger);
  }

  protected ProgressEvent<ResourceModel, CallbackContext> handleError(
          final AwsRequest request,
          final Exception exception,
          final ProxyClient<ComprehendClient> proxyClient,
          final ResourceModel flywheelModel,
          final CallbackContext callbackContext) {

    if (exception instanceof ApiCallAttemptTimeoutException ||
            exception instanceof ApiCallTimeoutException ||
            (exception instanceof ComprehendException && ((ComprehendException) exception).isThrottlingException())) {
      throw RetryableException.builder().cause(exception).build();
    }

    final BaseHandlerException handlerException;
    if (exception instanceof InvalidRequestException ||
            exception instanceof KmsKeyValidationException ||
            exception instanceof ResourceLimitExceededException ||
            exception instanceof TooManyTagsException ||
            exception instanceof UnsupportedLanguageException) {
      handlerException = new CfnInvalidRequestException(exception);
    } else if (exception instanceof ResourceNotFoundException) {
      handlerException = new CfnNotFoundException(exception);
    } else if (exception instanceof ResourceInUseException) {
      handlerException = new CfnResourceConflictException(exception);
    } else if (exception instanceof InternalServerException) {
      handlerException = new CfnServiceInternalErrorException(exception);
    } else {
      handlerException = new CfnGeneralServiceException(exception);
    }

    return ProgressEvent.defaultFailureHandler(handlerException, handlerException.getErrorCode());
  }

  /**
   * Get status of flywheel via DescribeFlywheel call.
   */
  protected FlywheelStatus getFlywheelStatus(final ProxyClient<ComprehendClient> comprehendClient,
                                             final String flywheelArn,
                                             final Logger logger) {
    logger.log(String.format("Getting flywheel status for flywheel [%s] via DescribeFlywheel.", flywheelArn));
    DescribeFlywheelRequest describeFlywheelRequest = DescribeFlywheelRequest.builder()
            .flywheelArn(flywheelArn)
            .build();

    FlywheelStatus flywheelStatus = comprehendClient
            .injectCredentialsAndInvokeV2(describeFlywheelRequest, comprehendClient.client()::describeFlywheel)
            .flywheelProperties()
            .status();

    logger.log(String.format("Flywheel [%s] has status %s.", flywheelArn, flywheelStatus));
    return flywheelStatus;
  }

}
