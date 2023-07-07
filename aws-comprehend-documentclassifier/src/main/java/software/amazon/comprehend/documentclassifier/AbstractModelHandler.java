package software.amazon.comprehend.documentclassifier;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.ComprehendException;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeResourcePolicyResponse;
import software.amazon.awssdk.services.comprehend.model.InternalServerException;
import software.amazon.awssdk.services.comprehend.model.InvalidRequestException;
import software.amazon.awssdk.services.comprehend.model.KmsKeyValidationException;
import software.amazon.awssdk.services.comprehend.model.ModelStatus;
import software.amazon.awssdk.services.comprehend.model.ResourceInUseException;
import software.amazon.awssdk.services.comprehend.model.ResourceLimitExceededException;
import software.amazon.awssdk.services.comprehend.model.ResourceNotFoundException;
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

/**
 * Functionality shared across Create/Read/Update/Delete/List Handlers
 */
public abstract class AbstractModelHandler extends BaseHandler<CallbackContext> {

    protected Logger logger;

    public abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<ComprehendClient> proxyClient,
            final Logger logger);

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {
        return handleRequest(
                proxy,
                request,
                callbackContext == null ? CallbackContext.builder().build() : callbackContext,
                proxy.newProxy(() -> ComprehendClient.builder().build()),
                logger
        );
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleError(
            final AwsRequest request,
            final Exception exception,
            final ProxyClient<ComprehendClient> proxyClient,
            final ResourceModel documentClassifierModel,
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
     * Get status of document classifier via DescribeDocumentClassifier call.
     */
    protected ModelStatus getDocumentClassifierStatus(final ProxyClient<ComprehendClient> proxyClient,
                                                      final String documentClassifierArn,
                                                      final Logger logger) {
        logger.log(String.format("Getting model status for document classifier [%s] via DescribeDocumentClassifier.", documentClassifierArn));
        DescribeDocumentClassifierRequest describeDocumentClassifierRequest = DescribeDocumentClassifierRequest.builder()
                .documentClassifierArn(documentClassifierArn)
                .build();

        ModelStatus modelStatus = proxyClient
                .injectCredentialsAndInvokeV2(describeDocumentClassifierRequest, proxyClient.client()::describeDocumentClassifier)
                .documentClassifierProperties()
                .status();

        logger.log(String.format("DocumentClassifier [%s] has status %s.", documentClassifierArn, modelStatus));
        return modelStatus;
    }

    /**
     * Get resource policy revision id of document classifier via DescribeDocumentClassifier call if it exists.
     */
    protected String getDocumentClassifierResourcePolicy(final ProxyClient<ComprehendClient> proxyClient,
                                                         final String documentClassifierArn,
                                                         final Logger logger) {
        logger.log(String.format("Getting resource policy for document classifier [%s] via DescribeResourcePolicy.", documentClassifierArn));
        DescribeResourcePolicyRequest describeResourcePolicyRequest = DescribeResourcePolicyRequest.builder()
                .resourceArn(documentClassifierArn)
                .build();

        DescribeResourcePolicyResponse describeResourcePolicyResponse = proxyClient
                .injectCredentialsAndInvokeV2(describeResourcePolicyRequest, proxyClient.client()::describeResourcePolicy);

        if (describeResourcePolicyResponse.resourcePolicy() == null) {
            logger.log(String.format("DocumentClassifier [%s] does not have a resource policy attached.", documentClassifierArn));
        } else {
            logger.log(String.format("DocumentClassifier [%s] has resource policy revision with id %s.", documentClassifierArn,
                    describeResourcePolicyResponse.policyRevisionId()));
        }
        return describeResourcePolicyResponse.resourcePolicy();
    }

}
