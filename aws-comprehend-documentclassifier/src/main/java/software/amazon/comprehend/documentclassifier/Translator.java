package software.amazon.comprehend.documentclassifier;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.comprehend.model.AugmentedManifestsListItem;
import software.amazon.awssdk.services.comprehend.model.CreateDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DeleteDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DeleteResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierResponse;
import software.amazon.awssdk.services.comprehend.model.DescribeResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierInputDataConfig;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierOutputDataConfig;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierProperties;
import software.amazon.awssdk.services.comprehend.model.DocumentReadFeatureTypes;
import software.amazon.awssdk.services.comprehend.model.ListDocumentClassifiersRequest;
import software.amazon.awssdk.services.comprehend.model.ListDocumentClassifiersResponse;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierDocuments;
import software.amazon.awssdk.services.comprehend.model.DocumentReaderConfig;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.PutResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.awssdk.services.comprehend.model.TagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.UntagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.VpcConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Translator {

    private static final Gson GSON = new Gson();
    

    /* * * * * * * * * * * * *
     *  Translation To SDK   *
     * * * * * * * * * * * * */
    
    public static VpcConfig toSdkVpcConfig(software.amazon.comprehend.documentclassifier.VpcConfig vpcConfig) {
        return vpcConfig == null ? null: VpcConfig.builder()
                .securityGroupIds(vpcConfig.getSecurityGroupIds())
                .subnets(vpcConfig.getSubnets())
                .build();
    }

    public static DocumentClassifierInputDataConfig toSdkInputDataConfig(software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig inputDataConfig) {
        return DocumentClassifierInputDataConfig.builder()
                .dataFormat(inputDataConfig.getDataFormat())
                .s3Uri(inputDataConfig.getS3Uri())
                .testS3Uri(inputDataConfig.getTestS3Uri())
                .labelDelimiter(inputDataConfig.getLabelDelimiter())
                .augmentedManifests(inputDataConfig.getAugmentedManifests() == null ? null: inputDataConfig.getAugmentedManifests().stream()
                        .map(manifest -> AugmentedManifestsListItem.builder()
                                .attributeNames(manifest.getAttributeNames())
                                .s3Uri(manifest.getS3Uri())
                                .split(manifest.getSplit())
                                .build())
                        .collect(Collectors.toList()))
                .documentType(inputDataConfig.getDocumentType())
                .documents(inputDataConfig.getDocuments() == null ? null : DocumentClassifierDocuments.builder()
                        .s3Uri(inputDataConfig.getDocuments().getS3Uri())
                        .testS3Uri(inputDataConfig.getDocuments().getTestS3Uri())
                        .build())
                .documentReaderConfig(inputDataConfig.getDocumentReaderConfig() == null ? null : DocumentReaderConfig.builder()
                        .documentReadAction(inputDataConfig.getDocumentReaderConfig().getDocumentReadAction())
                        .documentReadMode(inputDataConfig.getDocumentReaderConfig().getDocumentReadMode())
                        .featureTypes(streamOfOrEmpty(inputDataConfig.getDocumentReaderConfig().getFeatureTypes())
                                .map(DocumentReadFeatureTypes::fromValue)
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    public static DocumentClassifierOutputDataConfig toSdkOutputDataConfig(software.amazon.comprehend.documentclassifier.DocumentClassifierOutputDataConfig outputDataConfig) {
        return outputDataConfig == null ? null: DocumentClassifierOutputDataConfig.builder()
                .kmsKeyId(outputDataConfig.getKmsKeyId())
                .s3Uri(outputDataConfig.getS3Uri())
                .build();
    }

    public static Collection<Tag> toSdkTags(Set<software.amazon.comprehend.documentclassifier.Tag> tags) {
        return streamOfOrEmpty(tags).map(t ->
                        Tag.builder()
                                .key(t.getKey())
                                .value(t.getValue())
                                .build())
                .collect(Collectors.toList());
    }

    /* * * * * * * * * * * * *
     *  Translation To CFN   *
     * * * * * * * * * * * * */

    public static software.amazon.comprehend.documentclassifier.VpcConfig fromSdkVpcConfig(VpcConfig vpcConfig) {
        return vpcConfig == null ? null: software.amazon.comprehend.documentclassifier.VpcConfig.builder()
                .securityGroupIds(streamOfOrEmpty(vpcConfig.securityGroupIds()).collect(Collectors.toSet()))
                .subnets(streamOfOrEmpty(vpcConfig.subnets()).collect(Collectors.toSet()))
                .build();
    }

    public static software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig fromSdkInputDataConfig(DocumentClassifierInputDataConfig inputDataConfig) {
        return inputDataConfig == null ? null: software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig.builder()
                .dataFormat(inputDataConfig.dataFormatAsString())
                .s3Uri(inputDataConfig.s3Uri())
                .testS3Uri(inputDataConfig.testS3Uri())
                .labelDelimiter(inputDataConfig.labelDelimiter())
                .augmentedManifests(streamOfOrEmpty(inputDataConfig.augmentedManifests())
                        .map(manifest -> software.amazon.comprehend.documentclassifier.AugmentedManifestsListItem.builder()
                                .attributeNames(new HashSet<>(manifest.attributeNames()))
                                .s3Uri(manifest.s3Uri())
                                .split(manifest.splitAsString())
                                .build())
                        .collect(Collectors.toSet()))
                .documentType(inputDataConfig.documentTypeAsString())
                .documents(inputDataConfig.documents() == null ? null : software.amazon.comprehend.documentclassifier.DocumentClassifierDocuments.builder()
                        .s3Uri(inputDataConfig.documents().s3Uri())
                        .testS3Uri(inputDataConfig.documents().testS3Uri())
                        .build())
                .documentReaderConfig(inputDataConfig.documentReaderConfig() == null ? null : software.amazon.comprehend.documentclassifier.DocumentReaderConfig.builder()
                        .documentReadAction(inputDataConfig.documentReaderConfig().documentReadActionAsString())
                        .documentReadMode(inputDataConfig.documentReaderConfig().documentReadModeAsString())
                        .featureTypes(streamOfOrEmpty(inputDataConfig.documentReaderConfig().featureTypesAsStrings())
                                .collect(Collectors.toSet()))
                        .build())
                .build();
    }

    public static software.amazon.comprehend.documentclassifier.DocumentClassifierOutputDataConfig fromSdkOutputDataConfig(DocumentClassifierOutputDataConfig outputDataConfig) {
        return outputDataConfig == null ? null: software.amazon.comprehend.documentclassifier.DocumentClassifierOutputDataConfig.builder()
                .kmsKeyId(outputDataConfig.kmsKeyId())
                .s3Uri(outputDataConfig.s3Uri() == null ? null : trimOutputS3Uri(outputDataConfig.s3Uri()))
                .build();
    }

    public static Set<software.amazon.comprehend.documentclassifier.Tag> fromSdkTags(Collection<Tag> tags) {
        return streamOfOrEmpty(tags).map(t ->
                        software.amazon.comprehend.documentclassifier.Tag.builder()
                                .key(t.key())
                                .value(t.value())
                                .build())
                .collect(Collectors.toSet());
    }

    
    /* * * * * * * * * * * * * * * * *
     *  Translation To SDK Requests  *
     * * * * * * * * * * * * * * * * */

    /**
     * Request to create a resource
     * @param model resource model
     * @return awsRequest the aws service request to create a resource
     */
    public static CreateDocumentClassifierRequest translateToCreateRequest(
            final ResourceModel model, final String clientRequestToken,
            final Set<Tag> tags
    ) {
        
        return CreateDocumentClassifierRequest.builder()
                .documentClassifierName(model.getDocumentClassifierName())
                .dataAccessRoleArn(model.getDataAccessRoleArn())
                .volumeKmsKeyId(model.getVolumeKmsKeyId())
                .vpcConfig(toSdkVpcConfig(model.getVpcConfig()))
                .versionName(model.getVersionName())
                .modelKmsKeyId(model.getModelKmsKeyId())
                .languageCode(model.getLanguageCode())
                .inputDataConfig(toSdkInputDataConfig(model.getInputDataConfig()))
                .clientRequestToken(clientRequestToken)
                .modelPolicy(model.getModelPolicy())
                .mode(model.getMode())
                .outputDataConfig(toSdkOutputDataConfig(model.getOutputDataConfig()))
                .tags(tags)
                .build();
    }

    /**
     * Request to read a resource
     * @param model resource model
     * @return awsRequest the aws service request to describe a resource
     */
    static DescribeDocumentClassifierRequest translateToReadRequest(final ResourceModel model) {
        return DescribeDocumentClassifierRequest.builder()
                .documentClassifierArn(model.getArn())
                .build();
    }

    /**
     * Request to delete a resource
     * @param model resource model
     * @return awsRequest the aws service request to delete a resource
     */
    static DeleteDocumentClassifierRequest translateToDeleteRequest(final ResourceModel model) {
        return DeleteDocumentClassifierRequest.builder()
                .documentClassifierArn(model.getArn())
                .build();
    }

    /**
     * Request to list resources
     * @param nextToken token passed to the aws service list resources request
     * @return awsRequest the aws service request to list resources within aws account
     */
    static ListDocumentClassifiersRequest translateToListRequest(final String nextToken) {
        return ListDocumentClassifiersRequest.builder()
                .nextToken(nextToken)
                .build();
    }

    /**
     * Request to attach a resource policy to a resource
     * @param model resource model
     * @return awsRequest the aws service request to put the resource policy on a resource
     */    
    public static PutResourcePolicyRequest translateToPutResourcePolicyRequest(final ResourceModel model) {
        return PutResourcePolicyRequest.builder()
                .resourceArn(model.getArn())
                .resourcePolicy(model.getModelPolicy())
                .build();
    }

    /**
     * Request to describe the resource policy of a resource
     * @param model resource model
     * @return awsRequest the aws service request to describe the resource policy of a resource
     */
    static DescribeResourcePolicyRequest translateToDescribeResourcePolicyRequest(final ResourceModel model) {
        return DescribeResourcePolicyRequest.builder()
                .resourceArn(model.getArn())
                .build();
    }

    /**
     * Request to delete the resource policy of a resource
     * @param model resource model
     * @return awsRequest the aws service request to delete the resource policy of a resource
     */
    static DeleteResourcePolicyRequest translateToDeleteResourcePolicyRequest(final ResourceModel model) {
        return DeleteResourcePolicyRequest.builder()
                .resourceArn(model.getArn())
                .build();
    }
    
    /**
     * Request to add tags to a resource
     * @param model resource model
     * @return awsRequest the aws service request to tag a resource
     */
    static TagResourceRequest translateToTagResourceRequest(final ResourceModel model, Set<Tag> currentTags, Set<Tag> desiredTags) {
        Set<Tag> setOfCurrentTags = Sets.newHashSet(currentTags);
        Set<Tag> setOfDesiredTags = Sets.newHashSet(desiredTags);
        List<Tag> tagsToAdd = new ArrayList<>(Sets.difference(setOfDesiredTags, setOfCurrentTags));

        return TagResourceRequest.builder()
                .resourceArn(model.getArn())
                .tags(tagsToAdd)
                .build();
    }

    /**
     * Request to add tags to a resource
     * @param model resource model
     * @return awsRequest the aws service request to create a resource
     */
    static UntagResourceRequest translateToUntagResourceRequest(final ResourceModel model, Set<Tag> currentTags, Set<Tag> desiredTags) {
        Set<String> setOfCurrentTagKeys = streamOfOrEmpty(currentTags).map(Tag::key).collect(Collectors.toSet());
        Set<String> setOfDesiredTagKeys = streamOfOrEmpty(desiredTags).map(Tag::key).collect(Collectors.toSet());
        List<String> tagKeysToRemove = new ArrayList<>(Sets.difference(setOfCurrentTagKeys, setOfDesiredTagKeys));

        return UntagResourceRequest.builder()
                .resourceArn(model.getArn())
                .tagKeys(tagKeysToRemove)
                .build();
    }
    
    /**
     * Request to list tags for a resource
     * @param model resource model
     * @return awsRequest the aws service request to describe a resource
     */
    static ListTagsForResourceRequest translateToListTagsRequest(final software.amazon.comprehend.documentclassifier.ResourceModel model) {
        return ListTagsForResourceRequest.builder()
                .resourceArn(model.getArn())
                .build();
    }

    
    /* * * * * * * * * * * * * * * * * * *
     *  Translation From SDK Responses   *
     * * * * * * * * * * * * * * * * * * */

    /**
     * Translates resource object from sdk into a resource model
     * @param awsResponse the aws service describe resource response
     * @return model resource model
     */
    public static ResourceModel translateFromReadResponse(DescribeDocumentClassifierResponse awsResponse) {
        DocumentClassifierProperties properties = awsResponse.documentClassifierProperties();
        return ResourceModel.builder()
                .arn(properties.documentClassifierArn())
                .documentClassifierName(getDocumentClassifierNameFromArn(properties.documentClassifierArn()))
                .versionName(properties.versionName())
                .dataAccessRoleArn(properties.dataAccessRoleArn())
                .languageCode(properties.languageCodeAsString())
                .inputDataConfig(fromSdkInputDataConfig(properties.inputDataConfig()))
                .outputDataConfig(fromSdkOutputDataConfig(properties.outputDataConfig()))
                .modelKmsKeyId(properties.modelKmsKeyId())
                .volumeKmsKeyId(properties.volumeKmsKeyId())
                .vpcConfig(fromSdkVpcConfig(properties.vpcConfig()))
                .mode(properties.modeAsString())
                .build();
    }

    /**
     * Translates resource objects from sdk into a resource model (primary identifier only)
     * @param awsResponse the aws service describe resource response
     * @return list of resource models
     */
    static List<ResourceModel> translateFromListResponse(final ListDocumentClassifiersResponse awsResponse) {
        return streamOfOrEmpty(awsResponse.documentClassifierPropertiesList())
                .map(documentClassifierProperty -> ResourceModel.builder()
                        .arn(documentClassifierProperty.documentClassifierArn())
                        .build())
                .collect(Collectors.toList());
    }

    
    /* * * * * * * * * * *
     *  Helper Methods   *
     * * * * * * * * * * */

    /**
     * For output S3 uri to match the original output s3 uri that was in the input, need to trim the exact path suffix.
     * @param outputS3Uri the output S3 uri returned by DescribeDocumentClassifier
     * @return trimmed output S3 uri matching what the user passed in originally in CreateDocumentClassifier
     */
    private static String trimOutputS3Uri(final String outputS3Uri) {
        return outputS3Uri.replaceAll("\\d{12}-CLR-[0-9a-f]{32}\\/output\\/output.tar.gz$", "");
    }

    /**
     * Get the document classifier name without version name from the arn.
     * @param documentClassifierArn the arn of the DocumentClassifier
     * @return document classifier name without version name
     */
    private static String getDocumentClassifierNameFromArn(final String documentClassifierArn) {
        String documentClassifierName = StringUtils.substringAfter(documentClassifierArn, "/");
        return StringUtils.substringBefore(documentClassifierName, "/version/");
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }
}
