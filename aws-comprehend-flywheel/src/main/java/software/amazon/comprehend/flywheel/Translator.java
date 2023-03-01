package software.amazon.comprehend.flywheel;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.services.comprehend.model.CreateFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DataSecurityConfig;
import software.amazon.awssdk.services.comprehend.model.DeleteFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.DocumentClassificationConfig;
import software.amazon.awssdk.services.comprehend.model.EntityRecognitionConfig;
import software.amazon.awssdk.services.comprehend.model.FlywheelProperties;
import software.amazon.awssdk.services.comprehend.model.ListFlywheelsRequest;
import software.amazon.awssdk.services.comprehend.model.ListFlywheelsResponse;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.awssdk.services.comprehend.model.TagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.TaskConfig;
import software.amazon.awssdk.services.comprehend.model.EntityTypesListItem;
import software.amazon.awssdk.services.comprehend.model.UntagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.UpdateDataSecurityConfig;
import software.amazon.awssdk.services.comprehend.model.UpdateFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.VpcConfig;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * This class is a centralized placeholder for:
 *  - api request construction
 *  - object translation to/from aws sdk
 *  - resource model construction for read/list handlers
 */
public class Translator {


  /* * * * * * * * * * * * *
   *  Translation To SDK   *
   * * * * * * * * * * * * */

  static TaskConfig toSdkTaskConfig(final software.amazon.comprehend.flywheel.TaskConfig taskConfig) {
    software.amazon.comprehend.flywheel.DocumentClassificationConfig documentClassificationConfig = taskConfig.getDocumentClassificationConfig();
    software.amazon.comprehend.flywheel.EntityRecognitionConfig entityRecognitionConfig = taskConfig.getEntityRecognitionConfig();
    return TaskConfig.builder()
            .languageCode(taskConfig.getLanguageCode())
            .documentClassificationConfig(documentClassificationConfig == null ? null :
                    DocumentClassificationConfig.builder()
                            .mode(documentClassificationConfig.getMode())
                            .labels(streamOfOrEmpty(documentClassificationConfig.getLabels())
                                    .collect(Collectors.toList()))
                            .build())
            .entityRecognitionConfig(entityRecognitionConfig == null ? null :
                    EntityRecognitionConfig.builder()
                            .entityTypes(streamOfOrEmpty(entityRecognitionConfig.getEntityTypes())
                                    .map(entityType -> EntityTypesListItem.builder().type(entityType.getType()).build())
                                    .collect(Collectors.toSet()))
                            .build())
            .build();
  }

  static DataSecurityConfig toSdkDataSecurityConfigCreate(final software.amazon.comprehend.flywheel.DataSecurityConfig dataSecurityConfig) {
    return dataSecurityConfig == null ? null : DataSecurityConfig.builder()
            .dataLakeKmsKeyId(dataSecurityConfig.getDataLakeKmsKeyId())
            .modelKmsKeyId(dataSecurityConfig.getModelKmsKeyId())
            .volumeKmsKeyId(dataSecurityConfig.getVolumeKmsKeyId())
            .vpcConfig(toSdkVpcConfig(dataSecurityConfig.getVpcConfig()))
            .build();
  }

  static UpdateDataSecurityConfig toSdkDataSecurityConfigUpdate(final software.amazon.comprehend.flywheel.DataSecurityConfig dataSecurityConfig) {
    return dataSecurityConfig == null ? null : UpdateDataSecurityConfig.builder()
            .modelKmsKeyId(dataSecurityConfig.getModelKmsKeyId())
            .volumeKmsKeyId(dataSecurityConfig.getVolumeKmsKeyId())
            .vpcConfig(toSdkVpcConfig(dataSecurityConfig.getVpcConfig()))
            .build();
  }

  static VpcConfig toSdkVpcConfig(final software.amazon.comprehend.flywheel.VpcConfig vpcConfig) {
    return vpcConfig == null ? null : VpcConfig.builder()
            .securityGroupIds(vpcConfig.getSecurityGroupIds())
            .subnets(vpcConfig.getSubnets())
            .build();
  }

  public static Collection<Tag> toSdkTags(Set<software.amazon.comprehend.flywheel.Tag> tags) {
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

  static software.amazon.comprehend.flywheel.TaskConfig fromSdkTaskConfig(final TaskConfig taskConfig) {
    DocumentClassificationConfig documentClassificationConfig = taskConfig.documentClassificationConfig();
    EntityRecognitionConfig entityRecognitionConfig = taskConfig.entityRecognitionConfig();
    return software.amazon.comprehend.flywheel.TaskConfig.builder()
            .languageCode(taskConfig.languageCodeAsString())
            .documentClassificationConfig(documentClassificationConfig == null ? null :
                    software.amazon.comprehend.flywheel.DocumentClassificationConfig.builder()
                            .mode(documentClassificationConfig.modeAsString())
                            .labels(streamOfOrEmpty(documentClassificationConfig.labels())
                                    .collect(Collectors.toSet()))
                            .build())
            .entityRecognitionConfig(entityRecognitionConfig == null ? null :
                    software.amazon.comprehend.flywheel.EntityRecognitionConfig.builder()
                            .entityTypes(streamOfOrEmpty(entityRecognitionConfig.entityTypes())
                                    .map(entityType -> software.amazon.comprehend.flywheel.EntityTypesListItem.builder()
                                            .type(entityType.type())
                                            .build())
                                    .collect(Collectors.toSet()))
                            .build())
            .build();
  }

  static software.amazon.comprehend.flywheel.VpcConfig fromVpcConfig(final VpcConfig vpcConfig) {
    return vpcConfig == null ? null : software.amazon.comprehend.flywheel.VpcConfig.builder()
            .securityGroupIds(streamOfOrEmpty(vpcConfig.securityGroupIds()).collect(Collectors.toSet()))
            .subnets(streamOfOrEmpty(vpcConfig.subnets()).collect(Collectors.toSet()))
            .build();
  }

  static software.amazon.comprehend.flywheel.DataSecurityConfig fromSdkDataSecurityConfig(final DataSecurityConfig dataSecurityConfig) {
    return dataSecurityConfig == null ? null : software.amazon.comprehend.flywheel.DataSecurityConfig.builder()
            .dataLakeKmsKeyId(dataSecurityConfig.dataLakeKmsKeyId())
            .modelKmsKeyId(dataSecurityConfig.modelKmsKeyId())
            .volumeKmsKeyId(dataSecurityConfig.volumeKmsKeyId())
            .vpcConfig(fromVpcConfig(dataSecurityConfig.vpcConfig()))
            .build();
  }

  public static Set<software.amazon.comprehend.flywheel.Tag> fromSdkTags(Collection<Tag> tags) {
    return streamOfOrEmpty(tags).map(t ->
            software.amazon.comprehend.flywheel.Tag.builder()
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
  static CreateFlywheelRequest translateToCreateRequest(
          final ResourceModel model, final String clientRequestToken,
          final Set<Tag> tags
  ) {

    return CreateFlywheelRequest.builder()
            .flywheelName(model.getFlywheelName())
            .dataAccessRoleArn(model.getDataAccessRoleArn())
            .dataLakeS3Uri(model.getDataLakeS3Uri())
            .modelType(model.getModelType())
            .taskConfig(toSdkTaskConfig(model.getTaskConfig()))
            .dataSecurityConfig(toSdkDataSecurityConfigCreate(model.getDataSecurityConfig()))
            .activeModelArn(model.getActiveModelArn())
            .clientRequestToken(clientRequestToken)
            .tags(tags)
            .build();
  }

  /**
   * Request to read a resource
   * @param model resource model
   * @return awsRequest the aws service request to describe a resource
   */
  static DescribeFlywheelRequest translateToReadRequest(final ResourceModel model) {
    return DescribeFlywheelRequest.builder()
            .flywheelArn(model.getArn())
            .build();
  }

  /**
   * Request to update properties of a previously created resource
   * @param model resource model
   * @return awsRequest the aws service request to modify a resource
   */
  static UpdateFlywheelRequest translateToUpdateRequest(final ResourceModel model) {
    return UpdateFlywheelRequest.builder()
            .flywheelArn(model.getArn())
            .activeModelArn(model.getActiveModelArn())
            .dataAccessRoleArn(model.getDataAccessRoleArn())
            .dataSecurityConfig(toSdkDataSecurityConfigUpdate(model.getDataSecurityConfig()))
            .build();
  }

  /**
   * Request to delete a resource
   * @param model resource model
   * @return awsRequest the aws service request to delete a resource
   */
  static DeleteFlywheelRequest translateToDeleteRequest(final ResourceModel model) {
    return DeleteFlywheelRequest.builder()
            .flywheelArn(model.getArn())
            .build();
  }

  /**
   * Request to list resources
   * @param nextToken token passed to the aws service list resources request
   * @return awsRequest the aws service request to list resources within aws account
   */
  static ListFlywheelsRequest translateToListRequest(final String nextToken) {
    return ListFlywheelsRequest.builder()
            .nextToken(nextToken)
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
  static ListTagsForResourceRequest translateToListTagsRequest(final ResourceModel model) {
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
  static ResourceModel translateFromReadResponse(final DescribeFlywheelResponse awsResponse) {
    FlywheelProperties flywheelProperties = awsResponse.flywheelProperties();
    String flywheelArn = flywheelProperties.flywheelArn();
    ResourceModel model = ResourceModel.builder()
            .arn(flywheelArn)
            .dataAccessRoleArn(flywheelProperties.dataAccessRoleArn())
            .dataLakeS3Uri(trimDataLakeS3Uri(flywheelProperties.dataLakeS3Uri(), flywheelArn))
            .dataSecurityConfig(fromSdkDataSecurityConfig(flywheelProperties.dataSecurityConfig()))
            .flywheelName(flywheelArn.substring(flywheelArn.indexOf("/") + 1))
            .modelType(flywheelProperties.modelTypeAsString())
            .taskConfig(fromSdkTaskConfig(flywheelProperties.taskConfig()))
            .build();

    if (flywheelProperties.activeModelArn() != null) model.setActiveModelArn(flywheelProperties.activeModelArn());

    return model;
  }

  /**
   * Translates resource objects from sdk into a resource model (primary identifier only)
   * @param awsResponse the aws service describe resource response
   * @return list of resource models
   */
  static List<ResourceModel> translateFromListResponse(final ListFlywheelsResponse awsResponse) {
    return streamOfOrEmpty(awsResponse.flywheelSummaryList())
            .map(flywheelSummary -> ResourceModel.builder()
                    .arn(flywheelSummary.flywheelArn())
                    .build())
            .collect(Collectors.toList());
  }


  /* * * * * * * * * * *
   *  Helper Methods   *
   * * * * * * * * * * */

  /**
   * For output DataLake S3 uri to match the input uri, need to trim the flywheel name and timestamp suffix
   * @param dataLakeS3Uri the DataLake S3 returned by DescribeFlywheel
   * @param flywheelArn the arn of the flywheel from which to derive the flywheel name
   * @return trimmed DataLake S3 uri matching what the user passed in originally in CreateFlywheel
   */
  private static String trimDataLakeS3Uri(final String dataLakeS3Uri, final String flywheelArn) {
    String flywheelName = flywheelArn.substring(flywheelArn.lastIndexOf('/') + 1);
    int splitIndex = dataLakeS3Uri.lastIndexOf(flywheelName + "/");
    return splitIndex > 0 ? dataLakeS3Uri.substring(0, splitIndex) : dataLakeS3Uri;
  }

  private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
    return Optional.ofNullable(collection)
        .map(Collection::stream)
        .orElseGet(Stream::empty);
  }

}
