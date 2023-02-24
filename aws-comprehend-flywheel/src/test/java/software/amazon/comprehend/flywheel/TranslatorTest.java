package software.amazon.comprehend.flywheel;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.comprehend.model.CreateFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DataSecurityConfig;
import software.amazon.awssdk.services.comprehend.model.DeleteFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeFlywheelResponse;
import software.amazon.awssdk.services.comprehend.model.DocumentClassificationConfig;
import software.amazon.awssdk.services.comprehend.model.FlywheelProperties;
import software.amazon.awssdk.services.comprehend.model.ListFlywheelsResponse;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.ModelType;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.awssdk.services.comprehend.model.TagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.TaskConfig;
import software.amazon.awssdk.services.comprehend.model.UntagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.UpdateDataSecurityConfig;
import software.amazon.awssdk.services.comprehend.model.UpdateFlywheelRequest;
import software.amazon.awssdk.services.comprehend.model.VpcConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(MockitoExtension.class)
public class TranslatorTest extends AbstractTestBase {

    /* Test SDK objects */

    private static final DocumentClassificationConfig SDK_CLR_CONFIG_MODE_ONLY =
            DocumentClassificationConfig.builder()
                    .mode("MULTI_CLASS")
                    .build();

    private static final DataSecurityConfig SDK_DATA_SECURITY_CONFIG_S3_KEY_ONLY =
            DataSecurityConfig.builder()
                    .dataLakeKmsKeyId("test-dataLakeKmsKeyId")
                    .build();
    private static final DataSecurityConfig SDK_DATA_SECURITY_CONFIG_MODEL_KEY_ONLY =
            DataSecurityConfig.builder()
                    .modelKmsKeyId("test-modelKmsKeyId")
                    .build();
    private static final DataSecurityConfig SDK_DATA_SECURITY_CONFIG_VOLUME_KEY_ONLY =
            DataSecurityConfig.builder()
                    .volumeKmsKeyId("test-volumeKmsKeyId")
                    .build();
    private static final DataSecurityConfig SDK_DATA_SECURITY_CONFIG_VPC_ONLY =
            DataSecurityConfig.builder()
                    .vpcConfig(VpcConfig.builder()
                            .securityGroupIds("group1", "group2")
                            .subnets("subnet1", "subnet2")
                            .build())
                    .build();

    private static final Set<Tag> SDK_TAGS = new HashSet<>(Arrays.asList(
            Tag.builder().key("key1").value("value1").build(),
            Tag.builder().key("key2").value("value2").build(),
            Tag.builder().key("key3").value("value3").build()));

    private static final Set<Tag> SDK_TAGS_UPDATED = new HashSet<>(Arrays.asList(
            Tag.builder().key("key1").value("newValue1").build(),
            Tag.builder().key("key2").value("value2").build(),
            Tag.builder().key("key4").value("value4").build(),
            Tag.builder().key("key5").value("value5").build()));

    private static final Set<Tag> SDK_TAGS_EMPTY = new HashSet<>();

    protected static final FlywheelProperties FLYWHEEL_PROPERTIES_MINIMAL = FlywheelProperties.builder()
            .flywheelArn(TEST_FLYWHEEL_ARN)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .dataLakeS3Uri(TEST_OUTPUT_DATA_LAKE_S3_URI)
            .taskConfig(TaskConfig.builder()
                    .languageCode(TEST_LANGUAGE_CODE)
                    .documentClassificationConfig(SDK_CLR_CONFIG_MODE_ONLY)
                    .build())
            .modelType(ModelType.DOCUMENT_CLASSIFIER.toString())
            .build();


    /* Test CFN model objects */

    private static final software.amazon.comprehend.flywheel.DocumentClassificationConfig CFN_CLR_CONFIG_MODE_ONLY =
            software.amazon.comprehend.flywheel.DocumentClassificationConfig.builder()
                    .mode(SDK_CLR_CONFIG_MODE_ONLY.modeAsString())
                    .build();
    private static final software.amazon.comprehend.flywheel.DocumentClassificationConfig CFN_CLR_CONFIG =
            software.amazon.comprehend.flywheel.DocumentClassificationConfig.builder()
                    .mode(SDK_CLR_CONFIG_MODE_LABELS.modeAsString())
                    .labels(new HashSet<>(SDK_CLR_CONFIG_MODE_LABELS.labels()))
                    .build();
    private static final software.amazon.comprehend.flywheel.EntityRecognitionConfig CFN_ER_CONFIG =
            software.amazon.comprehend.flywheel.EntityRecognitionConfig.builder()
                    .entityTypes(SDK_ER_CONFIG.entityTypes().stream().map(e -> software.amazon.comprehend.flywheel.
                                    EntityTypesListItem.builder().type(e.type()).build())
                            .collect(Collectors.toSet()))
                    .build();

    private static final software.amazon.comprehend.flywheel.DataSecurityConfig CFN_DATA_SECURITY_CONFIG_S3_KEY_ONLY =
            software.amazon.comprehend.flywheel.DataSecurityConfig.builder()
                    .dataLakeKmsKeyId("test-dataLakeKmsKeyId")
                    .build();
    private static final software.amazon.comprehend.flywheel.DataSecurityConfig CFN_DATA_SECURITY_CONFIG_MODEL_KEY_ONLY =
            software.amazon.comprehend.flywheel.DataSecurityConfig.builder()
                    .modelKmsKeyId("test-modelKmsKeyId")
                    .build();
    private static final software.amazon.comprehend.flywheel.DataSecurityConfig CFN_DATA_SECURITY_CONFIG_VOLUME_KEY_ONLY =
            software.amazon.comprehend.flywheel.DataSecurityConfig.builder()
                    .volumeKmsKeyId("test-volumeKmsKeyId")
                    .build();
    private static final software.amazon.comprehend.flywheel.DataSecurityConfig CFN_DATA_SECURITY_CONFIG_VPC_ONLY =
            software.amazon.comprehend.flywheel.DataSecurityConfig.builder()
                    .vpcConfig(software.amazon.comprehend.flywheel.VpcConfig.builder()
                            .securityGroupIds(new HashSet<>(Arrays.asList("group1", "group2")))
                            .subnets(new HashSet<>(Arrays.asList("subnet1", "subnet2")))
                            .build())
                    .build();
    private static final software.amazon.comprehend.flywheel.DataSecurityConfig CFN_DATA_SECURITY_CONFIG =
            software.amazon.comprehend.flywheel.DataSecurityConfig.builder()
                    .dataLakeKmsKeyId("test-dataLakeKmsKeyId")
                    .modelKmsKeyId("test-modelKmsKeyId")
                    .volumeKmsKeyId("test-volumeKmsKeyId")
                    .vpcConfig(software.amazon.comprehend.flywheel.VpcConfig.builder()
                            .securityGroupIds(new HashSet<>(Arrays.asList("group1", "group2")))
                            .subnets(new HashSet<>(Arrays.asList("subnet1", "subnet2")))
                            .build())
                    .build();

    private final Set<software.amazon.comprehend.flywheel.Tag> CFN_TAGS = new HashSet<>(Arrays.asList(
            software.amazon.comprehend.flywheel.Tag.builder().key("key1").value("value1").build(),
            software.amazon.comprehend.flywheel.Tag.builder().key("key2").value("value2").build(),
            software.amazon.comprehend.flywheel.Tag.builder().key("key3").value("value3").build()));


    /* Test CFN flywheel resource models */

    private final ResourceModel MINIMAL_ER_FLYWHEEL_CFN_MODEL = ResourceModel.builder()
            .flywheelName(TEST_FLYWHEEL_NAME)
            .modelType(ModelType.ENTITY_RECOGNIZER.toString())
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .dataLakeS3Uri(TEST_DATA_LAKE_S3_URI)
            .taskConfig(software.amazon.comprehend.flywheel.TaskConfig.builder()
                    .languageCode(TEST_LANGUAGE_CODE)
                    .entityRecognitionConfig(CFN_ER_CONFIG)
                    .build())
            .build();

    private final ResourceModel MINIMAL_CLR_FLYWHEEL_CFN_MODEL = ResourceModel.builder()
            .flywheelName(TEST_FLYWHEEL_NAME)
            .modelType(ModelType.DOCUMENT_CLASSIFIER.toString())
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .dataLakeS3Uri(TEST_DATA_LAKE_S3_URI)
            .taskConfig(software.amazon.comprehend.flywheel.TaskConfig.builder()
                    .languageCode(TEST_LANGUAGE_CODE)
                    .documentClassificationConfig(CFN_CLR_CONFIG_MODE_ONLY)
                    .build())
            .build();

    private final ResourceModel FULL_CLR_FLYWHEEL_CFN_MODEL = ResourceModel.builder()
            .flywheelName(TEST_FLYWHEEL_NAME)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .dataLakeS3Uri(TEST_DATA_LAKE_S3_URI)
            .taskConfig(software.amazon.comprehend.flywheel.TaskConfig.builder()
                    .languageCode(TEST_LANGUAGE_CODE)
                    .documentClassificationConfig(CFN_CLR_CONFIG)
                    .build())
            .activeModelArn(TEST_ACTIVE_MODEL_ARN)
            .dataSecurityConfig(CFN_DATA_SECURITY_CONFIG)
            .tags(CFN_TAGS)
            .build();

    private final ResourceModel FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY = ResourceModel.builder()
            .arn(TEST_FLYWHEEL_ARN)
            .build();

    
    @Test
    public void testToAndFromSdkTaskConfig() {
        // Test CLR config without labels
        software.amazon.comprehend.flywheel.TaskConfig translatedFromSdkTaskConfigClr = Translator.fromSdkTaskConfig(TaskConfig.builder()
                .languageCode(TEST_LANGUAGE_CODE)
                .documentClassificationConfig(SDK_CLR_CONFIG_MODE_ONLY)
                .build());
        assertThat(translatedFromSdkTaskConfigClr.getLanguageCode()).isEqualTo(TEST_LANGUAGE_CODE);
        assertThat(translatedFromSdkTaskConfigClr.getDocumentClassificationConfig().getMode()).isEqualTo(SDK_CLR_CONFIG_MODE_ONLY.modeAsString());
        assertThat(translatedFromSdkTaskConfigClr.getDocumentClassificationConfig().getLabels()).isEmpty();
        assertThat(translatedFromSdkTaskConfigClr.getEntityRecognitionConfig()).isNull();

        TaskConfig translatedToSdkTaskConfigClr = Translator.toSdkTaskConfig(software.amazon.comprehend.flywheel.TaskConfig.builder()
                .languageCode(TEST_LANGUAGE_CODE)
                .documentClassificationConfig(CFN_CLR_CONFIG_MODE_ONLY)
                .build());
        assertThat(translatedToSdkTaskConfigClr.languageCodeAsString()).isEqualTo(TEST_LANGUAGE_CODE);
        assertThat(translatedToSdkTaskConfigClr.documentClassificationConfig().modeAsString()).isEqualTo(CFN_CLR_CONFIG_MODE_ONLY.getMode());
        assertThat(translatedToSdkTaskConfigClr.documentClassificationConfig().labels()).isEmpty();
        assertThat(translatedToSdkTaskConfigClr.entityRecognitionConfig()).isNull();


        // Test CLR config with labels
        software.amazon.comprehend.flywheel.TaskConfig translatedFromSdkTaskConfigClrWithLabels = Translator.fromSdkTaskConfig(SDK_CLR_TASK_CONFIG);
        assertThat(translatedFromSdkTaskConfigClrWithLabels.getDocumentClassificationConfig().getLabels())
                .isEqualTo(new HashSet<>(SDK_CLR_CONFIG_MODE_LABELS.labels()));

        TaskConfig translatedToSdkTaskConfigClrWithLabels = Translator.toSdkTaskConfig(software.amazon.comprehend.flywheel.TaskConfig.builder()
                .languageCode(TEST_LANGUAGE_CODE)
                .documentClassificationConfig(CFN_CLR_CONFIG)
                .build());
        assertThat(translatedToSdkTaskConfigClrWithLabels.documentClassificationConfig().labels())
                .isEqualTo(new ArrayList<>(CFN_CLR_CONFIG.getLabels()));


        // Test ER config
        software.amazon.comprehend.flywheel.TaskConfig translatedFromSdkTaskConfigER = Translator.fromSdkTaskConfig(TaskConfig.builder()
                .languageCode(TEST_LANGUAGE_CODE)
                .entityRecognitionConfig(SDK_ER_CONFIG)
                .build());
        assertThat(translatedFromSdkTaskConfigER.getLanguageCode()).isEqualTo(TEST_LANGUAGE_CODE);
        assertThat(translatedFromSdkTaskConfigER.getEntityRecognitionConfig().getEntityTypes())
                .isEqualTo(CFN_ER_CONFIG.getEntityTypes());
        assertThat(translatedFromSdkTaskConfigER.getDocumentClassificationConfig()).isNull();

        TaskConfig translatedToSdkTaskConfigEr = Translator.toSdkTaskConfig(software.amazon.comprehend.flywheel.TaskConfig.builder()
                .languageCode(TEST_LANGUAGE_CODE)
                .entityRecognitionConfig(CFN_ER_CONFIG)
                .build());
        assertThat(translatedToSdkTaskConfigEr.languageCodeAsString()).isEqualTo(TEST_LANGUAGE_CODE);
        assertThat(translatedToSdkTaskConfigEr.entityRecognitionConfig().entityTypes()).isEqualTo(
                SDK_ER_CONFIG.entityTypes());
        assertThat(translatedToSdkTaskConfigEr.documentClassificationConfig()).isNull();
    }

    @Test
    public void testToAndFromSdkDataSecurityConfig() {
        // Test data lake kms key
        software.amazon.comprehend.flywheel.DataSecurityConfig translatedFromSdkSecurityConfigDataLakeKey =
                Translator.fromSdkDataSecurityConfig(SDK_DATA_SECURITY_CONFIG_S3_KEY_ONLY);
        assertThat(translatedFromSdkSecurityConfigDataLakeKey).isEqualTo(CFN_DATA_SECURITY_CONFIG_S3_KEY_ONLY);
        assertThat(translatedFromSdkSecurityConfigDataLakeKey.getModelKmsKeyId()).isNull();
        assertThat(translatedFromSdkSecurityConfigDataLakeKey.getVolumeKmsKeyId()).isNull();
        assertThat(translatedFromSdkSecurityConfigDataLakeKey.getVpcConfig()).isNull();

        DataSecurityConfig translatedToSdkSecurityConfigDataLakeKeyCreate =
                Translator.toSdkDataSecurityConfigCreate(CFN_DATA_SECURITY_CONFIG_S3_KEY_ONLY);
        assertThat(translatedToSdkSecurityConfigDataLakeKeyCreate).isEqualTo(SDK_DATA_SECURITY_CONFIG_S3_KEY_ONLY);
        assertThat(translatedToSdkSecurityConfigDataLakeKeyCreate.modelKmsKeyId()).isNull();
        assertThat(translatedToSdkSecurityConfigDataLakeKeyCreate.volumeKmsKeyId()).isNull();
        assertThat(translatedToSdkSecurityConfigDataLakeKeyCreate.vpcConfig()).isNull();

        UpdateDataSecurityConfig translatedToSdkSecurityConfigDataLakeKeyUpdate =
                Translator.toSdkDataSecurityConfigUpdate(CFN_DATA_SECURITY_CONFIG_S3_KEY_ONLY);
        assertThat(translatedToSdkSecurityConfigDataLakeKeyUpdate.modelKmsKeyId()).isNull();
        assertThat(translatedToSdkSecurityConfigDataLakeKeyCreate.volumeKmsKeyId()).isNull();
        assertThat(translatedToSdkSecurityConfigDataLakeKeyCreate.vpcConfig()).isNull();


        // Test model kms key
        software.amazon.comprehend.flywheel.DataSecurityConfig translatedFromSdkSecurityConfigModelKey =
                Translator.fromSdkDataSecurityConfig(SDK_DATA_SECURITY_CONFIG_MODEL_KEY_ONLY);
        assertThat(translatedFromSdkSecurityConfigModelKey).isEqualTo(CFN_DATA_SECURITY_CONFIG_MODEL_KEY_ONLY);

        DataSecurityConfig translatedToSdkSecurityConfigModelKeyCreate =
                Translator.toSdkDataSecurityConfigCreate(CFN_DATA_SECURITY_CONFIG_MODEL_KEY_ONLY);
        assertThat(translatedToSdkSecurityConfigModelKeyCreate).isEqualTo(SDK_DATA_SECURITY_CONFIG_MODEL_KEY_ONLY);

        UpdateDataSecurityConfig translatedToSdkSecurityConfigModelKeyUpdate =
                Translator.toSdkDataSecurityConfigUpdate(CFN_DATA_SECURITY_CONFIG_MODEL_KEY_ONLY);
        assertThat(translatedToSdkSecurityConfigModelKeyUpdate.modelKmsKeyId())
                .isEqualTo(CFN_DATA_SECURITY_CONFIG_MODEL_KEY_ONLY.getModelKmsKeyId());

        // Test volume kms key
        software.amazon.comprehend.flywheel.DataSecurityConfig translatedFromSdkSecurityConfigVolumeKey =
                Translator.fromSdkDataSecurityConfig(SDK_DATA_SECURITY_CONFIG_VOLUME_KEY_ONLY);
        assertThat(translatedFromSdkSecurityConfigVolumeKey).isEqualTo(CFN_DATA_SECURITY_CONFIG_VOLUME_KEY_ONLY);

        DataSecurityConfig translatedToSdkSecurityConfigVolumeKeyCreate =
                Translator.toSdkDataSecurityConfigCreate(CFN_DATA_SECURITY_CONFIG_VOLUME_KEY_ONLY);
        assertThat(translatedToSdkSecurityConfigVolumeKeyCreate).isEqualTo(SDK_DATA_SECURITY_CONFIG_VOLUME_KEY_ONLY);

        UpdateDataSecurityConfig translatedToSdkSecurityConfigVolumeKeyUpdate =
                Translator.toSdkDataSecurityConfigUpdate(CFN_DATA_SECURITY_CONFIG_VOLUME_KEY_ONLY);
        assertThat(translatedToSdkSecurityConfigVolumeKeyUpdate.volumeKmsKeyId())
                .isEqualTo(CFN_DATA_SECURITY_CONFIG_VOLUME_KEY_ONLY.getVolumeKmsKeyId());

        // Test vpc config
        software.amazon.comprehend.flywheel.DataSecurityConfig translatedFromSdkSecurityConfigVpcConfig =
                Translator.fromSdkDataSecurityConfig(SDK_DATA_SECURITY_CONFIG_VPC_ONLY);
        assertThat(translatedFromSdkSecurityConfigVpcConfig.getVpcConfig().getSecurityGroupIds().stream().sorted().collect(Collectors.toList()))
                .isEqualTo(SDK_DATA_SECURITY_CONFIG_VPC_ONLY.vpcConfig().securityGroupIds().stream().sorted().collect(Collectors.toList()));
        assertThat(translatedFromSdkSecurityConfigVpcConfig.getVpcConfig().getSubnets().stream().sorted().collect(Collectors.toList()))
                .isEqualTo(SDK_DATA_SECURITY_CONFIG_VPC_ONLY.vpcConfig().subnets().stream().sorted().collect(Collectors.toList()));

        DataSecurityConfig translatedToSdkSecurityConfigVpcConfigCreate =
                Translator.toSdkDataSecurityConfigCreate(CFN_DATA_SECURITY_CONFIG_VPC_ONLY);
        assertThat(translatedToSdkSecurityConfigVpcConfigCreate.vpcConfig().securityGroupIds().stream().sorted().collect(Collectors.toList()))
                .isEqualTo(CFN_DATA_SECURITY_CONFIG_VPC_ONLY.getVpcConfig().getSecurityGroupIds().stream().sorted().collect(Collectors.toList()));
        assertThat(translatedToSdkSecurityConfigVpcConfigCreate.vpcConfig().subnets().stream().sorted().collect(Collectors.toList()))
                .isEqualTo(CFN_DATA_SECURITY_CONFIG_VPC_ONLY.getVpcConfig().getSubnets().stream().sorted().collect(Collectors.toList()));

        UpdateDataSecurityConfig translatedToSdkSecurityConfigVpcConfigUpdate =
                Translator.toSdkDataSecurityConfigUpdate(CFN_DATA_SECURITY_CONFIG_VPC_ONLY);
        assertThat(translatedToSdkSecurityConfigVpcConfigUpdate.vpcConfig().securityGroupIds().stream().sorted().collect(Collectors.toList()))
                .isEqualTo(CFN_DATA_SECURITY_CONFIG_VPC_ONLY.getVpcConfig().getSecurityGroupIds().stream().sorted().collect(Collectors.toList()));
        assertThat(translatedToSdkSecurityConfigVpcConfigUpdate.vpcConfig().subnets().stream().sorted().collect(Collectors.toList()))
                .isEqualTo(CFN_DATA_SECURITY_CONFIG_VPC_ONLY.getVpcConfig().getSubnets().stream().sorted().collect(Collectors.toList()));

    }

    @Test
    public void testToAndFromSdkTags() {
        Set<software.amazon.comprehend.flywheel.Tag> translatedFromSdkTags =
                Translator.fromSdkTags(SDK_TAGS);
        assertThat(translatedFromSdkTags).isEqualTo(CFN_TAGS);

        Collection<Tag> translatedToSdkTags =
                Translator.toSdkTags(CFN_TAGS);
        assertThat(new HashSet<>(translatedToSdkTags)).isEqualTo(new HashSet<>(SDK_TAGS));
    }

    @Test
    public void testToCreateRequest() {
        CreateFlywheelRequest minimalERFlywheelRequest = Translator.translateToCreateRequest(MINIMAL_ER_FLYWHEEL_CFN_MODEL, TEST_CLIENT_REQUEST_TOKEN, SDK_TAGS);
        assertThat(minimalERFlywheelRequest.flywheelName()).isEqualTo(MINIMAL_ER_FLYWHEEL_CFN_MODEL.getFlywheelName());
        assertThat(minimalERFlywheelRequest.modelTypeAsString()).isEqualTo(MINIMAL_ER_FLYWHEEL_CFN_MODEL.getModelType());
        assertThat(minimalERFlywheelRequest.dataAccessRoleArn()).isEqualTo(MINIMAL_ER_FLYWHEEL_CFN_MODEL.getDataAccessRoleArn());
        assertThat(minimalERFlywheelRequest.dataLakeS3Uri()).isEqualTo(MINIMAL_ER_FLYWHEEL_CFN_MODEL.getDataLakeS3Uri());
        assertThat(minimalERFlywheelRequest.clientRequestToken()).isEqualTo(TEST_CLIENT_REQUEST_TOKEN);
        assertThat(minimalERFlywheelRequest.taskConfig()).isEqualTo(Translator.toSdkTaskConfig(MINIMAL_ER_FLYWHEEL_CFN_MODEL.getTaskConfig()));

        CreateFlywheelRequest minimalCLRFlywheelRequest = Translator.translateToCreateRequest(MINIMAL_CLR_FLYWHEEL_CFN_MODEL, TEST_CLIENT_REQUEST_TOKEN, SDK_TAGS);
        assertThat(minimalCLRFlywheelRequest.modelTypeAsString()).isEqualTo(MINIMAL_CLR_FLYWHEEL_CFN_MODEL.getModelType());
        assertThat(minimalCLRFlywheelRequest.taskConfig()).isEqualTo(Translator.toSdkTaskConfig(MINIMAL_CLR_FLYWHEEL_CFN_MODEL.getTaskConfig()));

        CreateFlywheelRequest fullCLRFlywheelRequest = Translator.translateToCreateRequest(FULL_CLR_FLYWHEEL_CFN_MODEL, TEST_CLIENT_REQUEST_TOKEN, SDK_TAGS);
        assertThat(fullCLRFlywheelRequest.taskConfig()).isEqualTo(Translator.toSdkTaskConfig(FULL_CLR_FLYWHEEL_CFN_MODEL.getTaskConfig()));
        assertThat(fullCLRFlywheelRequest.dataSecurityConfig()).isEqualTo(Translator.toSdkDataSecurityConfigCreate(FULL_CLR_FLYWHEEL_CFN_MODEL.getDataSecurityConfig()));
        assertThat(fullCLRFlywheelRequest.activeModelArn()).isEqualTo(FULL_CLR_FLYWHEEL_CFN_MODEL.getActiveModelArn());
        assertThat(new HashSet<>(fullCLRFlywheelRequest.tags())).isEqualTo(new HashSet<>(Translator.toSdkTags(FULL_CLR_FLYWHEEL_CFN_MODEL.getTags())));
    }

    @Test
    public void testToReadFlywheelRequest() {
        DescribeFlywheelRequest describeFlywheelRequest = Translator.translateToReadRequest(FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY);
        assertThat(describeFlywheelRequest.flywheelArn()).isEqualTo(FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY.getArn());
    }

    @Test
    public void testFromReadFlywheelResponse() {
        ResourceModel flywheelModelMinimalProperties = Translator.translateFromReadResponse(DescribeFlywheelResponse.builder()
                .flywheelProperties(FLYWHEEL_PROPERTIES_MINIMAL)
                .build());
        assertThat(flywheelModelMinimalProperties.getArn()).isEqualTo(FLYWHEEL_PROPERTIES_MINIMAL.flywheelArn());
        assertThat(flywheelModelMinimalProperties.getDataAccessRoleArn()).isEqualTo(FLYWHEEL_PROPERTIES_MINIMAL.dataAccessRoleArn());
        assertThat(flywheelModelMinimalProperties.getDataLakeS3Uri()).isNotEqualTo(FLYWHEEL_PROPERTIES_MINIMAL.dataLakeS3Uri());
        assertThat(flywheelModelMinimalProperties.getDataLakeS3Uri()).isEqualTo(TEST_DATA_LAKE_S3_URI);
        assertThat(flywheelModelMinimalProperties.getModelType()).isEqualTo(FLYWHEEL_PROPERTIES_MINIMAL.modelTypeAsString());
        assertThat(flywheelModelMinimalProperties.getTaskConfig()).isEqualTo(Translator.fromSdkTaskConfig(FLYWHEEL_PROPERTIES_MINIMAL.taskConfig()));

        ResourceModel flywheelModelFullProperties = Translator.translateFromReadResponse(DescribeFlywheelResponse.builder()
                .flywheelProperties(TEST_FLYWHEEL_PROPERTIES_CREATING)
                .build());
        assertThat(flywheelModelFullProperties.getActiveModelArn()).isEqualTo(TEST_FLYWHEEL_PROPERTIES_CREATING.activeModelArn());
        assertThat(flywheelModelFullProperties.getTaskConfig()).isEqualTo(Translator.fromSdkTaskConfig(TEST_FLYWHEEL_PROPERTIES_CREATING.taskConfig()));
        assertThat(flywheelModelFullProperties.getDataSecurityConfig()).isEqualTo(
                Translator.fromSdkDataSecurityConfig(TEST_FLYWHEEL_PROPERTIES_CREATING.dataSecurityConfig()));
    }
    
    @Test
    public void testToUpdateRequest() {
        UpdateFlywheelRequest updateFlywheelRequestNoUpdate = Translator.translateToUpdateRequest(FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY);
        assertThat(updateFlywheelRequestNoUpdate.flywheelArn()).isEqualTo(FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY.getArn());
        assertThat(updateFlywheelRequestNoUpdate.dataAccessRoleArn()).isNull();
        assertThat(updateFlywheelRequestNoUpdate.dataSecurityConfig()).isNull();
        assertThat(updateFlywheelRequestNoUpdate.activeModelArn()).isNull();


        UpdateFlywheelRequest updateFlywheelRequestUpdateAll = Translator.translateToUpdateRequest(FULL_CLR_FLYWHEEL_CFN_MODEL);
        assertThat(updateFlywheelRequestUpdateAll.flywheelArn()).isEqualTo(FULL_CLR_FLYWHEEL_CFN_MODEL.getArn());
        assertThat(updateFlywheelRequestUpdateAll.dataAccessRoleArn()).isEqualTo(FULL_CLR_FLYWHEEL_CFN_MODEL.getDataAccessRoleArn());

        assertThat(updateFlywheelRequestUpdateAll.dataSecurityConfig()).isEqualTo(
                Translator.toSdkDataSecurityConfigUpdate(FULL_CLR_FLYWHEEL_CFN_MODEL.getDataSecurityConfig()));
        assertThat(updateFlywheelRequestUpdateAll.activeModelArn()).isEqualTo(FULL_CLR_FLYWHEEL_CFN_MODEL.getActiveModelArn());
    }

    @Test
    public void testToDeleteRequest() {
        DeleteFlywheelRequest deleteFlywheelRequest = Translator.translateToDeleteRequest(FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY);
        assertThat(deleteFlywheelRequest.flywheelArn()).isEqualTo(FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY.getArn());
    }

    @Test
    public void testToListRequest() {
        assertThat(Translator.translateToListRequest(TEST_NEXT_TOKEN).nextToken()).isEqualTo(TEST_NEXT_TOKEN);
        assertThat(Translator.translateToListRequest(null).nextToken()).isNull();
    }

    @Test
    public void testFromListResponse() {
        List<ResourceModel> flywheelModelList = Translator.translateFromListResponse(ListFlywheelsResponse.builder()
                .flywheelSummaryList(TEST_FLYWHEEL_SUMMARY_LIST)
                .build());
        assertThat(flywheelModelList.size()).isEqualTo(2);
        assertThat(flywheelModelList.get(0).getArn()).isEqualTo(TEST_FLYWHEEL_SUMMARY_LIST.get(0).flywheelArn());
        assertThat(flywheelModelList.get(0).getFlywheelName()).isNull();
        assertThat(flywheelModelList.get(0).getDataAccessRoleArn()).isNull();
        assertThat(flywheelModelList.get(0).getDataLakeS3Uri()).isNull();
        assertThat(flywheelModelList.get(0).getModelType()).isNull();
        assertThat(flywheelModelList.get(0).getTaskConfig()).isNull();
        assertThat(flywheelModelList.get(1).getArn()).isEqualTo(TEST_FLYWHEEL_SUMMARY_LIST.get(1).flywheelArn());
    }

    @Test
    public void testToTagResourceRequest() {
        Set<Tag> EXPECTED_TAGS_TO_ADD = new HashSet<>(Arrays.asList(
                Tag.builder().key("key1").value("newValue1").build(),
                Tag.builder().key("key4").value("value4").build(),
                Tag.builder().key("key5").value("value5").build()));

        TagResourceRequest tagResourceRequestAllEmpty = Translator.translateToTagResourceRequest(
                FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS_EMPTY, SDK_TAGS_EMPTY);
        assertThat(new HashSet<>(tagResourceRequestAllEmpty.tags())).isEmpty();

        TagResourceRequest tagResourceRequestNoChange = Translator.translateToTagResourceRequest(
                FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS, SDK_TAGS);
        assertThat(new HashSet<>(tagResourceRequestNoChange.tags())).isEmpty();

        TagResourceRequest tagResourceRequestAddOnly = Translator.translateToTagResourceRequest(
                FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS_EMPTY, SDK_TAGS);
        assertThat(new HashSet<>(tagResourceRequestAddOnly.tags())).isEqualTo(SDK_TAGS);

        TagResourceRequest tagResourceRequestRemoveOnly = Translator.translateToTagResourceRequest(
                FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS, SDK_TAGS_EMPTY);
        assertThat(new HashSet<>(tagResourceRequestRemoveOnly.tags())).isEmpty();

        TagResourceRequest tagResourceRequestAddAndRemove = Translator.translateToTagResourceRequest(
                FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS, SDK_TAGS_UPDATED);
        assertThat(tagResourceRequestAddAndRemove.resourceArn()).isEqualTo(FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY.getArn());
        assertThat(new HashSet<>(tagResourceRequestAddAndRemove.tags())).isEqualTo(EXPECTED_TAGS_TO_ADD);
    }

    @Test
    public void testToUntagResourceRequest() {
        Set<String> EXPECTED_TAG_KEYS_TO_REMOVE = new HashSet<>(Arrays.asList("key3"));

        UntagResourceRequest untagResourceRequestAllEmpty = Translator.translateToUntagResourceRequest(
                FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS_EMPTY, SDK_TAGS_EMPTY);
        assertThat(new HashSet<>(untagResourceRequestAllEmpty.tagKeys())).isEmpty();

        UntagResourceRequest untagResourceRequestNoChange = Translator.translateToUntagResourceRequest(
                FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS, SDK_TAGS);
        assertThat(new HashSet<>(untagResourceRequestNoChange.tagKeys())).isEmpty();

        UntagResourceRequest untagResourceRequestAddOnly = Translator.translateToUntagResourceRequest(
                FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS_EMPTY, SDK_TAGS);
        assertThat(new HashSet<>(untagResourceRequestAddOnly.tagKeys())).isEmpty();

        UntagResourceRequest untagResourceRequestRemoveOnly = Translator.translateToUntagResourceRequest(
                FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS, SDK_TAGS_EMPTY);
        assertThat(new HashSet<>(untagResourceRequestRemoveOnly.tagKeys())).isEqualTo(SDK_TAGS.stream()
                .map(Tag::key).collect(Collectors.toSet()));

        UntagResourceRequest untagResourceRequestAddAndRemove = Translator.translateToUntagResourceRequest(
                FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS, SDK_TAGS_UPDATED);
        assertThat(untagResourceRequestAddAndRemove.resourceArn()).isEqualTo(FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY.getArn());
        assertThat(new HashSet<>(untagResourceRequestAddAndRemove.tagKeys())).isEqualTo(EXPECTED_TAG_KEYS_TO_REMOVE);
    }

    @Test
    public void testToListTagsRequest() {
        ListTagsForResourceRequest listTagsForResourceRequest = Translator.translateToListTagsRequest(FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY);
        assertThat(listTagsForResourceRequest.resourceArn()).isEqualTo(FLYWHEEL_CFN_MODEL_WITH_ARN_ONLY.getArn());
    }

}
