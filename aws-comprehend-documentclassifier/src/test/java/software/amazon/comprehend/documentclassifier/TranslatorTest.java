package software.amazon.comprehend.documentclassifier;


import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.comprehend.model.AugmentedManifestsListItem;
import software.amazon.awssdk.services.comprehend.model.CreateDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DeleteDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DeleteResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierRequest;
import software.amazon.awssdk.services.comprehend.model.DescribeDocumentClassifierResponse;
import software.amazon.awssdk.services.comprehend.model.DescribeResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierDataFormat;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierDocumentTypeFormat;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierDocuments;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierInputDataConfig;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierMode;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierOutputDataConfig;
import software.amazon.awssdk.services.comprehend.model.DocumentClassifierProperties;
import software.amazon.awssdk.services.comprehend.model.DocumentReadAction;
import software.amazon.awssdk.services.comprehend.model.DocumentReadFeatureTypes;
import software.amazon.awssdk.services.comprehend.model.DocumentReadMode;
import software.amazon.awssdk.services.comprehend.model.ListDocumentClassifiersResponse;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.PutResourcePolicyRequest;
import software.amazon.awssdk.services.comprehend.model.Split;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.awssdk.services.comprehend.model.TagResourceRequest;
import software.amazon.awssdk.services.comprehend.model.UntagResourceRequest;
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
public class TranslatorTest extends AbstractModelTestBase {

    /* Test SDK objects */

    private static final VpcConfig SDK_VPC_CONFIG = VpcConfig.builder()
            .securityGroupIds("group1", "group2")
            .subnets("subnet1", "subnet2")
            .build();

    private static final DocumentClassifierInputDataConfig SDK_INPUT_DATA_CONFIG_ALL_NULL = DocumentClassifierInputDataConfig.builder()
            .build();

    private static final DocumentClassifierInputDataConfig SDK_INPUT_DATA_CONFIG_CSV = DocumentClassifierInputDataConfig.builder()
            .dataFormat(DocumentClassifierDataFormat.COMPREHEND_CSV)
            .s3Uri(TEST_INPUT_S3_URI)
            .testS3Uri(TEST_TEST_S3_URI)
            .labelDelimiter("|")
            .build();

    private static final DocumentClassifierInputDataConfig SDK_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV = DocumentClassifierInputDataConfig.builder()
            .dataFormat(DocumentClassifierDataFormat.COMPREHEND_CSV)
            .s3Uri(TEST_INPUT_S3_URI)
            .testS3Uri(TEST_TEST_S3_URI)
            .documents(DocumentClassifierDocuments.builder()
                    .s3Uri(TEST_INPUT_DOCUMENT_S3_URI)
                    .testS3Uri(TEST_TEST_DOCUMENT_S3_URI)
                    .build())
            .documentType(DocumentClassifierDocumentTypeFormat.SEMI_STRUCTURED_DOCUMENT)
            .documentReaderConfig(software.amazon.awssdk.services.comprehend.model.DocumentReaderConfig.builder()
                    .documentReadAction(DocumentReadAction.TEXTRACT_DETECT_DOCUMENT_TEXT.toString())
                    .documentReadMode(DocumentReadMode.SERVICE_DEFAULT.toString())
                    .featureTypes(DocumentReadFeatureTypes.FORMS)
                    .build())
            .labelDelimiter("|")
            .build();

    private static final AugmentedManifestsListItem SDK_AUGMENTED_MANIFEST_LIST_ITEM_1 = AugmentedManifestsListItem.builder()
            .s3Uri(TEST_INPUT_S3_URI)
            .split(Split.TRAIN)
            .attributeNames(TEST_AUGMENTED_MANIFEST_ATTRIBUTE_NAME)
            .build();

    private static final AugmentedManifestsListItem SDK_AUGMENTED_MANIFEST_LIST_ITEM_2 = AugmentedManifestsListItem.builder()
            .s3Uri(TEST_TEST_S3_URI)
            .split(Split.TEST)
            .attributeNames(TEST_AUGMENTED_MANIFEST_ATTRIBUTE_NAME)
            .build();

    private static final DocumentClassifierInputDataConfig SDK_INPUT_DATA_CONFIG_AM = DocumentClassifierInputDataConfig.builder()
            .dataFormat(DocumentClassifierDataFormat.AUGMENTED_MANIFEST)
            .augmentedManifests(Arrays.asList(
                    SDK_AUGMENTED_MANIFEST_LIST_ITEM_1
            ))
            .build();

    private static final DocumentClassifierInputDataConfig SDK_INPUT_DATA_CONFIG_AM_MULTIPLE_ITEMS = DocumentClassifierInputDataConfig.builder()
            .dataFormat(DocumentClassifierDataFormat.AUGMENTED_MANIFEST)
            .augmentedManifests(Arrays.asList(
                    SDK_AUGMENTED_MANIFEST_LIST_ITEM_1,
                    SDK_AUGMENTED_MANIFEST_LIST_ITEM_2
            ))
            .build();

    private static final DocumentClassifierOutputDataConfig SDK_OUTPUT_DATA_CONFIG = DocumentClassifierOutputDataConfig.builder()
            .kmsKeyId(TEST_OUTPUT_KMS_KEY_ARN)
            .s3Uri(DESCRIBED_OUTPUT_S3_URI)
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

    protected static final DocumentClassifierProperties DOCUMENT_CLASSIFIER_PROPERTIES_MINIMAL = DocumentClassifierProperties.builder()
            .documentClassifierArn(TEST_DOCUMENT_CLASSIFIER_ARN)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .inputDataConfig(TEST_INPUT_DATA_CONFIG)
            .languageCode(TEST_LANGUAGE_CODE)
            .build();

    /* Test CFN model objects */

    private static final software.amazon.comprehend.documentclassifier.VpcConfig CFN_VPC_CONFIG =
            software.amazon.comprehend.documentclassifier.VpcConfig.builder()
                    .securityGroupIds(new HashSet<>(Arrays.asList("group1", "group2")))
                    .subnets(new HashSet<>(Arrays.asList("subnet1", "subnet2")))
                    .build();

    private static final software.amazon.comprehend.documentclassifier.AugmentedManifestsListItem CFN_AUGMENTED_MANIFEST_LIST_ITEM_MINIMAL =
            software.amazon.comprehend.documentclassifier.AugmentedManifestsListItem.builder()
                    .s3Uri(TEST_INPUT_S3_URI)
                    .attributeNames(new HashSet<>(Arrays.asList(TEST_AUGMENTED_MANIFEST_ATTRIBUTE_NAME)))
                    .build();

    private static final software.amazon.comprehend.documentclassifier.AugmentedManifestsListItem CFN_AUGMENTED_MANIFEST_LIST_ITEM_1 =
            software.amazon.comprehend.documentclassifier.AugmentedManifestsListItem.builder()
                    .s3Uri(TEST_INPUT_S3_URI)
                    .split(Split.TRAIN.toString())
                    .attributeNames(new HashSet<>(Arrays.asList(TEST_AUGMENTED_MANIFEST_ATTRIBUTE_NAME)))
                    .build();

    private static final software.amazon.comprehend.documentclassifier.AugmentedManifestsListItem CFN_AUGMENTED_MANIFEST_LIST_ITEM_2 =
            software.amazon.comprehend.documentclassifier.AugmentedManifestsListItem.builder()
                    .s3Uri(TEST_TEST_S3_URI)
                    .split(Split.TEST.toString())
                    .attributeNames(new HashSet<>(Arrays.asList(TEST_AUGMENTED_MANIFEST_ATTRIBUTE_NAME)))
                    .build();

    private static final software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig CFN_INPUT_DATA_CONFIG_ALL_NULL =
            software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig.builder()
                    .build();

    private static final software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig CFN_INPUT_DATA_CONFIG_CSV =
            software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig.builder()
                    .dataFormat(DocumentClassifierDataFormat.COMPREHEND_CSV.toString())
                    .s3Uri(TEST_INPUT_S3_URI)
                    .testS3Uri(TEST_TEST_S3_URI)
                    .labelDelimiter("|")
                    .build();

    private static final software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig CFN_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV =
            software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig.builder()
                    .dataFormat(DocumentClassifierDataFormat.COMPREHEND_CSV.toString())
                    .s3Uri(TEST_INPUT_S3_URI)
                    .testS3Uri(TEST_TEST_S3_URI)
                    .documents(software.amazon.comprehend.documentclassifier.DocumentClassifierDocuments.builder()
                            .s3Uri(TEST_INPUT_DOCUMENT_S3_URI)
                            .testS3Uri(TEST_TEST_DOCUMENT_S3_URI)
                            .build())
                    .documentType(DocumentClassifierDocumentTypeFormat.SEMI_STRUCTURED_DOCUMENT.toString())
                    .documentReaderConfig(DocumentReaderConfig.builder()
                            .documentReadAction(DocumentReadAction.TEXTRACT_DETECT_DOCUMENT_TEXT.toString())
                            .documentReadMode(DocumentReadMode.SERVICE_DEFAULT.toString())
                            .featureTypes(ImmutableSet.of(DocumentReadFeatureTypes.TABLES.toString()))
                            .build())
                    .labelDelimiter("|")
                    .build();

    private static final software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig CFN_INPUT_DATA_CONFIG_CSV_MINIMAL =
            software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig.builder()
                    .s3Uri(TEST_INPUT_S3_URI)
                    .build();

    private static final software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig CFN_INPUT_DATA_CONFIG_AM_MINIMAL =
            software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig.builder()
                    .augmentedManifests(new HashSet<>(Arrays.asList(
                            CFN_AUGMENTED_MANIFEST_LIST_ITEM_MINIMAL
                    )))
                    .build();

    private static final software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig CFN_INPUT_DATA_CONFIG_AM =
            software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig.builder()
                    .dataFormat(DocumentClassifierDataFormat.AUGMENTED_MANIFEST.toString())
                    .augmentedManifests(new HashSet<>(Arrays.asList(
                            CFN_AUGMENTED_MANIFEST_LIST_ITEM_1
                    )))
                    .build();

    private static final software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig CFN_INPUT_DATA_CONFIG_AM_MULTIPLE_ITEMS =
            software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig.builder()
                    .dataFormat(DocumentClassifierDataFormat.AUGMENTED_MANIFEST.toString())
                    .augmentedManifests(new HashSet<>(Arrays.asList(
                            CFN_AUGMENTED_MANIFEST_LIST_ITEM_1,
                            CFN_AUGMENTED_MANIFEST_LIST_ITEM_2
                    )))
                    .build();

    private static final software.amazon.comprehend.documentclassifier.DocumentClassifierOutputDataConfig CFN_OUTPUT_DATA_CONFIG =
            software.amazon.comprehend.documentclassifier.DocumentClassifierOutputDataConfig.builder()
                    .kmsKeyId(TEST_OUTPUT_KMS_KEY_ARN)
                    .s3Uri(ORIGINAL_OUTPUT_S3_URI)
                    .build();

    private final Set<software.amazon.comprehend.documentclassifier.Tag> CFN_TAGS = new HashSet<>(Arrays.asList(
            software.amazon.comprehend.documentclassifier.Tag.builder().key("key1").value("value1").build(),
            software.amazon.comprehend.documentclassifier.Tag.builder().key("key2").value("value2").build(),
            software.amazon.comprehend.documentclassifier.Tag.builder().key("key3").value("value3").build()));

    private static final Set<software.amazon.comprehend.documentclassifier.Tag> CFN_TAGS_EMPTY = new HashSet<>();


    /* Test CFN document classifier resource models */

    private final ResourceModel MINIMAL_DOCUMENT_CLASSIFIER_CFN_MODEL_CSV = ResourceModel.builder()
            .documentClassifierName(TEST_MODEL_NAME)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .inputDataConfig(CFN_INPUT_DATA_CONFIG_CSV_MINIMAL)
            .languageCode(TEST_LANGUAGE_CODE)
            .build();

    private final ResourceModel MINIMAL_DOCUMENT_CLASSIFIER_CFN_MODEL_AM = ResourceModel.builder()
            .documentClassifierName(TEST_MODEL_NAME)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .inputDataConfig(CFN_INPUT_DATA_CONFIG_AM_MINIMAL)
            .languageCode(TEST_LANGUAGE_CODE)
            .build();

    private final ResourceModel FULL_DOCUMENT_CLASSIFIER_CFN_MODEL = ResourceModel.builder()
            .documentClassifierName(TEST_MODEL_NAME)
            .dataAccessRoleArn(TEST_DATA_ACCESS_ROLE_ARN)
            .inputDataConfig(CFN_INPUT_DATA_CONFIG_CSV)
            .languageCode(TEST_LANGUAGE_CODE)
            .outputDataConfig(CFN_OUTPUT_DATA_CONFIG)
            .mode(DocumentClassifierMode.MULTI_CLASS.toString())
            .modelKmsKeyId(TEST_MODEL_KMS_KEY_ARN)
            .modelKmsKeyId(TEST_VOLUME_KMS_KEY_ARN)
            .vpcConfig(CFN_VPC_CONFIG)
            .versionName(TEST_VERSION_NAME)
            .tags(CFN_TAGS)
            .modelPolicy(TEST_MODEL_POLICY)
            .build();

    private final ResourceModel DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY = ResourceModel.builder()
            .arn(TEST_DOCUMENT_CLASSIFIER_ARN)
            .build();


    @Test
    public void testToAndFromSdkVpcConfig() {
        // to sdk
        assertThat(Translator.toSdkVpcConfig(null)).isNull();

        software.amazon.comprehend.documentclassifier.VpcConfig translatedFromSdkVpcConfig =
                Translator.fromSdkVpcConfig(SDK_VPC_CONFIG);
        assertThat(translatedFromSdkVpcConfig.getSubnets().stream().sorted().collect(Collectors.toList()))
                .isEqualTo(SDK_VPC_CONFIG.subnets().stream().sorted().collect(Collectors.toList()));
        assertThat(translatedFromSdkVpcConfig.getSecurityGroupIds().stream().sorted().collect(Collectors.toList()))
                .isEqualTo(SDK_VPC_CONFIG.securityGroupIds().stream().sorted().collect(Collectors.toList()));

        // from sdk
        assertThat(Translator.fromSdkVpcConfig(null)).isNull();
        
        VpcConfig translatedToSdkVpcConfig = Translator.toSdkVpcConfig(CFN_VPC_CONFIG);
        assertThat(translatedToSdkVpcConfig.subnets().stream().sorted().collect(Collectors.toList()))
                .isEqualTo(CFN_VPC_CONFIG.getSubnets().stream().sorted().collect(Collectors.toList()));
        assertThat(translatedToSdkVpcConfig.securityGroupIds().stream().sorted().collect(Collectors.toList()))
                .isEqualTo(CFN_VPC_CONFIG.getSecurityGroupIds().stream().sorted().collect(Collectors.toList()));
    }

    @Test
    public void testToAndFromSdkInputDataConfig() {
        // to sdk
        DocumentClassifierInputDataConfig translatedToSdkInputDataConfigAllNull =
                Translator.toSdkInputDataConfig(CFN_INPUT_DATA_CONFIG_ALL_NULL);
        assertThat(translatedToSdkInputDataConfigAllNull).isNotNull();
        assertThat(translatedToSdkInputDataConfigAllNull.s3Uri()).isNull();
        assertThat(translatedToSdkInputDataConfigAllNull.dataFormatAsString()).isNull();
        assertThat(translatedToSdkInputDataConfigAllNull.testS3Uri()).isNull();
        assertThat(translatedToSdkInputDataConfigAllNull.labelDelimiter()).isNull();
        assertThat(translatedToSdkInputDataConfigAllNull.augmentedManifests()).isEmpty();

        DocumentClassifierInputDataConfig translatedToSdkInputDataConfigCSV =
                Translator.toSdkInputDataConfig(CFN_INPUT_DATA_CONFIG_CSV);
        assertThat(translatedToSdkInputDataConfigCSV.s3Uri()).isEqualTo(CFN_INPUT_DATA_CONFIG_CSV.getS3Uri());
        assertThat(translatedToSdkInputDataConfigCSV.testS3Uri()).isEqualTo(CFN_INPUT_DATA_CONFIG_CSV.getTestS3Uri());
        assertThat(translatedToSdkInputDataConfigCSV.dataFormatAsString()).isEqualTo(CFN_INPUT_DATA_CONFIG_CSV.getDataFormat());
        assertThat(translatedToSdkInputDataConfigCSV.labelDelimiter()).isEqualTo(CFN_INPUT_DATA_CONFIG_CSV.getLabelDelimiter());
        assertThat(translatedToSdkInputDataConfigCSV.augmentedManifests()).isEmpty();

        DocumentClassifierInputDataConfig translatedToSdkInputDataConfigNativeModelCSV =
                Translator.toSdkInputDataConfig(CFN_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV);
        assertThat(translatedToSdkInputDataConfigNativeModelCSV.s3Uri()).isEqualTo(CFN_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.getS3Uri());
        assertThat(translatedToSdkInputDataConfigNativeModelCSV.testS3Uri()).isEqualTo(CFN_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.getTestS3Uri());
        assertThat(translatedToSdkInputDataConfigNativeModelCSV.documentTypeAsString()).isEqualTo(CFN_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.getDocumentType());
        assertThat(translatedToSdkInputDataConfigNativeModelCSV.documents().s3Uri()).isEqualTo(CFN_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.getDocuments().getS3Uri());
        assertThat(translatedToSdkInputDataConfigNativeModelCSV.documents().testS3Uri()).isEqualTo(CFN_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.getDocuments().getTestS3Uri());
        assertThat(translatedToSdkInputDataConfigNativeModelCSV.documentReaderConfig().documentReadActionAsString())
                .isEqualTo(CFN_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.getDocumentReaderConfig().getDocumentReadAction());
        assertThat(translatedToSdkInputDataConfigNativeModelCSV.documentReaderConfig().documentReadModeAsString())
                .isEqualTo(CFN_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.getDocumentReaderConfig().getDocumentReadMode());
        assertThat(translatedToSdkInputDataConfigNativeModelCSV.documentReaderConfig().featureTypesAsStrings())
                .isEqualTo(new ArrayList<>(CFN_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.getDocumentReaderConfig().getFeatureTypes()));
        assertThat(translatedToSdkInputDataConfigNativeModelCSV.dataFormatAsString()).isEqualTo(CFN_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.getDataFormat());
        assertThat(translatedToSdkInputDataConfigNativeModelCSV.labelDelimiter()).isEqualTo(CFN_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.getLabelDelimiter());
        assertThat(translatedToSdkInputDataConfigNativeModelCSV.augmentedManifests()).isEmpty();

        DocumentClassifierInputDataConfig translatedToSdkInputDataConfigAM =
                Translator.toSdkInputDataConfig(CFN_INPUT_DATA_CONFIG_AM);
        assertThat(translatedToSdkInputDataConfigAM.s3Uri()).isNull();
        assertThat(translatedToSdkInputDataConfigAM.testS3Uri()).isNull();
        assertThat(translatedToSdkInputDataConfigAM.dataFormatAsString()).isEqualTo(CFN_INPUT_DATA_CONFIG_AM.getDataFormat());
        assertThat(translatedToSdkInputDataConfigAM.labelDelimiter()).isNull();
        assertThat(translatedToSdkInputDataConfigAM.augmentedManifests().size()).isEqualTo(1);
        AugmentedManifestsListItem sdkAugmentedManifestsListItem =
                new ArrayList<>(translatedToSdkInputDataConfigAM.augmentedManifests()).get(0);
        assertThat(sdkAugmentedManifestsListItem.s3Uri()).isEqualTo(CFN_AUGMENTED_MANIFEST_LIST_ITEM_1.getS3Uri());
        assertThat(sdkAugmentedManifestsListItem.split().toString()).isEqualTo(CFN_AUGMENTED_MANIFEST_LIST_ITEM_1.getSplit());
        assertThat(new ArrayList<>(sdkAugmentedManifestsListItem.attributeNames()).get(0)).isEqualTo(
                new ArrayList<>(CFN_AUGMENTED_MANIFEST_LIST_ITEM_1.getAttributeNames()).get(0));

        DocumentClassifierInputDataConfig translatedToSdkInputDataConfigAMMultiple =
                Translator.toSdkInputDataConfig(CFN_INPUT_DATA_CONFIG_AM_MULTIPLE_ITEMS);
        assertThat(translatedToSdkInputDataConfigAMMultiple.augmentedManifests().size()).isEqualTo(2);

        // from sdk
        software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig translatedFromSdkInputDataConfigAllNull =
                Translator.fromSdkInputDataConfig(SDK_INPUT_DATA_CONFIG_ALL_NULL);
        assertThat(translatedFromSdkInputDataConfigAllNull).isNotNull();
        assertThat(translatedFromSdkInputDataConfigAllNull.getS3Uri()).isNull();
        assertThat(translatedFromSdkInputDataConfigAllNull.getDataFormat()).isNull();
        assertThat(translatedFromSdkInputDataConfigAllNull.getTestS3Uri()).isNull();
        assertThat(translatedFromSdkInputDataConfigAllNull.getLabelDelimiter()).isNull();
        assertThat(translatedFromSdkInputDataConfigAllNull.getAugmentedManifests()).isEmpty();

        software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig translatedFromSdkInputDataConfigCSV =
                Translator.fromSdkInputDataConfig(SDK_INPUT_DATA_CONFIG_CSV);
        assertThat(translatedFromSdkInputDataConfigCSV.getS3Uri()).isEqualTo(SDK_INPUT_DATA_CONFIG_CSV.s3Uri());
        assertThat(translatedFromSdkInputDataConfigCSV.getTestS3Uri()).isEqualTo(SDK_INPUT_DATA_CONFIG_CSV.testS3Uri());
        assertThat(translatedFromSdkInputDataConfigCSV.getDataFormat()).isEqualTo(SDK_INPUT_DATA_CONFIG_CSV.dataFormatAsString());
        assertThat(translatedFromSdkInputDataConfigCSV.getLabelDelimiter()).isEqualTo(SDK_INPUT_DATA_CONFIG_CSV.labelDelimiter());
        assertThat(translatedFromSdkInputDataConfigCSV.getAugmentedManifests()).isEmpty();

        software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig translatedFromSdkInputDataConfigNativeModelCSV =
                Translator.fromSdkInputDataConfig(SDK_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV);
        assertThat(translatedFromSdkInputDataConfigNativeModelCSV.getS3Uri()).isEqualTo(SDK_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.s3Uri());
        assertThat(translatedFromSdkInputDataConfigNativeModelCSV.getTestS3Uri()).isEqualTo(SDK_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.testS3Uri());
        assertThat(translatedFromSdkInputDataConfigNativeModelCSV.getDocuments().getS3Uri())
                .isEqualTo(SDK_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.documents().s3Uri());
        assertThat(translatedFromSdkInputDataConfigNativeModelCSV.getDocuments().getTestS3Uri())
                .isEqualTo(SDK_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.documents().testS3Uri());
        assertThat(translatedFromSdkInputDataConfigNativeModelCSV.getDocumentType()).isEqualTo(SDK_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.documentTypeAsString());
        assertThat(translatedFromSdkInputDataConfigNativeModelCSV.getDocumentReaderConfig().getDocumentReadAction())
                .isEqualTo(SDK_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.documentReaderConfig().documentReadActionAsString());
        assertThat(translatedFromSdkInputDataConfigNativeModelCSV.getDocumentReaderConfig().getDocumentReadMode())
                .isEqualTo(SDK_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.documentReaderConfig().documentReadModeAsString());
        assertThat(translatedFromSdkInputDataConfigNativeModelCSV.getDocumentReaderConfig().getFeatureTypes())
                .isEqualTo(new HashSet<>(SDK_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.documentReaderConfig().featureTypesAsStrings()));
        assertThat(translatedFromSdkInputDataConfigNativeModelCSV.getDataFormat()).isEqualTo(SDK_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.dataFormatAsString());
        assertThat(translatedFromSdkInputDataConfigNativeModelCSV.getLabelDelimiter()).isEqualTo(SDK_INPUT_DATA_CONFIG_NATIVE_MODEL_CSV.labelDelimiter());
        assertThat(translatedFromSdkInputDataConfigNativeModelCSV.getAugmentedManifests()).isEmpty();

        software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig translatedFromSdkInputDataConfigAM =
                Translator.fromSdkInputDataConfig(SDK_INPUT_DATA_CONFIG_AM);
        assertThat(translatedFromSdkInputDataConfigAM.getS3Uri()).isNull();
        assertThat(translatedFromSdkInputDataConfigAM.getTestS3Uri()).isNull();
        assertThat(translatedFromSdkInputDataConfigAM.getDataFormat()).isEqualTo(SDK_INPUT_DATA_CONFIG_AM.dataFormatAsString());
        assertThat(translatedFromSdkInputDataConfigAM.getLabelDelimiter()).isNull();
        assertThat(translatedFromSdkInputDataConfigAM.getAugmentedManifests().size()).isEqualTo(1);
        software.amazon.comprehend.documentclassifier.AugmentedManifestsListItem cfnAugmentedManifestsListItem =
                new ArrayList<>(translatedFromSdkInputDataConfigAM.getAugmentedManifests()).get(0);
        assertThat(cfnAugmentedManifestsListItem.getS3Uri()).isEqualTo(SDK_AUGMENTED_MANIFEST_LIST_ITEM_1.s3Uri());
        assertThat(cfnAugmentedManifestsListItem.getSplit()).isEqualTo(SDK_AUGMENTED_MANIFEST_LIST_ITEM_1.split().toString());
        assertThat(new ArrayList<>(cfnAugmentedManifestsListItem.getAttributeNames()).get(0)).isEqualTo(
                new ArrayList<>(SDK_AUGMENTED_MANIFEST_LIST_ITEM_1.attributeNames()).get(0));

        software.amazon.comprehend.documentclassifier.DocumentClassifierInputDataConfig translatedFromSdkInputDataConfigAMMultiple =
                Translator.fromSdkInputDataConfig(SDK_INPUT_DATA_CONFIG_AM_MULTIPLE_ITEMS);
        assertThat(translatedFromSdkInputDataConfigAMMultiple.getAugmentedManifests().size()).isEqualTo(2);
    }

    @Test
    public void testToAndFromSdkOutputDataConfig() {
        // to sdk
        DocumentClassifierOutputDataConfig translatedToSdkOutputDataConfigAllNull = Translator.toSdkOutputDataConfig(
                software.amazon.comprehend.documentclassifier.DocumentClassifierOutputDataConfig.builder()
                        .build());
        assertThat(translatedToSdkOutputDataConfigAllNull.kmsKeyId()).isNull();
        assertThat(translatedToSdkOutputDataConfigAllNull.s3Uri()).isNull();

        DocumentClassifierOutputDataConfig translatedToSdkOutputDataConfig = Translator.toSdkOutputDataConfig(CFN_OUTPUT_DATA_CONFIG);
        assertThat(translatedToSdkOutputDataConfig.kmsKeyId()).isEqualTo(TEST_OUTPUT_KMS_KEY_ARN);
        assertThat(translatedToSdkOutputDataConfig.s3Uri()).isEqualTo(ORIGINAL_OUTPUT_S3_URI);

        // from sdk
        software.amazon.comprehend.documentclassifier.DocumentClassifierOutputDataConfig translatedFromSdkOutputDataConfigAllNull =
                Translator.fromSdkOutputDataConfig(DocumentClassifierOutputDataConfig.builder()
                        .build());
        assertThat(translatedFromSdkOutputDataConfigAllNull.getKmsKeyId()).isNull();
        assertThat(translatedFromSdkOutputDataConfigAllNull.getS3Uri()).isNull();

        software.amazon.comprehend.documentclassifier.DocumentClassifierOutputDataConfig translatedFromSdkOutputDataConfig =
                Translator.fromSdkOutputDataConfig(SDK_OUTPUT_DATA_CONFIG);
        assertThat(translatedFromSdkOutputDataConfig.getKmsKeyId()).isEqualTo(TEST_OUTPUT_KMS_KEY_ARN);
        assertThat(translatedFromSdkOutputDataConfig.getS3Uri()).isEqualTo(ORIGINAL_OUTPUT_S3_URI);
    }

    @Test
    public void testToAndFromSdkTags() {
        Collection<Tag> translatedToSdkTags = Translator.toSdkTags(CFN_TAGS);
        assertThat(new HashSet<>(translatedToSdkTags)).isEqualTo(new HashSet<>(SDK_TAGS));

        Set<software.amazon.comprehend.documentclassifier.Tag> translatedFromSdkTags = Translator.fromSdkTags(SDK_TAGS);
        assertThat(translatedFromSdkTags).isEqualTo(CFN_TAGS);
    }

    @Test
    public void testToCreateRequest() {
        CreateDocumentClassifierRequest minimalCSVDocumentClassifierRequest = Translator.translateToCreateRequest(MINIMAL_DOCUMENT_CLASSIFIER_CFN_MODEL_CSV, TEST_CLIENT_REQUEST_TOKEN, SDK_TAGS_EMPTY);
        assertThat(minimalCSVDocumentClassifierRequest.documentClassifierName()).isEqualTo(MINIMAL_DOCUMENT_CLASSIFIER_CFN_MODEL_CSV.getDocumentClassifierName());
        assertThat(minimalCSVDocumentClassifierRequest.dataAccessRoleArn()).isEqualTo(MINIMAL_DOCUMENT_CLASSIFIER_CFN_MODEL_CSV.getDataAccessRoleArn());
        assertThat(minimalCSVDocumentClassifierRequest.inputDataConfig()).isEqualTo(Translator.toSdkInputDataConfig(MINIMAL_DOCUMENT_CLASSIFIER_CFN_MODEL_CSV.getInputDataConfig()));
        assertThat(minimalCSVDocumentClassifierRequest.languageCodeAsString()).isEqualTo(MINIMAL_DOCUMENT_CLASSIFIER_CFN_MODEL_CSV.getLanguageCode());
        assertThat(minimalCSVDocumentClassifierRequest.clientRequestToken()).isEqualTo(TEST_CLIENT_REQUEST_TOKEN);
        assertThat(minimalCSVDocumentClassifierRequest.tags()).isEqualTo(Translator.toSdkTags(CFN_TAGS_EMPTY));

        CreateDocumentClassifierRequest minimalAMDocumentClassifierRequest = Translator.translateToCreateRequest(MINIMAL_DOCUMENT_CLASSIFIER_CFN_MODEL_AM, TEST_CLIENT_REQUEST_TOKEN, SDK_TAGS_EMPTY);
        assertThat(minimalAMDocumentClassifierRequest.documentClassifierName()).isEqualTo(MINIMAL_DOCUMENT_CLASSIFIER_CFN_MODEL_AM.getDocumentClassifierName());
        assertThat(minimalAMDocumentClassifierRequest.dataAccessRoleArn()).isEqualTo(MINIMAL_DOCUMENT_CLASSIFIER_CFN_MODEL_AM.getDataAccessRoleArn());
        assertThat(minimalAMDocumentClassifierRequest.inputDataConfig()).isEqualTo(Translator.toSdkInputDataConfig(MINIMAL_DOCUMENT_CLASSIFIER_CFN_MODEL_AM.getInputDataConfig()));
        assertThat(minimalAMDocumentClassifierRequest.languageCodeAsString()).isEqualTo(MINIMAL_DOCUMENT_CLASSIFIER_CFN_MODEL_AM.getLanguageCode());
        assertThat(minimalAMDocumentClassifierRequest.clientRequestToken()).isEqualTo(TEST_CLIENT_REQUEST_TOKEN);
        assertThat(minimalAMDocumentClassifierRequest.tags()).isEqualTo(Translator.toSdkTags(CFN_TAGS_EMPTY));

        CreateDocumentClassifierRequest fullDocumentClassifierRequest = Translator.translateToCreateRequest(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL, TEST_CLIENT_REQUEST_TOKEN, SDK_TAGS);
        assertThat(fullDocumentClassifierRequest.documentClassifierName()).isEqualTo(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getDocumentClassifierName());
        assertThat(fullDocumentClassifierRequest.dataAccessRoleArn()).isEqualTo(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getDataAccessRoleArn());
        assertThat(fullDocumentClassifierRequest.inputDataConfig()).isEqualTo(Translator.toSdkInputDataConfig(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getInputDataConfig()));
        assertThat(fullDocumentClassifierRequest.languageCodeAsString()).isEqualTo(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getLanguageCode());
        assertThat(fullDocumentClassifierRequest.outputDataConfig()).isEqualTo(Translator.toSdkOutputDataConfig(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getOutputDataConfig()));
        assertThat(fullDocumentClassifierRequest.mode().toString()).isEqualTo(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getMode());
        assertThat(fullDocumentClassifierRequest.modelKmsKeyId()).isEqualTo(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getModelKmsKeyId());
        assertThat(fullDocumentClassifierRequest.volumeKmsKeyId()).isEqualTo(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getVolumeKmsKeyId());
        assertThat(fullDocumentClassifierRequest.vpcConfig()).isEqualTo(Translator.toSdkVpcConfig(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getVpcConfig()));
        assertThat(fullDocumentClassifierRequest.versionName()).isEqualTo(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getVersionName());
        assertThat(fullDocumentClassifierRequest.modelPolicy()).isEqualTo(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getModelPolicy());
        assertThat(fullDocumentClassifierRequest.clientRequestToken()).isEqualTo(TEST_CLIENT_REQUEST_TOKEN);
        assertThat(new HashSet<>(fullDocumentClassifierRequest.tags())).isEqualTo(new HashSet<>(Translator.toSdkTags(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getTags())));
    }

    @Test
    public void testToReadDocumentClassifierRequest() {
        DescribeDocumentClassifierRequest describeDocumentClassifierRequest = software.amazon.comprehend.documentclassifier.Translator.translateToReadRequest(DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY);
        assertThat(describeDocumentClassifierRequest.documentClassifierArn()).isEqualTo(DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY.getArn());
    }

    @Test
    public void testFromReadDocumentClassifierResponse() {
        ResourceModel documentClassifierModelMinimalProperties = Translator.translateFromReadResponse(DescribeDocumentClassifierResponse.builder()
                .documentClassifierProperties(DOCUMENT_CLASSIFIER_PROPERTIES_MINIMAL)
                .build());
        assertThat(documentClassifierModelMinimalProperties.getArn()).isEqualTo(DOCUMENT_CLASSIFIER_PROPERTIES_MINIMAL.documentClassifierArn());
        assertThat(documentClassifierModelMinimalProperties.getVersionName()).isNull();
        assertThat(documentClassifierModelMinimalProperties.getDocumentClassifierName()).isEqualTo(TEST_MODEL_NAME);
        assertThat(documentClassifierModelMinimalProperties.getDataAccessRoleArn()).isEqualTo(DOCUMENT_CLASSIFIER_PROPERTIES_MINIMAL.dataAccessRoleArn());
        assertThat(documentClassifierModelMinimalProperties.getInputDataConfig()).isEqualTo(Translator.fromSdkInputDataConfig(DOCUMENT_CLASSIFIER_PROPERTIES_MINIMAL.inputDataConfig()));
        assertThat(documentClassifierModelMinimalProperties.getLanguageCode()).isEqualTo(DOCUMENT_CLASSIFIER_PROPERTIES_MINIMAL.languageCode().toString());

        ResourceModel documentClassifierModelFullProperties = Translator.translateFromReadResponse(DescribeDocumentClassifierResponse.builder()
                .documentClassifierProperties(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING)
                .build());
        assertThat(documentClassifierModelFullProperties.getMode()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING.mode().toString());
        assertThat(documentClassifierModelFullProperties.getOutputDataConfig()).isEqualTo(Translator.fromSdkOutputDataConfig(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING.outputDataConfig()));
        assertThat(documentClassifierModelFullProperties.getVersionName()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING.versionName());
        assertThat(documentClassifierModelFullProperties.getVolumeKmsKeyId()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING.volumeKmsKeyId());
        assertThat(documentClassifierModelFullProperties.getModelKmsKeyId()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING.modelKmsKeyId());
        assertThat(documentClassifierModelFullProperties.getVpcConfig()).isEqualTo(Translator.fromSdkVpcConfig(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_TRAINING.vpcConfig()));
    }

    @Test
    public void testToDeleteRequest() {
        DeleteDocumentClassifierRequest deleteDocumentClassifierRequest = Translator.translateToDeleteRequest(DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY);
        assertThat(deleteDocumentClassifierRequest.documentClassifierArn()).isEqualTo(DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY.getArn());
    }

    @Test
    public void testToListRequest() {
        assertThat(Translator.translateToListRequest(TEST_NEXT_TOKEN).nextToken()).isEqualTo(TEST_NEXT_TOKEN);
        assertThat(Translator.translateToListRequest(null).nextToken()).isNull();
    }

    @Test
    public void testFromListResponse() {
        List<ResourceModel> documentClassifierModelList = Translator.translateFromListResponse(ListDocumentClassifiersResponse.builder()
                .documentClassifierPropertiesList(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_LIST)
                .build());
        assertThat(documentClassifierModelList.size()).isEqualTo(5);
        assertThat(documentClassifierModelList.get(0).getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_LIST.get(0).documentClassifierArn());
        assertThat(documentClassifierModelList.get(0).getDocumentClassifierName()).isNull();
        assertThat(documentClassifierModelList.get(0).getDataAccessRoleArn()).isNull();
        assertThat(documentClassifierModelList.get(0).getModelPolicy()).isNull();
        assertThat(documentClassifierModelList.get(0).getMode()).isNull();
        assertThat(documentClassifierModelList.get(0).getVersionName()).isNull();
        assertThat(documentClassifierModelList.get(0).getInputDataConfig()).isNull();
        assertThat(documentClassifierModelList.get(0).getOutputDataConfig()).isNull();
        assertThat(documentClassifierModelList.get(0).getLanguageCode()).isNull();
        assertThat(documentClassifierModelList.get(0).getModelKmsKeyId()).isNull();
        assertThat(documentClassifierModelList.get(0).getVolumeKmsKeyId()).isNull();
        assertThat(documentClassifierModelList.get(0).getTags()).isNull();
        assertThat(documentClassifierModelList.get(0).getVpcConfig()).isNull();
        assertThat(documentClassifierModelList.get(1).getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_LIST.get(1).documentClassifierArn());
        assertThat(documentClassifierModelList.get(2).getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_LIST.get(2).documentClassifierArn());
        assertThat(documentClassifierModelList.get(3).getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_LIST.get(3).documentClassifierArn());
        assertThat(documentClassifierModelList.get(4).getArn()).isEqualTo(TEST_DOCUMENT_CLASSIFIER_PROPERTIES_LIST.get(4).documentClassifierArn());
    }

    @Test
    public void testToTagResourceRequest() {
        Set<Tag> EXPECTED_TAGS_TO_ADD = new HashSet<>(Arrays.asList(
                Tag.builder().key("key1").value("newValue1").build(),
                Tag.builder().key("key4").value("value4").build(),
                Tag.builder().key("key5").value("value5").build()));

        TagResourceRequest tagResourceRequestAllEmpty = Translator.translateToTagResourceRequest(
                DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS_EMPTY, SDK_TAGS_EMPTY);
        assertThat(new HashSet<>(tagResourceRequestAllEmpty.tags())).isEmpty();

        TagResourceRequest tagResourceRequestNoChange = Translator.translateToTagResourceRequest(
                DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS, SDK_TAGS);
        assertThat(new HashSet<>(tagResourceRequestNoChange.tags())).isEmpty();

        TagResourceRequest tagResourceRequestAddOnly = Translator.translateToTagResourceRequest(
                DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS_EMPTY, SDK_TAGS);
        assertThat(new HashSet<>(tagResourceRequestAddOnly.tags())).isEqualTo(SDK_TAGS);

        TagResourceRequest tagResourceRequestRemoveOnly = Translator.translateToTagResourceRequest(
                DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS, SDK_TAGS_EMPTY);
        assertThat(new HashSet<>(tagResourceRequestRemoveOnly.tags())).isEmpty();

        TagResourceRequest tagResourceRequestAddAndRemove = Translator.translateToTagResourceRequest(
                DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS, SDK_TAGS_UPDATED);
        assertThat(tagResourceRequestAddAndRemove.resourceArn()).isEqualTo(DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY.getArn());
        assertThat(new HashSet<>(tagResourceRequestAddAndRemove.tags())).isEqualTo(EXPECTED_TAGS_TO_ADD);
    }

    @Test
    public void testToUntagResourceRequest() {
        Set<String> EXPECTED_TAG_KEYS_TO_REMOVE = new HashSet<>(Arrays.asList("key3"));

        UntagResourceRequest untagResourceRequestAllEmpty = Translator.translateToUntagResourceRequest(
                DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS_EMPTY, SDK_TAGS_EMPTY);
        assertThat(new HashSet<>(untagResourceRequestAllEmpty.tagKeys())).isEmpty();

        UntagResourceRequest untagResourceRequestNoChange = Translator.translateToUntagResourceRequest(
                DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS, SDK_TAGS);
        assertThat(new HashSet<>(untagResourceRequestNoChange.tagKeys())).isEmpty();

        UntagResourceRequest untagResourceRequestAddOnly = Translator.translateToUntagResourceRequest(
                DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS_EMPTY, SDK_TAGS);
        assertThat(new HashSet<>(untagResourceRequestAddOnly.tagKeys())).isEmpty();

        UntagResourceRequest untagResourceRequestRemoveOnly = Translator.translateToUntagResourceRequest(
                DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS, SDK_TAGS_EMPTY);
        assertThat(new HashSet<>(untagResourceRequestRemoveOnly.tagKeys())).isEqualTo(SDK_TAGS.stream()
                .map(Tag::key).collect(Collectors.toSet()));

        UntagResourceRequest untagResourceRequestAddAndRemove = Translator.translateToUntagResourceRequest(
                DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY, SDK_TAGS, SDK_TAGS_UPDATED);
        assertThat(untagResourceRequestAddAndRemove.resourceArn()).isEqualTo(DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY.getArn());
        assertThat(new HashSet<>(untagResourceRequestAddAndRemove.tagKeys())).isEqualTo(EXPECTED_TAG_KEYS_TO_REMOVE);
    }

    @Test
    public void testToListTagsRequest() {
        ListTagsForResourceRequest listTagsForResourceRequest = Translator.translateToListTagsRequest(DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY);
        assertThat(listTagsForResourceRequest.resourceArn()).isEqualTo(DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY.getArn());
    }

    @Test
    public void testToPutResourcePolicyRequest() {
        PutResourcePolicyRequest putResourcePolicyRequest = Translator.translateToPutResourcePolicyRequest(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL);
        assertThat(putResourcePolicyRequest.resourceArn()).isEqualTo(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getArn());
        assertThat(putResourcePolicyRequest.resourcePolicy()).isEqualTo(FULL_DOCUMENT_CLASSIFIER_CFN_MODEL.getModelPolicy());
    }

    @Test
    public void testToDescribeResourcePolicyRequest() {
        DescribeResourcePolicyRequest describeResourcePolicyRequest = Translator.translateToDescribeResourcePolicyRequest(DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY);
        assertThat(describeResourcePolicyRequest.resourceArn()).isEqualTo(DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY.getArn());
    }

    @Test
    public void testToDeleteResourcePolicyRequest() {
        DeleteResourcePolicyRequest deleteResourcePolicyRequest = Translator.translateToDeleteResourcePolicyRequest(DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY);
        assertThat(deleteResourcePolicyRequest.resourceArn()).isEqualTo(DOCUMENT_CLASSIFIER_CFN_MODEL_WITH_ARN_ONLY.getArn());
    }

}
