package software.amazon.comprehend.flywheel;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.comprehend.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class TagHelperTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<ComprehendClient> proxyClient;

    @Mock
    private ComprehendClient comprehendClient;


    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(LOGGER, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = MOCK_PROXY(proxy, comprehendClient);
    }

    @Test
    public void testGetCurrentTags() {
        when(comprehendClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(USER_TAGS_WITH_SYSTEM_TAGS)
                        .build());

        Set<Tag> currentTags = TagHelper.getCurrentTags(proxyClient, TEST_RESOURCE_MODEL);
        assertThat(currentTags).isEqualTo(new HashSet<>(USER_TAGS_WITH_SYSTEM_TAGS));
    }

    @Test
    public void testGetDesiredTags_IncludeAllSystemTagsStackTagsResourceTags() {
        ResourceHandlerRequest<ResourceModel> request =
                buildResourceHandlerRequest(TEST_RESOURCE_MODEL, null, SYSTEM_TAGS_MAP);
        Set<Tag> expectedDesiredTags = new HashSet<>();
        expectedDesiredTags.addAll(TagHelper.convertKeyValueMapToSdkTagSet(request.getSystemTags()));
        expectedDesiredTags.addAll(TagHelper.convertKeyValueMapToSdkTagSet(request.getDesiredResourceTags()));
        expectedDesiredTags.addAll(Translator.toSdkTags(request.getDesiredResourceState().getTags()));

        Set<Tag> desiredTags = TagHelper.getDesiredTags(request);
        assertThat(desiredTags).isEqualTo(expectedDesiredTags);
    }

}
