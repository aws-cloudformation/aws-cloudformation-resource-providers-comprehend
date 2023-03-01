package software.amazon.comprehend.flywheel;

import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


import java.util.Collections;
import java.util.HashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.cloudformation.proxy.ProxyClient;


public class TagHelper {
    
    /**
     * Converts a collection of Tag objects to a tag-name -> tag-value map.
     *
     * Note: Tag objects with null tag values will not be included in the output
     * map.
     *
     * @param tags Collection of tags to convert
     * @return Converted Map of tags
     */
    public static Map<String, String> convertSdkTagsToKeyValueMap(final Collection<Tag> tags) {
        if (CollectionUtils.isEmpty(tags)) {
            return Collections.emptyMap();
        }
        return tags.stream()
                .filter(tag -> tag.value() != null)
                .collect(Collectors.toMap(
                        Tag::key,
                        Tag::value,
                        (oldValue, newValue) -> newValue));
    }

    /**
     * Converts a tag map to a set of Tag objects.
     *
     * Note: Like convertSdkTagsToKeyValueMap, convertKeyValueMapToSdkTagSet filters out value-less tag entries.
     *
     * @param tagMap Map of tags to convert
     * @return Set of Tag objects
     */
    public static Set<Tag> convertKeyValueMapToSdkTagSet(final Map<String, String> tagMap) {
        if (MapUtils.isEmpty(tagMap)) {
            return Collections.emptySet();
        }
        return tagMap.entrySet().stream()
                .filter(tag -> tag.getValue() != null)
                .map(tag -> Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build())
                .collect(Collectors.toSet());
    }

    /**
     * Get all tags currently attached to some resource.
     */
    public static Set<Tag> getCurrentTags(final ProxyClient<ComprehendClient> proxyClient,
                                           final ResourceModel flywheelModel) {
        return new HashSet<>(proxyClient.injectCredentialsAndInvokeV2(
                Translator.translateToListTagsRequest(flywheelModel),
                proxyClient.client()::listTagsForResource).tags());
    }

    /**
     * Get desired tags by combining desired system tags, stack level tags, and resource tags.
     *
     * If stack tags and resource tags are not merged together in Configuration class,
     * we will get new desired system (with `aws:cloudformation` prefix) and user defined tags from
     * handlerRequest.getSystemTags() (system tags),
     * handlerRequest.getDesiredResourceTags() (stack tags),
     * handlerRequest.getDesiredResourceState().getTags() (resource tags).
     *
     * System tags are an optional feature. Merge them to your tags if you have enabled them for your resource.
     * System tags can change on resource update if the resource is imported to the stack.
     */
    public static Set<Tag> getDesiredTags(final ResourceHandlerRequest<ResourceModel> handlerRequest) {
        final Map<String, String> desiredTags = new HashMap<>();

        // merge in system tags
        if (handlerRequest.getSystemTags() != null) {
            desiredTags.putAll(handlerRequest.getSystemTags());
        }

        // merge in stack level tags if any
        if (handlerRequest.getDesiredResourceTags() != null) {
            desiredTags.putAll(handlerRequest.getDesiredResourceTags());
        }

        // merge in resource tags if any
        Map<String, String> resourceTags = convertSdkTagsToKeyValueMap(
                Translator.toSdkTags(handlerRequest.getDesiredResourceState().getTags()));
        if (resourceTags != null) {
            desiredTags.putAll(resourceTags);
        }

        return convertKeyValueMapToSdkTagSet(desiredTags);
    }

}
