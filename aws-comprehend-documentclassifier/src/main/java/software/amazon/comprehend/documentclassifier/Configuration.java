package software.amazon.comprehend.documentclassifier;

import java.util.Map;
import java.util.stream.Collectors;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-comprehend-documentclassifier.json");
    }

    /**
     * Providers should implement this method if their resource has a 'Tags' property to define resource-level tags
     *
     * @param resourceModel ResourceModel object that represents the resource model.
     * @return map of tags.
     */
    public Map<String, String> resourceDefinedTags(final ResourceModel resourceModel) {
        if (resourceModel.getTags() == null) {
            return null;
        } else {
            return resourceModel.getTags().stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue));
        }
    }
}
