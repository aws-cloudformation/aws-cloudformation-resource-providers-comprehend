package software.amazon.comprehend.documentclassifier;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import software.amazon.awssdk.services.comprehend.model.ModelStatus;
import software.amazon.awssdk.services.comprehend.model.Tag;
import software.amazon.cloudformation.proxy.StdCallbackContext;

import java.util.Set;

@Getter
@Setter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext {
    private String arn;
    private ModelStatus modelStatus;
    private Set<Tag> tagsToAdd;
    private Set<String> tagKeysToRemove;
}
