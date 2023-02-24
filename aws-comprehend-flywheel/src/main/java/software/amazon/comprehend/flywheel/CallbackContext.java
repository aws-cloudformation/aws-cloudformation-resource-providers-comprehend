package software.amazon.comprehend.flywheel;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import software.amazon.awssdk.services.comprehend.model.FlywheelStatus;
import software.amazon.cloudformation.proxy.StdCallbackContext;

import java.util.Set;
import software.amazon.awssdk.services.comprehend.model.Tag;


@Getter
@Setter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext {
    private String flywheelArn;
    private FlywheelStatus flywheelStatus;
    private Set<Tag> tagsToAdd;
    private Set<String> tagKeysToRemove;
}
