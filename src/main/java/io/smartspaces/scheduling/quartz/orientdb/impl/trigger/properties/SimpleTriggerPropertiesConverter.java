package io.smartspaces.scheduling.quartz.orientdb.impl.trigger.properties;

import org.quartz.SimpleTrigger;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.impl.trigger.TriggerPropertiesConverter;

public class SimpleTriggerPropertiesConverter extends TriggerPropertiesConverter {

  private static final String TRIGGER_REPEAT_COUNT = "repeatCount";
  private static final String TRIGGER_REPEAT_INTERVAL = "repeatInterval";
  private static final String TRIGGER_TIMES_TRIGGERED = "timesTriggered";

  @Override
  protected boolean canHandle(OperableTrigger trigger) {
    return ((trigger instanceof SimpleTriggerImpl)
        && !((SimpleTriggerImpl) trigger).hasAdditionalProperties());
  }

  @Override
  public ODocument injectExtraPropertiesForInsert(OperableTrigger trigger, ODocument original) {
    SimpleTrigger t = (SimpleTrigger) trigger;

    ODocument newDoc = new ODocument();
    original.copyTo(newDoc);

    newDoc.field(TRIGGER_REPEAT_COUNT, t.getRepeatCount())
        .field(TRIGGER_REPEAT_INTERVAL, t.getRepeatInterval())
        .field(TRIGGER_TIMES_TRIGGERED, t.getTimesTriggered());

    return newDoc;
  }

  @Override
  public void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, ODocument stored) {
    SimpleTriggerImpl t = (SimpleTriggerImpl) trigger;

    Integer repeatCount = stored.field(TRIGGER_REPEAT_COUNT);
    if (repeatCount != null) {
      t.setRepeatCount(repeatCount);
    }
    Long repeatInterval = stored.field(TRIGGER_REPEAT_INTERVAL);
    if (repeatInterval != null) {
      t.setRepeatInterval(repeatInterval);
    }
    Integer timesTriggered = stored.field(TRIGGER_TIMES_TRIGGERED);
    if (timesTriggered != null) {
      t.setTimesTriggered(timesTriggered);
    }
  }
}
