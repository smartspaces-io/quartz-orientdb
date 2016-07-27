package io.smartspaces.scheduling.quartz.orientdb.trigger.properties;

import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.DateBuilder;
import org.quartz.TimeOfDay;
import org.quartz.impl.triggers.DailyTimeIntervalTriggerImpl;
import org.quartz.spi.OperableTrigger;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.trigger.TriggerPropertiesConverter;

public class DailyTimeIntervalTriggerPropertiesConverter extends TriggerPropertiesConverter {

  private static final String TRIGGER_REPEAT_INTERVAL_UNIT = "repeatIntervalUnit";
  private static final String TRIGGER_REPEAT_INTERVAL = "repeatInterval";
  private static final String TRIGGER_TIMES_TRIGGERED = "timesTriggered";
  private static final String TRIGGER_START_TIME_OF_DAY = "startTimeOfDay";
  private static final String TRIGGER_END_TIME_OF_DAY = "endTimeOfDay";

  @Override
  protected boolean canHandle(OperableTrigger trigger) {
    return ((trigger instanceof DailyTimeIntervalTrigger)
        && !((DailyTimeIntervalTriggerImpl) trigger).hasAdditionalProperties());
  }

  @Override
  public ODocument injectExtraPropertiesForInsert(OperableTrigger trigger, ODocument original) {
    DailyTimeIntervalTriggerImpl t = (DailyTimeIntervalTriggerImpl) trigger;

    ODocument newDoc = new ODocument();
    original.copyTo(newDoc);

    return newDoc.field(TRIGGER_REPEAT_INTERVAL_UNIT, t.getRepeatIntervalUnit().name())
        .field(TRIGGER_REPEAT_INTERVAL, t.getRepeatInterval())
        .field(TRIGGER_TIMES_TRIGGERED, t.getTimesTriggered())
        .field(TRIGGER_START_TIME_OF_DAY, toDocument(t.getStartTimeOfDay()))
        .field(TRIGGER_END_TIME_OF_DAY, toDocument(t.getEndTimeOfDay()));
  }

  private ODocument toDocument(TimeOfDay tod) {
    return new ODocument().field("hour", tod.getHour()).field("minute", tod.getMinute())
        .field("second", tod.getSecond());
  }

  @Override
  public void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, ODocument stored) {
    DailyTimeIntervalTriggerImpl t = (DailyTimeIntervalTriggerImpl) trigger;

    String interval_unit = stored.field(TRIGGER_REPEAT_INTERVAL_UNIT);
    if (interval_unit != null) {
      t.setRepeatIntervalUnit(DateBuilder.IntervalUnit.valueOf(interval_unit));
    }
    Integer repeatInterval = stored.field(TRIGGER_REPEAT_INTERVAL);
    if (repeatInterval != null) {
      t.setRepeatInterval(repeatInterval);
    }
    Integer timesTriggered = stored.field(TRIGGER_TIMES_TRIGGERED);
    if (timesTriggered != null) {
      t.setTimesTriggered(timesTriggered);
    }

    ODocument startTOD = stored.field(TRIGGER_START_TIME_OF_DAY);
    if (startTOD != null) {
      t.setStartTimeOfDay(fromDocument(startTOD));
    }
    ODocument endTOD = stored.field(TRIGGER_END_TIME_OF_DAY);
    if (endTOD != null) {
      t.setEndTimeOfDay(fromDocument(endTOD));
    }
  }

  private TimeOfDay fromDocument(ODocument tod) {
    Integer hour = tod.field("hour");
    Integer minute = tod.field("minute");
    Integer second = tod.field("second");

    return new TimeOfDay(hour, minute, second);
  }
}
