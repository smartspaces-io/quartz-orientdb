package io.smartspaces.scheduling.quartz.orientdb.impl.trigger.properties;

import java.text.ParseException;
import java.util.TimeZone;

import org.quartz.CronExpression;
import org.quartz.CronTrigger;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.OperableTrigger;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.impl.Constants;
import io.smartspaces.scheduling.quartz.orientdb.impl.trigger.TriggerPropertiesConverter;

public class CronTriggerPropertiesConverter extends TriggerPropertiesConverter {


  @Override
  protected boolean canHandle(OperableTrigger trigger) {
    return ((trigger instanceof CronTriggerImpl)
        && !((CronTriggerImpl) trigger).hasAdditionalProperties());
  }

  @Override
  public ODocument injectExtraPropertiesForInsert(OperableTrigger trigger, ODocument original) {
    CronTrigger t = (CronTrigger) trigger;

    ODocument newDoc = new ODocument();
    original.copyTo(newDoc);

    newDoc.field(Constants.TRIGGER_CRON_EXPRESSION, t.getCronExpression()).field(Constants.TRIGGER_TIMEZONE,
        t.getTimeZone().getID());

    return newDoc;
  }

  @Override
  public void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, ODocument stored) {
    CronTriggerImpl t = (CronTriggerImpl) trigger;

    String expression = stored.field(Constants.TRIGGER_CRON_EXPRESSION);
    if (expression != null) {
      try {
        t.setCronExpression(new CronExpression(expression));
      } catch (ParseException e) {
        // no good handling strategy and
        // checked exceptions route sucks just as much.
      }
    }
    String tz = stored.field(Constants.TRIGGER_TIMEZONE);
    if (tz != null) {
      t.setTimeZone(TimeZone.getTimeZone(tz));
    }
  }
}
