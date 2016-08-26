/*
 * Copyright (C) 2016 Keith M. Hughes
 * Forked from code (c) Michael S. Klishin, Alex Petrov, 2011-2015.
 * Forked from code from MuleSoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.smartspaces.scheduling.quartz.orientdb.internal.trigger.properties;

import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;
import org.quartz.spi.OperableTrigger;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.TriggerPropertiesConverter;

public class CalendarIntervalTriggerPropertiesConverter extends TriggerPropertiesConverter {

  private static final String TRIGGER_REPEAT_INTERVAL_UNIT = "repeatIntervalUnit";
  private static final String TRIGGER_REPEAT_INTERVAL = "repeatInterval";
  private static final String TRIGGER_TIMES_TRIGGERED = "timesTriggered";

  @Override
  protected boolean canHandle(OperableTrigger trigger) {
    return ((trigger instanceof CalendarIntervalTriggerImpl)
        && !((CalendarIntervalTriggerImpl) trigger).hasAdditionalProperties());
  }

  @Override
  public ODocument injectExtraPropertiesForInsert(OperableTrigger trigger, ODocument original) {
    CalendarIntervalTriggerImpl t = (CalendarIntervalTriggerImpl) trigger;

    ODocument newDoc = new ODocument();
    original.copyTo(newDoc);

    newDoc.field(TRIGGER_REPEAT_INTERVAL_UNIT, t.getRepeatIntervalUnit().name())
        .field(TRIGGER_REPEAT_INTERVAL, t.getRepeatInterval())
        .field(TRIGGER_TIMES_TRIGGERED, t.getTimesTriggered());

    return newDoc;
  }

  @Override
  public void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, ODocument stored) {
    CalendarIntervalTriggerImpl t = (CalendarIntervalTriggerImpl) trigger;

    String repeatIntervalUnit = stored.field(TRIGGER_REPEAT_INTERVAL_UNIT);
    if (repeatIntervalUnit != null) {
      t.setRepeatIntervalUnit(IntervalUnit.valueOf(repeatIntervalUnit));
    }
    Integer repeatInterval = stored.field(TRIGGER_REPEAT_INTERVAL);
    if (repeatInterval != null) {
      t.setRepeatInterval(repeatInterval);
    }
    Integer timesTriggered = stored.field(TRIGGER_TIMES_TRIGGERED);
    if (timesTriggered != null) {
      t.setTimesTriggered(timesTriggered);
    }
  }
}
