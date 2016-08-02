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

package io.smartspaces.scheduling.quartz.orientdb.impl.trigger;

import java.util.Arrays;
import java.util.List;

import org.quartz.spi.OperableTrigger;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.impl.trigger.properties.CalendarIntervalTriggerPropertiesConverter;
import io.smartspaces.scheduling.quartz.orientdb.impl.trigger.properties.CronTriggerPropertiesConverter;
import io.smartspaces.scheduling.quartz.orientdb.impl.trigger.properties.DailyTimeIntervalTriggerPropertiesConverter;
import io.smartspaces.scheduling.quartz.orientdb.impl.trigger.properties.SimpleTriggerPropertiesConverter;

/**
 * Converts trigger type specific properties.
 */
public abstract class TriggerPropertiesConverter {

    private static final List<TriggerPropertiesConverter> propertiesConverters = Arrays.asList(
            new SimpleTriggerPropertiesConverter(),
            new CalendarIntervalTriggerPropertiesConverter(),
            new CronTriggerPropertiesConverter(),
            new DailyTimeIntervalTriggerPropertiesConverter());

    /**
     * Returns properties converter for given trigger or null when not found.
     * @param trigger    a trigger instance
     * @return converter or null
     */
    public static TriggerPropertiesConverter getConverterFor(OperableTrigger trigger) {
        for (TriggerPropertiesConverter converter : propertiesConverters) {
            if (converter.canHandle(trigger)) {
                return converter;
            }
        }
        return null;
    }

    protected abstract boolean canHandle(OperableTrigger trigger);

    public abstract ODocument injectExtraPropertiesForInsert(OperableTrigger trigger, ODocument original);

    public abstract void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, ODocument stored);
}
