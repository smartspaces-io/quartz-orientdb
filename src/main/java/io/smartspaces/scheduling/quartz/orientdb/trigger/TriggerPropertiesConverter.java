package io.smartspaces.scheduling.quartz.orientdb.trigger;

import org.bson.Document;
import org.quartz.spi.OperableTrigger;

import io.smartspaces.scheduling.quartz.orientdb.trigger.properties.CalendarIntervalTriggerPropertiesConverter;
import io.smartspaces.scheduling.quartz.orientdb.trigger.properties.CronTriggerPropertiesConverter;
import io.smartspaces.scheduling.quartz.orientdb.trigger.properties.DailyTimeIntervalTriggerPropertiesConverter;
import io.smartspaces.scheduling.quartz.orientdb.trigger.properties.SimpleTriggerPropertiesConverter;

import java.util.Arrays;
import java.util.List;

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

    public abstract Document injectExtraPropertiesForInsert(OperableTrigger trigger, Document original);

    public abstract void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Document stored);
}
