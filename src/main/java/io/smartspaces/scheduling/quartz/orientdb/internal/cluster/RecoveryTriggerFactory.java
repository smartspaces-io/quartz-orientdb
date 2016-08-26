package io.smartspaces.scheduling.quartz.orientdb.internal.cluster;

import io.smartspaces.scheduling.quartz.orientdb.internal.util.Clock;

import org.quartz.JobDataMap;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SimpleTrigger;
import org.quartz.TriggerKey;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;

import java.util.Date;

public class RecoveryTriggerFactory {

    private final String instanceId;
    private final Clock clock;

    public RecoveryTriggerFactory(String instanceId, Clock clock) {
        this.instanceId = instanceId;
        this.clock = clock;
    }

    public OperableTrigger from(OperableTrigger trigger) {
      long currentTimestamp = clock.millis();
      
        TriggerKey tKey = trigger.getKey();
        JobKey jKey = trigger.getJobKey();
        //TODO was ftRec.getScheduleTimestamp();
        long scheduleTimestamp = currentTimestamp;
        //TODO was ftRec.getFireTimestamp()
        long fireTimestamp = currentTimestamp;
        SimpleTriggerImpl rcvryTrig = new SimpleTriggerImpl();
        rcvryTrig.setName("recover_" + instanceId + "_" + currentTimestamp); //String.valueOf(recoverIds++)
        rcvryTrig.setGroup(Scheduler.DEFAULT_RECOVERY_GROUP);
        rcvryTrig.setStartTime(new Date(scheduleTimestamp));
        rcvryTrig.setJobName(jKey.getName());
        rcvryTrig.setJobGroup(jKey.getGroup());
        rcvryTrig.setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY);
        //TODO was ftRec.getPriority()
        rcvryTrig.setPriority(trigger.getPriority());

        // Cannot reuse JobDataMap, because the original trigger
        // is may be persisted after applying misfire.
        JobDataMap jd = new JobDataMap(trigger.getJobDataMap());
        jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_NAME, tKey.getName());
        jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_GROUP, tKey.getGroup());
        jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_FIRETIME_IN_MILLISECONDS,
                String.valueOf(fireTimestamp));
        //TODO jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_SCHEDULED_FIRETIME_IN_MILLISECONDS,
        //TODO String.valueOf(scheduleTimestamp));
        rcvryTrig.setJobDataMap(jd);

        rcvryTrig.computeFirstFireTime(null);
        return rcvryTrig;
    }
}
