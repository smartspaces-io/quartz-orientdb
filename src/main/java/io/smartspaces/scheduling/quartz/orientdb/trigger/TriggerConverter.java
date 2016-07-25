package io.smartspaces.scheduling.quartz.orientdb.trigger;

import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.KEY_GROUP;
import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.KEY_NAME;

import java.io.IOException;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.Constants;
import io.smartspaces.scheduling.quartz.orientdb.dao.JobDao;
import io.smartspaces.scheduling.quartz.orientdb.util.SerialUtils;

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

public class TriggerConverter {

    private static final String TRIGGER_CALENDAR_NAME = "calendarName";
    private static final String TRIGGER_CLASS = "class";
    private static final String TRIGGER_DESCRIPTION = "description";
    private static final String TRIGGER_END_TIME = "endTime";
    private static final String TRIGGER_FINAL_FIRE_TIME = "finalFireTime";
    private static final String TRIGGER_FIRE_INSTANCE_ID = "fireInstanceId";
    private static final String TRIGGER_MISFIRE_INSTRUCTION = "misfireInstruction";
    private static final String TRIGGER_PREVIOUS_FIRE_TIME = "previousFireTime";
    private static final String TRIGGER_PRIORITY = "priority";
    private static final String TRIGGER_START_TIME = "startTime";

    private static final Logger log = LoggerFactory.getLogger(TriggerConverter.class);

    private JobDao jobDao;

    public TriggerConverter(JobDao jobDao) {
        this.jobDao = jobDao;
    }

    public ODocument toDocument(OperableTrigger newTrigger, ObjectId jobId)
            throws JobPersistenceException {
        ODocument trigger = convertToBson(newTrigger, jobId);
        if (newTrigger.getJobDataMap().size() > 0) {
            try {
                String jobDataString = SerialUtils.serialize(newTrigger.getJobDataMap());
                trigger.field(Constants.JOB_DATA, jobDataString);
            } catch (IOException ioe) {
                throw new JobPersistenceException("Could not serialise job data map on the trigger for "
                        + newTrigger.getKey(), ioe);
            }
        }

        TriggerPropertiesConverter tpd = TriggerPropertiesConverter.getConverterFor(newTrigger);
        trigger = tpd.injectExtraPropertiesForInsert(newTrigger, trigger);
        return trigger;
    }

    /**
     * Restore trigger from Mongo Document.
     *
     * @param triggerKey
     * @param triggerDoc
     * @return trigger from Document or null when trigger has no associated job
     * @throws JobPersistenceException
     */
    public OperableTrigger toTrigger(TriggerKey triggerKey, Document triggerDoc)
            throws JobPersistenceException {
        OperableTrigger trigger = createNewInstance(triggerDoc);

        TriggerPropertiesConverter tpd = TriggerPropertiesConverter.getConverterFor(trigger);

        loadCommonProperties(triggerKey, triggerDoc, trigger);

        loadJobData(triggerDoc, trigger);

        loadStartAndEndTimes(triggerDoc, trigger);

        tpd.setExtraPropertiesAfterInstantiation(trigger, triggerDoc);

        Document job = jobDao.getById(triggerDoc.get(Constants.TRIGGER_JOB_ID));
        if (job != null) {
            trigger.setJobKey(new JobKey(job.getString(KEY_NAME), job.getString(KEY_GROUP)));
            return trigger;
        } else {
            // job was deleted
            return null;
        }
    }

    public OperableTrigger toTrigger(Document doc) throws JobPersistenceException {
        TriggerKey key = new TriggerKey(doc.getString(KEY_NAME), doc.getString(KEY_GROUP));
        return toTrigger(key, doc);
    }

    private ODocument convertToBson(OperableTrigger newTrigger, ObjectId jobId) {
        ODocument trigger = new Document();
        trigger.field(Constants.TRIGGER_STATE, Constants.STATE_WAITING);
        trigger.field(TRIGGER_CALENDAR_NAME, newTrigger.getCalendarName());
        trigger.field(TRIGGER_CLASS, newTrigger.getClass().getName());
        trigger.field(TRIGGER_DESCRIPTION, newTrigger.getDescription());
        trigger.field(TRIGGER_END_TIME, newTrigger.getEndTime());
        trigger.field(TRIGGER_FINAL_FIRE_TIME, newTrigger.getFinalFireTime());
        trigger.field(TRIGGER_FIRE_INSTANCE_ID, newTrigger.getFireInstanceId());
        trigger.field(Constants.TRIGGER_JOB_ID, jobId);
        trigger.field(KEY_NAME, newTrigger.getKey().getName());
        trigger.field(KEY_GROUP, newTrigger.getKey().getGroup());
        trigger.field(TRIGGER_MISFIRE_INSTRUCTION, newTrigger.getMisfireInstruction());
        trigger.field(Constants.TRIGGER_NEXT_FIRE_TIME, newTrigger.getNextFireTime());
        trigger.field(TRIGGER_PREVIOUS_FIRE_TIME, newTrigger.getPreviousFireTime());
        trigger.field(TRIGGER_PRIORITY, newTrigger.getPriority());
        trigger.field(TRIGGER_START_TIME, newTrigger.getStartTime());
        return trigger;
    }

    private OperableTrigger createNewInstance(Document triggerDoc) throws JobPersistenceException {
        String triggerClassName = triggerDoc.getString(TRIGGER_CLASS);
        try {
            @SuppressWarnings("unchecked")
            Class<OperableTrigger> triggerClass = (Class<OperableTrigger>) getTriggerClassLoader()
                    .loadClass(triggerClassName);
            return triggerClass.newInstance();
        } catch (ClassNotFoundException e) {
            throw new JobPersistenceException("Could not find trigger class " + triggerClassName);
        } catch (Exception e) {
            throw new JobPersistenceException("Could not instantiate trigger class " + triggerClassName);
        }
    }

    private ClassLoader getTriggerClassLoader() {
        return Job.class.getClassLoader();
    }

    private void loadCommonProperties(TriggerKey triggerKey, Document triggerDoc, OperableTrigger trigger) {
        trigger.setKey(triggerKey);
        trigger.setCalendarName(triggerDoc.getString(TRIGGER_CALENDAR_NAME));
        trigger.setDescription(triggerDoc.getString(TRIGGER_DESCRIPTION));
        trigger.setFireInstanceId(triggerDoc.getString(TRIGGER_FIRE_INSTANCE_ID));
        trigger.setMisfireInstruction(triggerDoc.getInteger(TRIGGER_MISFIRE_INSTRUCTION));
        trigger.setNextFireTime(triggerDoc.getDate(Constants.TRIGGER_NEXT_FIRE_TIME));
        trigger.setPreviousFireTime(triggerDoc.getDate(TRIGGER_PREVIOUS_FIRE_TIME));
        trigger.setPriority(triggerDoc.getInteger(TRIGGER_PRIORITY));
    }

    private void loadJobData(Document triggerDoc, OperableTrigger trigger)
            throws JobPersistenceException {
        String jobDataString = triggerDoc.getString(Constants.JOB_DATA);

        if (jobDataString != null) {
            try {
                SerialUtils.deserialize(trigger.getJobDataMap(), jobDataString);
            } catch (IOException e) {
                throw new JobPersistenceException("Could not deserialize job data for trigger "
                        + triggerDoc.get(TRIGGER_CLASS));
            }
        }
    }

    private void loadStartAndEndTimes(Document triggerDoc, OperableTrigger trigger) {
        loadStartAndEndTime(triggerDoc, trigger);
        loadStartAndEndTime(triggerDoc, trigger);
    }

    private void loadStartAndEndTime(Document triggerDoc, OperableTrigger trigger) {
        try {
            trigger.setStartTime(triggerDoc.getDate(TRIGGER_START_TIME));
            trigger.setEndTime(triggerDoc.getDate(TRIGGER_END_TIME));
        } catch (IllegalArgumentException e) {
            //Ignore illegal arg exceptions thrown by triggers doing JIT validation of start and endtime
            log.warn("Trigger had illegal start / end time combination: {}", trigger.getKey(), e);
        }
    }
}
