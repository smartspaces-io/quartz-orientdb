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

package io.smartspaces.scheduling.quartz.orientdb.internal.trigger;

import java.io.IOException;
import java.util.Date;

import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.internal.Constants;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardJobDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.SerialUtils;

public class TriggerConverter {

  private static final Logger log = LoggerFactory.getLogger(TriggerConverter.class);

  private StandardJobDao jobDao;

  public TriggerConverter(StandardJobDao jobDao) {
    this.jobDao = jobDao;
  }

  public ODocument toDocument(OperableTrigger newTrigger, ORID jobId)
      throws JobPersistenceException {
    ODocument trigger = convertToDocument(newTrigger, jobId);
    if (newTrigger.getJobDataMap().size() > 0) {
      try {
        String jobDataString = SerialUtils.serialize(newTrigger.getJobDataMap());
        trigger.field(Constants.JOB_DATA, jobDataString);
      } catch (IOException ioe) {
        throw new JobPersistenceException(
            "Could not serialise job data map on the trigger for " + newTrigger.getKey(), ioe);
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
  public OperableTrigger toTrigger(TriggerKey triggerKey, ODocument triggerDoc)
      throws JobPersistenceException {
    OperableTrigger trigger = createNewInstance(triggerDoc);

    TriggerPropertiesConverter tpd = TriggerPropertiesConverter.getConverterFor(trigger);

    loadCommonProperties(triggerKey, triggerDoc, trigger);

    loadJobData(triggerDoc, trigger);

    loadStartAndEndTimes(triggerDoc, trigger);

    tpd.setExtraPropertiesAfterInstantiation(trigger, triggerDoc);

    ODocument job = triggerDoc.field(Constants.TRIGGER_JOB_ID);
    if (job != null) {
      String keyName = job.field(Constants.KEY_NAME);
      String keyGroup = job.field(Constants.KEY_GROUP);
      trigger.setJobKey(new JobKey(keyName, keyGroup));
      return trigger;
    } else {
      // job was deleted
      return null;
    }
  }

  public OperableTrigger toTrigger(ODocument doc) throws JobPersistenceException {
    TriggerKey key = new TriggerKey((String) doc.field(Constants.KEY_NAME), (String) doc.field(Constants.KEY_GROUP));
    return toTrigger(key, doc);
  }

  private ODocument convertToDocument(OperableTrigger newTrigger, ORID jobId) {
    ODocument trigger = new ODocument("Trigger");
    trigger.field(Constants.TRIGGER_STATE, Constants.STATE_WAITING);
    trigger.field(Constants.TRIGGER_CALENDAR_NAME, newTrigger.getCalendarName());
    trigger.field(Constants.TRIGGER_CLASS, newTrigger.getClass().getName());
    trigger.field(Constants.TRIGGER_DESCRIPTION, newTrigger.getDescription());
    trigger.field(Constants.TRIGGER_END_TIME, newTrigger.getEndTime());
    trigger.field(Constants.TRIGGER_FINAL_FIRE_TIME, newTrigger.getFinalFireTime());
    trigger.field(Constants.TRIGGER_FIRE_INSTANCE_ID, newTrigger.getFireInstanceId());
    trigger.field(Constants.TRIGGER_JOB_ID, jobId);
    trigger.field(Constants.KEY_NAME, newTrigger.getKey().getName());
    trigger.field(Constants.KEY_GROUP, newTrigger.getKey().getGroup());
    trigger.field(Constants.TRIGGER_MISFIRE_INSTRUCTION, newTrigger.getMisfireInstruction());
    trigger.field(Constants.TRIGGER_NEXT_FIRE_TIME, newTrigger.getNextFireTime());
    trigger.field(Constants.TRIGGER_PREVIOUS_FIRE_TIME, newTrigger.getPreviousFireTime());
    trigger.field(Constants.TRIGGER_PRIORITY, newTrigger.getPriority());
    trigger.field(Constants.TRIGGER_START_TIME, newTrigger.getStartTime());
    return trigger;
  }

  private OperableTrigger createNewInstance(ODocument triggerDoc) throws JobPersistenceException {
    String triggerClassName = triggerDoc.field(Constants.TRIGGER_CLASS);
    try {
      @SuppressWarnings("unchecked")
      Class<OperableTrigger> triggerClass =
          (Class<OperableTrigger>) getTriggerClassLoader().loadClass(triggerClassName);
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

  private void loadCommonProperties(TriggerKey triggerKey, ODocument triggerDoc,
      OperableTrigger trigger) {
    trigger.setKey(triggerKey);
    trigger.setCalendarName((String) triggerDoc.field(Constants.TRIGGER_CALENDAR_NAME));
    trigger.setDescription((String) triggerDoc.field(Constants.TRIGGER_DESCRIPTION));
    trigger.setFireInstanceId((String) triggerDoc.field(Constants.TRIGGER_FIRE_INSTANCE_ID));
    trigger.setMisfireInstruction((Integer) triggerDoc.field(Constants.TRIGGER_MISFIRE_INSTRUCTION));
    trigger.setNextFireTime((Date) triggerDoc.field(Constants.TRIGGER_NEXT_FIRE_TIME));
    trigger.setPreviousFireTime((Date) triggerDoc.field(Constants.TRIGGER_PREVIOUS_FIRE_TIME));
    trigger.setPriority((Integer) triggerDoc.field(Constants.TRIGGER_PRIORITY));
  }

  private void loadJobData(ODocument triggerDoc, OperableTrigger trigger)
      throws JobPersistenceException {
    String jobDataString = triggerDoc.field(Constants.JOB_DATA);

    if (jobDataString != null) {
      try {
        SerialUtils.deserialize(trigger.getJobDataMap(), jobDataString);
      } catch (IOException e) {
        throw new JobPersistenceException(
            "Could not deserialize job data for trigger " + trigger.getClass().getName());
      }
    }
  }

  private void loadStartAndEndTimes(ODocument triggerDoc, OperableTrigger trigger) {
    loadStartAndEndTime(triggerDoc, trigger);
    loadStartAndEndTime(triggerDoc, trigger);
  }

  private void loadStartAndEndTime(ODocument triggerDoc, OperableTrigger trigger) {
    try {
      trigger.setStartTime((Date) triggerDoc.field(Constants.TRIGGER_START_TIME));
      trigger.setEndTime((Date) triggerDoc.field(Constants.TRIGGER_END_TIME));
    } catch (IllegalArgumentException e) {
      // Ignore illegal arg exceptions thrown by triggers doing JIT validation
      // of start and endtime
      log.warn("Trigger had illegal start / end time combination: {}", trigger.getKey(), e);
    }
  }
}
