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

package io.smartspaces.scheduling.quartz.orientdb.impl;

public interface Constants {

  /**
   * The job field giving the job durability.
   */
  String JOB_DURABILITY = "durability";

  String JOB_CLASS = "jobClass";

  String JOB_DESCRIPTION = "jobDescription";

  String JOB_REQUESTS_RECOVERY = "requestsRecovery";

  String JOB_DATA = "jobData";

  String TRIGGER_NEXT_FIRE_TIME = "nextFireTime";
  String TRIGGER_JOB_ID = "jobId";
  String TRIGGER_STATE = "state";

  String TRIGGER_CALENDAR_NAME = "calendarName";
  String TRIGGER_CLASS = "class";
  String TRIGGER_DESCRIPTION = "description";
  String TRIGGER_END_TIME = "endTime";
  String TRIGGER_FINAL_FIRE_TIME = "finalFireTime";
  String TRIGGER_FIRE_INSTANCE_ID = "fireInstanceId";
  String TRIGGER_MISFIRE_INSTRUCTION = "misfireInstruction";
  String TRIGGER_PREVIOUS_FIRE_TIME = "previousFireTime";
  String TRIGGER_PRIORITY = "priority";
  String TRIGGER_START_TIME = "startTime";
  
  String TRIGGER_CRON_EXPRESSION = "cronExpression";
  String TRIGGER_TIMEZONE = "timezone";


  public enum LockType {
    /**
     * A trigger lock.
     */
    trigger,

    /**
     * A job lock.
     */
    job
  }

  String LOCK_TYPE = "type";

  String LOCK_INSTANCE_ID = "instanceId";
  String LOCK_TIME = "time";

  String KEY_NAME = "keyName";
  String KEY_GROUP = "keyGroup";
  

  String SCHEDULER_NAME_FIELD = "schedulerName";
  String SCHEDULER_INSTANCE_ID_FIELD = "instanceId";
  String SCHEDULER_LAST_CHECKIN_TIME_FIELD = "lastCheckinTime";
  String SCHEDULER_CHECKIN_INTERVAL_FIELD = "checkinInterval";
  
  String CALENDAR_NAME = "name";
  String CALENDAR_SERIALIZED_OBJECT = "serializedObject";


  String STATE_WAITING = "waiting";
  String STATE_DELETED = "deleted";
  String STATE_COMPLETE = "complete";
  String STATE_PAUSED = "paused";
  String STATE_PAUSED_BLOCKED = "pausedBlocked";
  String STATE_BLOCKED = "blocked";
  String STATE_ERROR = "error";
}
