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

package io.smartspaces.scheduling.quartz.orientdb.impl.util;

import java.util.Date;

import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.utils.Key;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.impl.Constants;
import io.smartspaces.scheduling.quartz.orientdb.impl.Constants.LockType;

public class Keys {

  public static ODocument createJobLock(JobKey jobKey, String instanceId, Date lockTime) {
    return createLock(LockType.job, instanceId, jobKey, lockTime);
  }

  public static ODocument createTriggerLock(TriggerKey triggerKey, String instanceId,
      Date lockTime) {
    return createLock(LockType.trigger, instanceId, triggerKey, lockTime);
  }

  public static JobKey toJobKey(ODocument dbo) {
    return new JobKey((String) dbo.field(Constants.KEY_NAME), (String) dbo.field(Constants.KEY_GROUP));
  }

  public static TriggerKey toTriggerKey(ODocument dbo) {
    return new TriggerKey((String)dbo.field(Constants.KEY_NAME), (String)dbo.field(Constants.KEY_GROUP));
  }

  private static ODocument createLock(LockType type, String instanceId, Key<?> key, Date lockTime) {
    ODocument lock = new ODocument("QuartzLock");
    lock.field(Constants.LOCK_TYPE, type.name());
    lock.field(Constants.KEY_GROUP, key.getGroup());
    lock.field(Constants.KEY_NAME, key.getName());
    lock.field(Constants.LOCK_INSTANCE_ID, instanceId);
    lock.field(Constants.LOCK_TIME, lockTime);
    return lock;
  }
}
