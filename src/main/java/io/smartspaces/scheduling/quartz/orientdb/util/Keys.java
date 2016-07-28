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

package io.smartspaces.scheduling.quartz.orientdb.util;

import java.util.Date;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.utils.Key;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.orientechnologies.orient.core.record.impl.ODocument;

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

import io.smartspaces.scheduling.quartz.orientdb.Constants;

public class Keys {

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

  public static final String LOCK_TYPE = "type";
  public static final String KEY_NAME = "keyName";
  public static final String KEY_GROUP = "keyGroup";

  public static final Bson KEY_AND_GROUP_FIELDS = Projections.include(KEY_GROUP, KEY_NAME);

  public static Bson createLockRefreshFilter(String instanceId) {
    return Filters.eq(Constants.LOCK_INSTANCE_ID, instanceId);
  }

  public static Bson createRelockFilter(TriggerKey key, Date lockTime) {
    return Filters.and(createTriggerLockFilter(key), createLockTimeFilter(lockTime));
  }

  public static ODocument createJobLock(JobKey jobKey, String instanceId, Date lockTime) {
    return createLock(LockType.job, instanceId, jobKey, lockTime);
  }

  public static ODocument createTriggerLock(TriggerKey triggerKey, String instanceId,
      Date lockTime) {
    return createLock(LockType.trigger, instanceId, triggerKey, lockTime);
  }

  public static Bson toFilter(Key<?> key) {
    return Filters.and(Filters.eq(KEY_GROUP, key.getGroup()), Filters.eq(KEY_NAME, key.getName()));
  }

  public static Bson toFilter(Key<?> key, String instanceId) {
    return Filters.and(Filters.eq(KEY_GROUP, key.getGroup()), Filters.eq(KEY_NAME, key.getName()),
        Filters.eq(Constants.LOCK_INSTANCE_ID, instanceId));
  }

  public static JobKey toJobKey(ODocument dbo) {
    return new JobKey((String) dbo.field(KEY_NAME), (String) dbo.field(KEY_GROUP));
  }

  public static TriggerKey toTriggerKey(ODocument dbo) {
    return new TriggerKey((String)dbo.field(KEY_NAME), (String)dbo.field(KEY_GROUP));
  }

  private static ODocument createLock(LockType type, String instanceId, Key<?> key, Date lockTime) {
    ODocument lock = new ODocument();
    lock.field(LOCK_TYPE, type.name());
    lock.field(KEY_GROUP, key.getGroup());
    lock.field(KEY_NAME, key.getName());
    lock.field(Constants.LOCK_INSTANCE_ID, instanceId);
    lock.field(Constants.LOCK_TIME, lockTime);
    return lock;
  }

  public static Document createLockUpdateDocument(String instanceId, Date newLockTime) {
    return new Document("$set", new Document().append(Constants.LOCK_INSTANCE_ID, instanceId)
        .append(Constants.LOCK_TIME, newLockTime));
  }

 
  private static Bson createLockTimeFilter(Date lockTime) {
    return Filters.eq(Constants.LOCK_TIME, lockTime);
  }
}
