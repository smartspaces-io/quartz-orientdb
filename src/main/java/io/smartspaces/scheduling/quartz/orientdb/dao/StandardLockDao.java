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

package io.smartspaces.scheduling.quartz.orientdb.dao;

import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.createJobLock;
import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.createTriggerLock;
import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.toTriggerKey;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.quartz.utils.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import io.smartspaces.scheduling.quartz.orientdb.Constants;
import io.smartspaces.scheduling.quartz.orientdb.Constants.LockType;
import io.smartspaces.scheduling.quartz.orientdb.StandardOrientDbStoreAssembler;
import io.smartspaces.scheduling.quartz.orientdb.util.Clock;

public class StandardLockDao {

  private static final Logger log = LoggerFactory.getLogger(StandardLockDao.class);

  private final StandardOrientDbStoreAssembler storeAssembler;
  private Clock clock;
  private final String instanceId;

  public StandardLockDao(StandardOrientDbStoreAssembler storeAssembler, Clock clock, String instanceId) {
    this.storeAssembler = storeAssembler;
    this.clock = clock;
    this.instanceId = instanceId;
  }

  public void createIndex(boolean clustered) {
    /*
     * locksCollection.createIndex(Projections.include(KEY_GROUP, KEY_NAME,
     * LOCK_TYPE), new IndexOptions().unique(true));
     * 
     * if (!clustered) { // Need this to stop table scan when removing all locks
     * locksCollection.createIndex(Projections.include(LOCK_INSTANCE_ID));
     * 
     * // remove all locks for this instance on startup
     * locksCollection.deleteMany(Filters.eq(LOCK_INSTANCE_ID, instanceId)); }
     */
  }

  public void dropIndex() {
    // locksCollection.dropIndex("keyName_1_keyGroup_1");
    // locksCollection.dropIndex(KEY_AND_GROUP_FIELDS);
  }

  /**
   * Find the lock for a given job, if it exists.
   * 
   * @param jobKey
   *          the key for the job
   * 
   * @return the lock, or {@code null} if no such lock available
   */
  public ODocument findJobLock(JobKey jobKey) {
    List<ODocument> filter = createLockFilter(LockType.job, jobKey);

    if (!filter.isEmpty()) {
      return filter.get(0);
    } else {
      return null;
    }
  }

  /**
   * Find the lock for a given trigger, if it exists.
   * 
   * @param triggerKey
   *          the key for the trigger
   * 
   * @return the lock, or {@code null} if no such lock available
   */
  public ODocument findTriggerLock(TriggerKey triggerKey) {
    List<ODocument> filter = createLockFilter(LockType.trigger, triggerKey);

    if (!filter.isEmpty()) {
      return filter.get(0);
    } else {
      return null;
    }
  }

  /**
   * Get the keys for all triggers that have locks in this instance.
   * 
   * @return the trigger keys
   */
  public List<TriggerKey> findOwnTriggersLocks() {
    List<TriggerKey> keys = new LinkedList<>();

    for (ODocument doc : getInstanceLocksByLockType(LockType.trigger)) {
      keys.add(toTriggerKey(doc));
    }
    return keys;
  }

  private List<ODocument> getInstanceLocksByLockType(LockType lockType) {
    // TODO(keith): class and field names should come from external constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("select from QuartzLock where instanceId=? and type=?");
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> result = database.command(query).execute(instanceId, lockType.name());
    return result;
  }

  public void lockJob(JobDetail job) {
    log.debug("Inserting lock for job {}", job.getKey());
    ODocument lock = createJobLock(job.getKey(), instanceId, clock.now());
    lock.save();
  }

  public void lockTrigger(TriggerKey key) {
    log.info("Inserting lock for trigger {}", key);
    ODocument lock = createTriggerLock(key, instanceId, clock.now());
    lock.save();
  }

  /**
   * Lock given trigger iff its <b>lockTime</b> haven't changed.
   *
   * <p>
   * Update is performed using "Update document if current" pattern to update
   * iff document in DB hasn't changed - haven't been relocked by other
   * scheduler.
   * </p>
   *
   * @param key
   *          identifies trigger lock
   * @param lockTime
   *          expected current lockTime
   * 
   * @return {@code false} when not found or caught an exception
   */
  public boolean relock(TriggerKey key, Date lockTime) {
    try {
      // TODO(keith): class and field names should come from external constants
      // Also create query ahead of time when DAO starts.
      OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
          "select from QuartzLock where instanceId=? and type=? and keyGroup=? and keyName=? and time=?");
      ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
      List<ODocument> result = database.command(query).execute(instanceId, LockType.trigger.name(),
          key.getGroup(), key.getName(), lockTime);

      if (!result.isEmpty()) {
        ODocument lockDoc = result.get(0);
        lockDoc.field(Constants.LOCK_TIME, clock.now());
        lockDoc.save();
      }
    } catch (Exception e) {
      log.error("Relock failed because: " + e.getMessage(), e);
      return false;
    }

    log.info("Scheduler {} relocked the trigger: {}", instanceId, key);
    return true;
  }

  /**
   * Reset lock time on own lock.
   * 
   * @param key
   *          trigger whose lock to refresh
   * 
   * @return {@code true} on successful update
   *
   * @throws JobPersistenceException
   *           in case of errors from OrientDB
   */
  public boolean updateOwnLock(TriggerKey key) throws JobPersistenceException {
    try {
      // TODO(keith): class and field names should come from external constants
      // Also create query ahead of time when DAO starts.
      OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
          "select from QuartzLock where instanceId=? and keyGroup=? and keyName=?");
      ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
      List<ODocument> result =
          database.command(query).execute(instanceId, key.getGroup(), key.getName());

      for (ODocument lockDoc : result) {
        lockDoc.field(Constants.LOCK_TIME, clock.now());
        lockDoc.save();
      }
    } catch (Exception e) {
      log.error("Lock refresh failed because: " + e.getMessage(), e);
      throw new JobPersistenceException("Lock refresh for scheduler: " + instanceId, e);
    }

    log.info("Scheduler {} refreshed locking time.", instanceId);
    return true;
  }

  /**
   * Unlock the trigger if it still belongs to the current scheduler.
   *
   * @param trigger
   *          to unlock
   */
  public void unlockTrigger(OperableTrigger trigger) {
    log.info("Removing trigger lock {}.{}", trigger.getKey(), instanceId);
    List<ODocument> locks = createLockFilter(LockType.trigger, trigger.getKey());
    for (ODocument lock : locks) {
      lock.delete();
    }
    log.info("Trigger lock {}.{} removed.", trigger.getKey(), instanceId);
  }

  public void unlockJob(JobDetail job) {
    log.debug("Removing lock for job {}", job.getKey());

    List<ODocument> locks = createLockFilter(LockType.job, job.getKey());
    for (ODocument lock : locks) {
      lock.delete();
    }
  }
  
  public void remove(ODocument lockDoc) {
    lockDoc.delete();
  }

  private List<ODocument> createLockFilter(LockType lockType, Key<?> key) {
    // TODO(keith): class and field names should come from external constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
        "select from QuartzLock where instanceId=? and type=? and keyGroup=? and keyName=?");
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> result =
        database.command(query).execute(instanceId, lockType.name(), key.getGroup(), key.getName());

    return result;
  }

}
