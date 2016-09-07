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

package io.smartspaces.scheduling.quartz.orientdb.internal;

import java.util.Date;

import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardLockDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.ExpiryCalculator;

public class LockManager {

  private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);

  private StandardLockDao locksDao;
  private ExpiryCalculator expiryCalculator;

  public LockManager(StandardLockDao locksDao, ExpiryCalculator expiryCalculator) {
    this.locksDao = locksDao;
    this.expiryCalculator = expiryCalculator;
  }

  /**
   * Lock job if it doesn't allow concurrent executions.
   *
   * @param job
   *          job to lock
   */
  public void lockJob(JobDetail job) {
    if (job.isConcurrentExectionDisallowed()) {
      locksDao.lockJob(job);
    }
  }

  public void releaseAcquiredTrigger(OperableTrigger trigger) throws JobPersistenceException {
    try {
      locksDao.unlockTrigger(trigger);
    } catch (Exception e) {
      throw new JobPersistenceException(e.getLocalizedMessage(), e);
    }
  }

  /**
   * Unlock job that have existing, expired lock.
   *
   * @param job
   *          job to potentially unlock
   */
  public void unlockExpiredJob(JobDetail job) {
    ODocument existingLock = locksDao.findJobLock(job.getKey());
    if (existingLock != null) {
      if (expiryCalculator.isJobLockExpired(existingLock)) {
        LOG.debug("Removing expired lock for job {}", job.getKey());
        locksDao.remove(existingLock);
      }
    }
  }

  /**
   * Try to lock given trigger, ignoring errors.
   * 
   * @param triggerKey
   *          trigger to lock
   * 
   * @return {@code true} when successfully locked
   */
  public boolean tryTriggerLock(TriggerKey triggerKey) {
    if (locksDao.doesTriggerLockExist(triggerKey)) {
      return false;
    }
    try {
      locksDao.lockTrigger(triggerKey);
      return true;
    } catch (Exception e) {
      LOG.debug("Failed to lock trigger {}, reason: {}", triggerKey, e.getMessage());
    }
    return false;
  }

  /**
   * Relock trigger if its lock has expired.
   *
   * @param key
   *          trigger to lock
   * @return true when successfully relocked
   */
  public boolean relockExpired(TriggerKey key) {
    ODocument existingLock = locksDao.findTriggerLock(key);
    if (existingLock != null) {
      if (expiryCalculator.isTriggerLockExpired(existingLock)) {
        // When a scheduler is defunct then its triggers become expired
        // after sometime and can be recovered by other schedulers.
        // To check that a trigger is owned by a defunct scheduler we
        // evaluate its LOCK_TIME and try to reassign it to this scheduler.
        // Relock may not be successful when some other scheduler has
        // done it first.
        LOG.debug("Trigger lock {} is expired - re-locking", key);
        Date existingLockDate = existingLock.field(Constants.LOCK_TIME);
        return locksDao.relock(key, existingLockDate);
      } else {
        Date lockTime = existingLock.field(Constants.LOCK_TIME);
        LOG.debug("Trigger lock {} hasn't expired yet. Lock time: {}", key, lockTime);
      }
    } else {
      LOG.warn("Error retrieving expired lock from the database. Maybe it was deleted");
    }
    return false;
  }
}
