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

package io.smartspaces.scheduling.quartz.orientdb;

import java.util.Date;

import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.dao.StandardLocksDao;
import io.smartspaces.scheduling.quartz.orientdb.util.ExpiryCalculator;

public class LockManager {

  private static final Logger log = LoggerFactory.getLogger(LockManager.class);

  private StandardLocksDao locksDao;
  private ExpiryCalculator expiryCalculator;

  public LockManager(StandardLocksDao locksDao, ExpiryCalculator expiryCalculator) {
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

  public void unlockAcquiredTrigger(OperableTrigger trigger) throws JobPersistenceException {
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
  public void unlockExpired(JobDetail job) {
    ODocument existingLock = locksDao.findJobLock(job.getKey());
    if (existingLock != null) {
      if (expiryCalculator.isJobLockExpired(existingLock)) {
        log.debug("Removing expired lock for job {}", job.getKey());
        locksDao.remove(existingLock);
      }
    }
  }

  /**
   * Try to lock given trigger, ignoring errors.
   * 
   * @param key
   *          trigger to lock
   * @return true when successfully locked, false otherwise
   */
  public boolean tryLock(TriggerKey key) {
    try {
      locksDao.lockTrigger(key);
      return true;
    } catch (Exception e) {
      log.info("Failed to lock trigger {}, reason: {}", key, e.getMessage());
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
        // evaluate
        // its LOCK_TIME and try to reassign it to this scheduler.
        // Relock may not be successful when some other scheduler has
        // done
        // it first.
        log.info("Trigger {} is expired - re-locking", key);
        Date existingLockDate = existingLock.field(Constants.LOCK_TIME);
        return locksDao.relock(key, existingLockDate);
      } else {
        Date lockTime = existingLock.field(Constants.LOCK_TIME);
        log.info("Trigger {} hasn't expired yet. Lock time: {}", key, lockTime);
      }
    } else {
      log.warn("Error retrieving expired lock from the database. Maybe it was deleted");
    }
    return false;
  }
}
