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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.internal.cluster.TriggerRecoverer;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardCalendarDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardJobDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardLockDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardTriggerDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.MisfireHandler;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.TriggerConverter;

public class TriggerRunner {

  private static final Logger log = LoggerFactory.getLogger(TriggerRunner.class);

  private static final Comparator<OperableTrigger> NEXT_FIRE_TIME_COMPARATOR =
      new Comparator<OperableTrigger>() {
        @Override
        public int compare(OperableTrigger o1, OperableTrigger o2) {
          return (int) (o1.getNextFireTime().getTime() - o2.getNextFireTime().getTime());
        }
      };

  private MisfireHandler misfireHandler;
  private TriggerAndJobPersister persister;
  private StandardTriggerDao triggerDao;
  private TriggerConverter triggerConverter;
  private LockManager lockManager;
  private TriggerRecoverer recoverer;
  private StandardJobDao jobDao;
  private StandardLockDao locksDao;
  private StandardCalendarDao calendarDao;

  public TriggerRunner(TriggerAndJobPersister persister, StandardTriggerDao triggerDao,
      StandardJobDao jobDao, StandardLockDao locksDao, StandardCalendarDao calendarDao,
      MisfireHandler misfireHandler, TriggerConverter triggerConverter, LockManager lockManager,
      TriggerRecoverer recoverer) {
    this.persister = persister;
    this.triggerDao = triggerDao;
    this.jobDao = jobDao;
    this.locksDao = locksDao;
    this.calendarDao = calendarDao;
    this.misfireHandler = misfireHandler;
    this.triggerConverter = triggerConverter;
    this.lockManager = lockManager;
    this.recoverer = recoverer;
  }

  public List<OperableTrigger> acquireNext(long noLaterThan, int maxCount, long timeWindow)
      throws JobPersistenceException {
    Date noLaterThanDate = new Date(noLaterThan + timeWindow);

    log.debug("Finding up to {} triggers which have time less than {}", maxCount, noLaterThanDate);

    List<OperableTrigger> triggers = acquireNextTriggers(noLaterThanDate, maxCount);

    // Because we are handling a batch, we may have done multiple queries and
    // while the result for each
    // query is in fire order, the result for the whole might not be, so sort
    // them again

    Collections.sort(triggers, NEXT_FIRE_TIME_COMPARATOR);

    return triggers;
  }

  public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
      throws JobPersistenceException {
    List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>(triggers.size());

    for (OperableTrigger trigger : triggers) {
      log.info("Fired trigger {}", trigger.getKey());

      TriggerFiredBundle bundle = createTriggerFiredBundle(trigger);

      if (hasJobDetail(bundle)) {
        JobDetail job = bundle.getJobDetail();
        try {
          lockManager.lockJob(job);
          results.add(new TriggerFiredResult(bundle));
          persister.storeTrigger(trigger, true);
        } catch (Exception dk) {
          log.debug("Job disallows concurrent execution and is already running {}", job.getKey());
          locksDao.unlockTrigger(trigger);
          lockManager.unlockExpired(job);
        }
      }

    }
    return results;
  }

  private List<OperableTrigger> acquireNextTriggers(Date noLaterThanDate, int maxCount)
      throws JobPersistenceException {
    Map<TriggerKey, OperableTrigger> triggers = new HashMap<>();

    for (ODocument triggerDoc : triggerDao.findEligibleToRun(noLaterThanDate)) {
      if (acquiredEnough(triggers, maxCount)) {
        break;
      }

      OperableTrigger trigger = triggerConverter.toTrigger(triggerDoc);

      if (cannotAcquire(triggers, trigger)) {
        continue;
      }

      TriggerKey key = trigger.getKey();
      if (lockManager.tryTriggerLock(key)) {
        if (prepareForFire(noLaterThanDate, trigger)) {
          log.info("Acquired trigger: {}", trigger.getKey());
          triggers.put(trigger.getKey(), trigger);
        } else {
          lockManager.unlockAcquiredTrigger(trigger);
        }
      } else if (lockManager.relockExpired(key)) {
        log.info("Recovering trigger: {}", trigger.getKey());
        OperableTrigger recoveryTrigger = recoverer.doRecovery(trigger);
        lockManager.unlockAcquiredTrigger(trigger);
        if (recoveryTrigger != null && lockManager.tryTriggerLock(recoveryTrigger.getKey())) {
          log.info("Acquired trigger: {}", recoveryTrigger.getKey());
          triggers.put(recoveryTrigger.getKey(), recoveryTrigger);
        }
      }
    }

    return new ArrayList<OperableTrigger>(triggers.values());
  }

  private boolean prepareForFire(Date noLaterThanDate, OperableTrigger trigger)
      throws JobPersistenceException {
    // TODO don't remove when recovering trigger
    if (persister.removeTriggerWithoutNextFireTime(trigger)) {
      return false;
    }

    if (notAcquirableAfterMisfire(noLaterThanDate, trigger)) {
      return false;
    }
    return true;
  }

  private boolean acquiredEnough(Map<TriggerKey, OperableTrigger> triggers, int maxCount) {
    return maxCount <= triggers.size();
  }

  private boolean cannotAcquire(Map<TriggerKey, OperableTrigger> triggers,
      OperableTrigger trigger) {
    if (trigger == null) {
      return true;
    }

    if (triggers.containsKey(trigger.getKey())) {
      log.debug("Skipping trigger {} as we have already acquired it.", trigger.getKey());
      return true;
    }
    return false;
  }

  private TriggerFiredBundle createTriggerFiredBundle(OperableTrigger trigger)
      throws JobPersistenceException {
    Calendar cal = calendarDao.retrieveCalendar(trigger.getCalendarName());
    if (expectedCalendarButNotFound(trigger, cal)) {
      return null;
    }

    Date prevFireTime = trigger.getPreviousFireTime();
    trigger.triggered(cal);

    return new TriggerFiredBundle(retrieveJob(trigger), trigger, cal, isRecovering(trigger),
        new Date(), trigger.getPreviousFireTime(), prevFireTime, trigger.getNextFireTime());
  }

  private boolean expectedCalendarButNotFound(OperableTrigger trigger, Calendar cal) {
    return trigger.getCalendarName() != null && cal == null;
  }

  private boolean isRecovering(OperableTrigger trigger) {
    return trigger.getKey().getGroup().equals(Scheduler.DEFAULT_RECOVERY_GROUP);
  }

  private boolean hasJobDetail(TriggerFiredBundle bundle) {
    return (bundle != null) && (bundle.getJobDetail() != null);
  }

  private boolean notAcquirableAfterMisfire(Date noLaterThanDate, OperableTrigger trigger)
      throws JobPersistenceException {
    if (misfireHandler.applyMisfire(trigger)) {
      persister.storeTrigger(trigger, true);

      log.debug("Misfire trigger {}.", trigger.getKey());

      if (persister.removeTriggerWithoutNextFireTime(trigger)) {
        return true;
      }

      // The trigger has misfired and was rescheduled, its firetime may be too
      // far in the future
      // and we don't want to hang the quartz scheduler thread up on
      // <code>sigLock.wait(timeUntilTrigger);</code>
      // so, check again that the trigger is due to fire
      if (trigger.getNextFireTime().after(noLaterThanDate)) {
        log.debug("Skipping trigger {} as it misfired and was scheduled for {}.", trigger.getKey(),
            trigger.getNextFireTime());
        return true;
      }
    }
    return false;
  }

  private JobDetail retrieveJob(OperableTrigger trigger) throws JobPersistenceException {
    try {
      return jobDao.retrieveJob(trigger.getJobKey());
    } catch (JobPersistenceException e) {
      locksDao.unlockTrigger(trigger);
      throw e;
    }
  }
}
