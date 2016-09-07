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

import io.smartspaces.scheduling.quartz.orientdb.internal.cluster.TriggerRecoverer;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardCalendarDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardJobDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardTriggerDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.MisfireHandler;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.TriggerConverter;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.Clock;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The manager for triggers that have triggered.
 */
public class TriggerRunner {

  private static final Logger LOG = LoggerFactory.getLogger(TriggerRunner.class);

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
  private TriggerRecoverer recoverer;
  private StandardJobDao jobDao;
  private StandardCalendarDao calendarDao;
  private Clock clock;

  public TriggerRunner(TriggerAndJobPersister persister, StandardTriggerDao triggerDao,
      StandardJobDao jobDao, StandardCalendarDao calendarDao, MisfireHandler misfireHandler,
      TriggerConverter triggerConverter, TriggerRecoverer recoverer, Clock clock) {
    this.persister = persister;
    this.triggerDao = triggerDao;
    this.jobDao = jobDao;
    this.calendarDao = calendarDao;
    this.misfireHandler = misfireHandler;
    this.triggerConverter = triggerConverter;
    this.recoverer = recoverer;
    this.clock = clock;
  }

  public List<OperableTrigger> acquireNext(long noLaterThan, int maxCount, long timeWindow)
      throws JobPersistenceException {
    Date noLaterThanDate = new Date(noLaterThan + timeWindow);

    LOG.debug("Finding up to {} triggers which have time less than {}", maxCount, noLaterThanDate);

    List<OperableTrigger> triggers = acquireNextTriggers(noLaterThanDate, maxCount);

    // Because we are handling a batch, we may have done multiple queries and
    // while the result for each
    // query is in fire order, the result for the whole might not be, so sort
    // them again

    Collections.sort(triggers, NEXT_FIRE_TIME_COMPARATOR);

    return triggers;
  }

  private List<OperableTrigger> acquireNextTriggers(Date noLaterThanDate, int maxCount)
      throws JobPersistenceException {
    Map<TriggerKey, OperableTrigger> triggers = new LinkedHashMap<>();

    Set<JobKey> acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();

    // Might want to put this in a loop like the JDBCStore to make sure we get
    // as many triggers as possible.
    for (ODocument triggerDoc : triggerDao.findEligibleToRun(Constants.STATE_WAITING,
        noLaterThanDate, new Date(misfireHandler.getMisfireTime()))) {
      if (maxCount <= triggers.size()) {
        break;
      }

      OperableTrigger trigger = triggerConverter.toTrigger(triggerDoc);

      if (cannotAcquire(triggers, trigger)) {
        continue;
      }

      TriggerKey triggerKey = trigger.getKey();

      JobKey jobKey = trigger.getJobKey();
      JobDetail jobDetail = null;
      try {
        jobDetail = jobDao.retrieveJob(jobKey);
      } catch (Exception e) {
        LOG.error("Error retrieving job {}", jobKey, e);

        try {
          triggerDao.setState(triggerKey, Constants.STATE_ERROR);
        } catch (Exception e2) {
          LOG.error("Could not set trigger {} to error state", triggerKey, e2);
        }
        continue;
      }

      // If can't run more than once, make sure only ends up in list once
      if (jobDetail.isConcurrentExectionDisallowed()) {
        // If shows up again, we don't want to add it into the list of triggers.
        if (acquiredJobKeysForNoConcurrentExec.add(jobKey)) {
          continue;
        }
      }

      if (prepareForFire(noLaterThanDate, trigger)) {
        LOG.debug("Prepared acquired trigger: {}", triggerKey);
        triggerDao.setState(triggerKey, Constants.STATE_ACQUIRED);
        triggers.put(triggerKey, trigger);
      } else {
        LOG.debug("Unable to prepare acquired trigger, unlocking: {}", triggerKey);
      }
      // TODO(keith): Sort out this recovery stuff
      // } else if (lockManager.relockExpired(triggerKey)) {
      // log.debug("Recovering trigger: {}", trigger.getKey());
      // OperableTrigger recoveryTrigger = recoverer.doRecovery(trigger);
      // lockManager.unlockAcquiredTrigger(trigger);
      // if (recoveryTrigger != null &&
      // lockManager.tryTriggerLock(recoveryTrigger.getKey())) {
      // log.debug("Acquired trigger: {}", recoveryTrigger.getKey());
      // triggers.put(recoveryTrigger.getKey(), recoveryTrigger);
      // }
      // }
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

  private boolean cannotAcquire(Map<TriggerKey, OperableTrigger> triggers,
      OperableTrigger trigger) {
    if (trigger == null) {
      return true;
    }

    if (triggers.containsKey(trigger.getKey())) {
      LOG.debug("Skipping trigger {} as we have already acquired it.", trigger.getKey());
      return true;
    }
    return false;
  }

  public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
      throws JobPersistenceException {
    List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>(triggers.size());

    for (OperableTrigger trigger : triggers) {
      LOG.debug("Fired trigger {}", trigger);

      TriggerFiredResult result = null;

      try {
        TriggerFiredBundle bundle = createTriggerFiredBundle(trigger);
        result = new TriggerFiredResult(bundle);
      } catch (Exception dk) {
        result = new TriggerFiredResult(dk);
      }

      results.add(result);

    }
    return results;
  }

  private TriggerFiredBundle createTriggerFiredBundle(OperableTrigger trigger)
      throws JobPersistenceException {
    TriggerKey triggerKey = trigger.getKey();
    String triggerState = triggerDao.getState(triggerKey);
    if (!triggerState.equals(Constants.STATE_ACQUIRED)) {
      return null;
    }

    JobDetail job;
    try {
      job = jobDao.retrieveJob(trigger.getJobKey());
      if (job == null) {
        return null;
      }
    } catch (JobPersistenceException e) {
      LOG.error("Error retrieving job, setting trigger state to error", e);

      triggerDao.setState(triggerKey, Constants.STATE_ERROR);

      throw e;
    }

    Calendar cal = null;
    String calName = trigger.getCalendarName();
    if (calName != null) {
      cal = calendarDao.getCalendar(calName);
      if (cal == null) {
        return null;
      }
    }

    // TODO(keith): Probably need a fired trigger table
    // triggerDao.setState(triggerKey, Constants.STATE_EXECUTING);

    Date prevFireTime = trigger.getPreviousFireTime();

    // This updates the next fire time for the trigger.
    trigger.triggered(cal);

    String state = Constants.STATE_WAITING;
    boolean force = true;

    // TODO: Need code to block all other triggers who might run the job if it
    // doesn't allow concurrent execution.

    if (trigger.getNextFireTime() == null) {
      state = Constants.STATE_COMPLETE;
      force = true;
    }

    LOG.debug("Triggers fired has set trigger to {}", trigger);
    persister.storeTrigger(trigger, job, true, state, force, false);

    job.getJobDataMap().clearDirtyFlag();

    return new TriggerFiredBundle(job, trigger, cal, isRecovering(trigger), clock.now(),
        trigger.getPreviousFireTime(), prevFireTime, trigger.getNextFireTime());
  }

  private boolean isRecovering(OperableTrigger trigger) {
    return trigger.getKey().getGroup().equals(Scheduler.DEFAULT_RECOVERY_GROUP);
  }

  private boolean notAcquirableAfterMisfire(Date noLaterThanDate, OperableTrigger trigger)
      throws JobPersistenceException {
    if (misfireHandler.applyMisfire(trigger)) {
      persister.storeTrigger(trigger, Constants.STATE_WAITING, true);

      LOG.debug("Misfire trigger {}.", trigger.getKey());

      if (persister.removeTriggerWithoutNextFireTime(trigger)) {
        return true;
      }

      // The trigger has misfired and was rescheduled, its firetime may be too
      // far in the future
      // and we don't want to hang the quartz scheduler thread up on
      // <code>sigLock.wait(timeUntilTrigger);</code>
      // so, check again that the trigger is due to fire
      if (trigger.getNextFireTime().after(noLaterThanDate)) {
        LOG.debug("Skipping trigger {} as it misfired and was scheduled for {}.", trigger.getKey(),
            trigger.getNextFireTime());
        return true;
      }
    }
    return false;
  }
}
