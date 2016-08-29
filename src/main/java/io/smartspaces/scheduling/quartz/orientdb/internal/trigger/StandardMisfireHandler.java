/*
 * Copyright (C) 2016 Keith M. Hughes
 * Forked from code (c) Michael S. Klishin, Alex Petrov, 2011-2015.
 * Forked from code from MuleSoft.
 * Contains some code from the Terrecota JDBC Job Store.
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

import io.smartspaces.scheduling.quartz.orientdb.internal.Constants;
import io.smartspaces.scheduling.quartz.orientdb.internal.LockProvider;
import io.smartspaces.scheduling.quartz.orientdb.internal.TriggerAndJobPersister;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardCalendarDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardTriggerDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.db.OrientDbConnector;
import io.smartspaces.scheduling.quartz.orientdb.internal.db.OrientDbConnector.TransactionMethod;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.Clock;

import org.quartz.Calendar;
import org.quartz.JobPersistenceException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Handle misfires.
 */
public class StandardMisfireHandler implements MisfireHandler, MisfireHandler {

  /**
   * The time in milliseconds to sleep between misfire scans.
   */
  private static final long TIME_TO_SLEEP_BETWEEN_SCANS = 50l;

  /**
   * The logger for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(StandardMisfireHandler.class);

  /**
   * The persister for triggers and jobs.
   */
  private final TriggerAndJobPersister triggerAndJobPersister;

  /**
   * The DAO for triggers.
   */
  private final StandardTriggerDao triggerDao;

  /**
   * O The DAO for calendars.
   */
  private final StandardCalendarDao calendarDao;

  /**
   * The threshold for misfires, in milliseconds.
   */
  private final long misfireThreshold;

  /**
   * The time between database retries, in milliseconds.
   */
  private long dbRetryInterval;

  /**
   * The maximum number of jobs to try and recover at a time.
   */
  private int maxToRecoverAtATime = 20;

  /**
   * The clock to use for time.
   */
  private final Clock clock;

  /**
   * The database connector.
   */
  private final OrientDbConnector orientDbConnector;

  /**
   * The signaler for the scheduler
   */
  private final SchedulerSignaler schedulerSignaler;

  /**
   * {@code true} if the misfire scan should be shut down.
   */
  private volatile boolean shutdownMisfireScan = false;

  /**
   * The number of failures there has been during misfire scans.
   */
  private int numMisfireScanFails = 0;

  public StandardMisfireHandler(TriggerAndJobPersister triggerAndJobPersister,
      StandardTriggerDao triggerDao, StandardCalendarDao calendarDao, long misfireThreshold,
      long dbRetryInterval, OrientDbConnector orientDbConnector, Clock clock,
      SchedulerSignaler schedulerSignaler) {
    this.triggerAndJobPersister = triggerAndJobPersister;
    this.triggerDao = triggerDao;
    this.calendarDao = calendarDao;
    this.misfireThreshold = misfireThreshold;
    this.dbRetryInterval = dbRetryInterval;
    this.orientDbConnector = orientDbConnector;
    this.clock = clock;
    this.schedulerSignaler = schedulerSignaler;
  }

  /* (non-Javadoc)
   * @see io.smartspaces.scheduling.quartz.orientdb.internal.trigger.MisfireHandler#applyMisfireOnRecovery(org.quartz.spi.OperableTrigger)
   */
  @Override
  public boolean applyMisfireOnRecovery(OperableTrigger trigger) throws JobPersistenceException {
    if (trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
      return false;
    }

    Calendar cal = null;
    if (trigger.getCalendarName() != null) {
      cal = retrieveCalendar(trigger);
    }

    schedulerSignaler.notifyTriggerListenersMisfired(trigger);

    trigger.updateAfterMisfire(cal);

    return trigger.getNextFireTime() != null;
  }

  /* (non-Javadoc)
   * @see io.smartspaces.scheduling.quartz.orientdb.internal.trigger.MisfireHandler#applyMisfire(org.quartz.spi.OperableTrigger)
   */
  @Override
  public boolean applyMisfire(OperableTrigger trigger) throws JobPersistenceException {
    Date fireTime = trigger.getNextFireTime();
    if (misfireIsNotApplicable(trigger, fireTime)) {
      return false;
    }

    org.quartz.Calendar cal = retrieveCalendar(trigger);

    schedulerSignaler.notifyTriggerListenersMisfired((OperableTrigger) trigger.clone());

    trigger.updateAfterMisfire(cal);

    if (trigger.getNextFireTime() == null) {
      schedulerSignaler.notifySchedulerListenersFinalized(trigger);
    } else if (fireTime.equals(trigger.getNextFireTime())) {
      return false;
    }
    return true;
  }

  /* (non-Javadoc)
   * @see io.smartspaces.scheduling.quartz.orientdb.internal.trigger.MisfireHandler#getMisfireTime()
   */
  @Override
  public long getMisfireTime() {
    long misfireTime = clock.millis();
    if (misfireThreshold > 0) {
      misfireTime -= misfireThreshold;
    }

    return misfireTime;
  }

  private int getMaxMisfiresToHandleAtATime() {
    return maxToRecoverAtATime;
  }

  /* (non-Javadoc)
   * @see io.smartspaces.scheduling.quartz.orientdb.internal.trigger.MisfireHandler#scanForMisfires()
   */
  @Override
  public void scanForMisfires() {

    while (!shutdownMisfireScan) {

      long sTime = clock.millis();

      RecoverMisfiredJobsResult recoverMisfiredJobsResult = scanAndProcessMisfires();

      if (recoverMisfiredJobsResult.getProcessedMisfiredTriggerCount() > 0) {
        schedulerSignaler.signalSchedulingChange(recoverMisfiredJobsResult.getEarliestNewTime());
      }

      if (!shutdownMisfireScan) {
        // At least a short pause to help balance threads
        long timeToSleep = TIME_TO_SLEEP_BETWEEN_SCANS;
        if (!recoverMisfiredJobsResult.hasMoreMisfiredTriggers()) {
          timeToSleep = misfireThreshold - (clock.millis() - sTime);
          if (timeToSleep <= 0) {
            timeToSleep = TIME_TO_SLEEP_BETWEEN_SCANS;
          }

          if (numMisfireScanFails > 0) {
            timeToSleep = Math.max(dbRetryInterval, timeToSleep);
          }
        }

        try {
          Thread.sleep(timeToSleep);
        } catch (Exception ignore) {
        }
      } // while !shutdown
    }
  }

  /* (non-Javadoc)
   * @see io.smartspaces.scheduling.quartz.orientdb.internal.trigger.MisfireHandler#shutdownScanForMisfires()
   */
  @Override
  public void shutdownScanForMisfires() {
    shutdownMisfireScan = true;
  }

  /**
   * Scan and process any misfires.
   * 
   * @return the result of the processing
   */
  private RecoverMisfiredJobsResult scanAndProcessMisfires() {
    try {
      LOG.debug("MisfireHandler: scanning for misfires...");

      RecoverMisfiredJobsResult res = doRecoverMisfires();
      numMisfireScanFails = 0;
      return res;
    } catch (Exception e) {
      if (numMisfireScanFails % 4 == 0) {
        LOG.error("MisfireHandler: Error handling misfires: " + e.getMessage(), e);
      }
      numMisfireScanFails++;
    }
    return RecoverMisfiredJobsResult.NO_OP;
  }

  /**
   * Attempt to recover misfires.
   * 
   * <p>
   * This runs inside a transaction.
   * 
   * @return the result for misfires.
   * 
   * @throws JobPersistenceException
   */
  private RecoverMisfiredJobsResult doRecoverMisfires() throws JobPersistenceException {
    return orientDbConnector.doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<RecoverMisfiredJobsResult>() {

          @Override
          public RecoverMisfiredJobsResult doInTransaction() throws JobPersistenceException {
            return recoverMisfiredJobs(false);
          }

        });
  }

  protected RecoverMisfiredJobsResult recoverMisfiredJobs(boolean recovering)
      throws JobPersistenceException {

    // If recovering, we want to handle all of the misfired
    // triggers right away.
    int maxMisfiresToHandleAtATime = (recovering) ? -1 : getMaxMisfiresToHandleAtATime();

    List<TriggerKey> misfiredTriggers = new LinkedList<TriggerKey>();
    long earliestNewTime = Long.MAX_VALUE;

    // We must still look for the MISFIRED state in case triggers were left
    // in this state when upgrading to this version that does not support it.
    boolean hasMoreMisfiredTriggers = triggerDao.hasMisfiredTriggersInState(Constants.STATE_WAITING,
        getMisfireTime(), maxMisfiresToHandleAtATime, misfiredTriggers);

    if (hasMoreMisfiredTriggers) {
      LOG.info("Handling the first " + misfiredTriggers.size()
          + " triggers that missed their scheduled fire-time.  "
          + "More misfired triggers remain to be processed.");
    } else if (misfiredTriggers.size() > 0) {
      LOG.info("Handling " + misfiredTriggers.size()
          + " trigger(s) that missed their scheduled fire-time.");
    } else {
      LOG.debug("Found 0 triggers that missed their scheduled fire-time.");
      return RecoverMisfiredJobsResult.NO_OP;
    }

    for (TriggerKey triggerKey : misfiredTriggers) {

      OperableTrigger trig = triggerDao.getTrigger(triggerKey);

      if (trig == null) {
        continue;
      }

      doUpdateOfMisfiredTrigger(trig, false, Constants.STATE_WAITING, recovering);

      if (trig.getNextFireTime() != null && trig.getNextFireTime().getTime() < earliestNewTime)
        earliestNewTime = trig.getNextFireTime().getTime();
    }

    return new RecoverMisfiredJobsResult(hasMoreMisfiredTriggers, misfiredTriggers.size(),
        earliestNewTime);
  }

  /* (non-Javadoc)
   * @see io.smartspaces.scheduling.quartz.orientdb.internal.trigger.MisfireHandler#updateMisfiredTrigger(org.quartz.TriggerKey, java.lang.String, boolean)
   */
  @Override
  public boolean updateMisfiredTrigger(TriggerKey triggerKey, String newStateIfNotComplete,
      boolean forceState) throws JobPersistenceException {
    // TODO(keith): For resumeTrigger
    try {

      OperableTrigger trig = triggerDao.getTrigger(triggerKey);

      long misfireTime = getMisfireTime();

      if (trig.getNextFireTime().getTime() > misfireTime) {
        return false;
      }

      doUpdateOfMisfiredTrigger(trig, forceState, newStateIfNotComplete, false);

      return true;

    } catch (Exception e) {
      throw new JobPersistenceException(
          "Couldn't update misfired trigger '" + triggerKey + "': " + e.getMessage(), e);
    }
  }

  private void doUpdateOfMisfiredTrigger(OperableTrigger trig, boolean forceState,
      String newStateIfNotComplete, boolean recovering) throws JobPersistenceException {
    Calendar cal = null;
    if (trig.getCalendarName() != null) {
      cal = calendarDao.getCalendar(trig.getCalendarName());
    }

    schedulerSignaler.notifyTriggerListenersMisfired(trig);

    trig.updateAfterMisfire(cal);

    if (trig.getNextFireTime() == null) {
      triggerAndJobPersister.storeTrigger(trig, null, true, Constants.STATE_COMPLETE, forceState,
          recovering);
      schedulerSignaler.notifySchedulerListenersFinalized(trig);
    } else {
      triggerAndJobPersister.storeTrigger(trig, null, true, newStateIfNotComplete, forceState,
          false);
    }
  }

  private boolean misfireIsNotApplicable(OperableTrigger trigger, Date fireTime) {
    return fireTime == null || isNotMisfired(fireTime)
        || trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY;
  }

  private boolean isNotMisfired(Date fireTime) {
    return getMisfireTime() < fireTime.getTime();
  }

  private Calendar retrieveCalendar(OperableTrigger trigger) throws JobPersistenceException {
    return calendarDao.getCalendar(trigger.getCalendarName());
  }

  private static class RecoverMisfiredJobsResult {
    /**
     * A result that means there is nothing to be handled.
     */
    public static final RecoverMisfiredJobsResult NO_OP =
        new RecoverMisfiredJobsResult(false, 0, Long.MAX_VALUE);

    private boolean hasMoreMisfiredTriggers;
    private int processedMisfiredTriggerCount;
    private long earliestNewTime;

    public RecoverMisfiredJobsResult(boolean hasMoreMisfiredTriggers,
        int processedMisfiredTriggerCount, long earliestNewTime) {
      this.hasMoreMisfiredTriggers = hasMoreMisfiredTriggers;
      this.processedMisfiredTriggerCount = processedMisfiredTriggerCount;
      this.earliestNewTime = earliestNewTime;
    }

    public boolean hasMoreMisfiredTriggers() {
      return hasMoreMisfiredTriggers;
    }

    public int getProcessedMisfiredTriggerCount() {
      return processedMisfiredTriggerCount;
    }

    public long getEarliestNewTime() {
      return earliestNewTime;
    }

    @Override
    public String toString() {
      return "RecoverMisfiredJobsResult [hasMoreMisfiredTriggers=" + hasMoreMisfiredTriggers
          + ", processedMisfiredTriggerCount=" + processedMisfiredTriggerCount
          + ", earliestNewTime=" + earliestNewTime + "]";
    }
  }

}
