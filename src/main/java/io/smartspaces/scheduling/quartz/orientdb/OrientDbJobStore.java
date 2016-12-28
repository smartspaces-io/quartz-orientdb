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
/*
 * $Id: MongoDBJobStore.java 253170 2014-01-06 02:28:03Z waded $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package io.smartspaces.scheduling.quartz.orientdb;

import io.smartspaces.scheduling.quartz.orientdb.internal.Constants;
import io.smartspaces.scheduling.quartz.orientdb.internal.InternalClassLoaderHelper;
import io.smartspaces.scheduling.quartz.orientdb.internal.LockProvider;
import io.smartspaces.scheduling.quartz.orientdb.internal.StandardOrientDbStoreAssembler;
import io.smartspaces.scheduling.quartz.orientdb.internal.cluster.CheckinExecutor;
import io.smartspaces.scheduling.quartz.orientdb.internal.db.OrientDbConnector;
import io.smartspaces.scheduling.quartz.orientdb.internal.db.OrientDbConnector.TransactionMethod;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.Clock;

import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The Quartz Job Store that uses OrientDB.
 */
public class OrientDbJobStore implements JobStore {

  private static final Logger LOG = LoggerFactory.getLogger(OrientDbJobStore.class);

  private String collectionPrefix = "quartz_";
  private String dbName;
  private String authDbName;
  private String schedulerName;
  private String instanceId;
  private String orientDbUri;
  private String username = "sooperdooper";
  private String password = "sooperdooper";

  private ClassLoader externalClassLoader;

  /**
   * The threshold for detecting misfires.
   */
  private long misfireThreshold = 60000;
  private long triggerTimeoutMillis = 10 * 60 * 1000L;
  private long jobTimeoutMillis = 10 * 60 * 1000L;

  /**
   * The internal in milliseconds for retrying
   */
  private long dbRetryInterval = 15000L; // 15 secs

  /**
   * The clock to use for timing events.
   */
  private Clock clock = Clock.SYSTEM_CLOCK;

  /**
   * The executor service to use for threads.
   */
  private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

  /**
   * The assembler for the job store.
   */
  private StandardOrientDbStoreAssembler assembler = new StandardOrientDbStoreAssembler();

  /**
   * The future for controlling the misfire handler.
   */
  private Future<?> misfireFuture;

  /**
   * Construct a job store.
   */
  public OrientDbJobStore() {
  }

  /**
   * Construct a job store.
   * 
   * @param orientdbUri
   *          the URI for the OrientDB database
   * @param username
   *          the user name for the database
   * @param password
   *          the password for the database
   */
  public OrientDbJobStore(String orientdbUri, String username, String password) {
    this.orientDbUri = orientdbUri;
    this.username = username;
    this.password = password;
  }

  /**
   * Override to change class loading mechanism, to e.g. dynamic
   * 
   * @param original
   *          default provided by Quartz
   * 
   * @return loader to use for loading of Quartz Jobs classes
   */
  private ClassLoadHelper getClassLoaderHelper(ClassLoadHelper original) {
    ClassLoadHelper retval = original;

    if (externalClassLoader != null) {
      retval = new InternalClassLoaderHelper(externalClassLoader, original);
    }

    return retval;
  }

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler schedulerSignaler)
      throws SchedulerConfigException {
    assembler.build(this, getClassLoaderHelper(loadHelper), schedulerSignaler, clock,
        dbRetryInterval);

    try {
      assembler.getOrientDbConnector().doInTransactionWithoutLock(new TransactionMethod<Void>() {
        @Override
        public Void doInTransaction() throws JobPersistenceException {
          completeInitialize();

          return null;
        }
      });
    } catch (JobPersistenceException e) {
      if (e.getCause() instanceof SchedulerConfigException) {
        throw (SchedulerConfigException) e.getCause();
      }
    }
  }

  private void completeInitialize() throws JobPersistenceException {
    if (isClustered()) {
      try {
        assembler.getTriggerRecoverer().recover();
      } catch (JobPersistenceException e) {
        throw new JobPersistenceException("Fail",
            new SchedulerConfigException("Cannot recover triggers", e));
      }
      assembler.getCheckinExecutor().start();
    } else {
      try {
        // Should look for misfires
        // assembler.getLocksDao().removeAllInstanceLocks();
      } catch (Exception e) {
        throw new JobPersistenceException("Fail",
            new SchedulerConfigException("Cannot remove instance locks", e));
      }
    }
  }

  @Override
  public void schedulerStarted() throws SchedulerException {
    LOG.debug("scheduler started");

    misfireFuture = executorService.submit(new Runnable() {
      @Override
      public void run() {
        Thread.currentThread().setName("Quartz-Misfire");
        assembler.getMisfireHandler().scanForMisfires();
      }
    });
  }

  @Override
  public void schedulerPaused() {
    LOG.debug("scheduler paused");
    // No-op
  }

  @Override
  public void schedulerResumed() {
    LOG.debug("scheduler resumed");
  }

  @Override
  public void shutdown() {
    if (assembler != null) {
      CheckinExecutor checkinExecutor = assembler.getCheckinExecutor();
      if (checkinExecutor != null) {
        checkinExecutor.shutdown();
      }

      assembler.getMisfireHandler().shutdownScanForMisfires();

      OrientDbConnector orientDbConnector = assembler.getOrientDbConnector();
      if (orientDbConnector != null) {
        orientDbConnector.shutdown();
      }
    }
  }

  @Override
  public boolean supportsPersistence() {
    return true;
  }

  @Override
  public long getEstimatedTimeToReleaseAndAcquireTrigger() {
    // this will vary...
    return 200;
  }

  @Override
  public boolean isClustered() {
    return false;
  }

  @Override
  public void storeJob(final JobDetail newJob, final boolean replaceExisting)
      throws JobPersistenceException {
    LOG.debug("Adding job {} with replace={}", newJob, replaceExisting);
    String lockRequired = replaceExisting ? LockProvider.LOCK_TRIGGER : null;
    lockRequired = LockProvider.LOCK_TRIGGER;
    assembler.getOrientDbConnector().doInTransaction(lockRequired, new TransactionMethod<Void>() {
      @Override
      public Void doInTransaction() throws JobPersistenceException {
        assembler.getJobDao().storeJob(newJob, replaceExisting);

        return null;
      }
    });
  }

  @Override
  public void storeJobAndTrigger(final JobDetail newJob, final OperableTrigger newTrigger)
      throws JobPersistenceException {
    LOG.debug("Adding job {}  and trigger {}", newJob, newTrigger);
    assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Void>() {
          @Override
          public Void doInTransaction() throws JobPersistenceException {
            assembler.getPersister().storeJobAndTrigger(newJob, newTrigger);

            return null;
          }
        });
  }

  @Override
  public void storeJobsAndTriggers(Map<JobDetail, List<Trigger>> triggersAndJobs, boolean replace)
      throws JobPersistenceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeJob(final JobKey jobKey) throws JobPersistenceException {
    LOG.debug("Removing job {}", jobKey);
    return assembler.getOrientDbConnector()
        .doInTransaction(LockProvider.LOCK_TRIGGER, new TransactionMethod<Boolean>() {
          @Override
          public Boolean doInTransaction() throws JobPersistenceException {
            return assembler.getPersister().removeJob(jobKey);
          }
        }).booleanValue();
  }

  @Override
  public boolean removeJobs(final List<JobKey> jobKeys) throws JobPersistenceException {
    LOG.debug("Removing jobs {}", jobKeys);
    return assembler.getOrientDbConnector()
        .doInTransaction(LockProvider.LOCK_TRIGGER, new TransactionMethod<Boolean>() {
          @Override
          public Boolean doInTransaction() throws JobPersistenceException {
            return assembler.getPersister().removeJobs(jobKeys);
          }
        }).booleanValue();
  }

  @Override
  public JobDetail retrieveJob(final JobKey jobKey) throws JobPersistenceException {
    LOG.debug("Retrieve job {}", jobKey);
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<JobDetail>() {
          @Override
          public JobDetail doInTransaction() throws JobPersistenceException {
            return assembler.getJobDao().retrieveJob(jobKey);
          }
        });
  }

  @Override
  public void storeTrigger(final OperableTrigger newTrigger, final boolean replaceExisting)
      throws JobPersistenceException {
    LOG.debug("Store trigger {} with replace", newTrigger, replaceExisting);
    String lockRequired = replaceExisting ? LockProvider.LOCK_TRIGGER : null;
    lockRequired = LockProvider.LOCK_TRIGGER;
    assembler.getOrientDbConnector().doInTransaction(lockRequired, new TransactionMethod<Void>() {
      @Override
      public Void doInTransaction() throws JobPersistenceException {
        assembler.getPersister().storeTrigger(newTrigger, Constants.STATE_WAITING, replaceExisting);

        return null;
      }
    });
  }

  @Override
  public boolean removeTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
    LOG.debug("Removing trigger {}", triggerKey);
    return assembler.getOrientDbConnector()
        .doInTransaction(LockProvider.LOCK_TRIGGER, new TransactionMethod<Boolean>() {
          @Override
          public Boolean doInTransaction() throws JobPersistenceException {
            return assembler.getPersister().removeTrigger(triggerKey);
          }
        }).booleanValue();
  }

  @Override
  public boolean removeTriggers(final List<TriggerKey> triggerKeys) throws JobPersistenceException {
    LOG.debug("Removing triggers {}", triggerKeys);
    return assembler.getOrientDbConnector()
        .doInTransaction(LockProvider.LOCK_TRIGGER, new TransactionMethod<Boolean>() {
          @Override
          public Boolean doInTransaction() throws JobPersistenceException {
            return assembler.getPersister().removeTriggers(triggerKeys);
          }
        }).booleanValue();
  }

  @Override
  public boolean replaceTrigger(final TriggerKey triggerKey, final OperableTrigger newTrigger)
      throws JobPersistenceException {
    LOG.debug("Replacing trigger {} with {}", triggerKey, newTrigger);
    return assembler.getOrientDbConnector()
        .doInTransaction(LockProvider.LOCK_TRIGGER, new TransactionMethod<Boolean>() {
          @Override
          public Boolean doInTransaction() throws JobPersistenceException {
            return assembler.getPersister().replaceTrigger(triggerKey, newTrigger,
                Constants.STATE_WAITING);
          }
        }).booleanValue();
  }

  @Override
  public OperableTrigger retrieveTrigger(final TriggerKey triggerKey)
      throws JobPersistenceException {
    LOG.debug("Retrieving trigger {}", triggerKey);
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<OperableTrigger>() {
          @Override
          public OperableTrigger doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerDao().getTrigger(triggerKey);
          }
        });
  }

  @Override
  public boolean checkExists(final JobKey jobKey) throws JobPersistenceException {
    LOG.debug("Checking existence of job {}", jobKey);
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<Boolean>() {
          @Override
          public Boolean doInTransaction() throws JobPersistenceException {
            return assembler.getJobDao().exists(jobKey);
          }
        }).booleanValue();
  }

  @Override
  public boolean checkExists(final TriggerKey triggerKey) throws JobPersistenceException {
    LOG.debug("Checking existence of trigger {}", triggerKey);
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<Boolean>() {
          @Override
          public Boolean doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerDao().exists(triggerKey);
          }
        }).booleanValue();
  }

  @Override
  public void clearAllSchedulingData() throws JobPersistenceException {
    LOG.debug("Clearing all scheduling data");
    assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Void>() {
          @Override
          public Void doInTransaction() throws JobPersistenceException {
            assembler.getJobDao().removeAll();
            assembler.getTriggerDao().removeAll();
            assembler.getCalendarDao().removeAll();
            assembler.getPausedJobGroupsDao().removeAll();
            assembler.getPausedTriggerGroupsDao().removeAll();

            return null;
          }
        });
  }

  @Override
  public void storeCalendar(final String name, final Calendar calendar, boolean replaceExisting,
      boolean updateTriggers) throws JobPersistenceException {
    LOG.debug("Storing calendar {}: {} with replace {}", name, calendar, replaceExisting);
    // TODO implement updating triggers
    if (updateTriggers) {
      throw new UnsupportedOperationException("Updating triggers is not supported.");
    }

    assembler.getOrientDbConnector().doInTransaction(
        updateTriggers ? LockProvider.LOCK_TRIGGER : null, new TransactionMethod<Void>() {
          @Override
          public Void doInTransaction() throws JobPersistenceException {
            assembler.getCalendarDao().store(name, calendar);

            return null;
          }
        });
  }

  @Override
  public boolean removeCalendar(final String calName) throws JobPersistenceException {
    LOG.debug("Remove calendar {}", calName);
    return assembler.getOrientDbConnector()
        .doInTransaction(LockProvider.LOCK_TRIGGER, new TransactionMethod<Boolean>() {
          @Override
          public Boolean doInTransaction() throws JobPersistenceException {
            return assembler.getCalendarDao().remove(calName);
          }
        }).booleanValue();

  }

  @Override
  public Calendar retrieveCalendar(final String calName) throws JobPersistenceException {
    LOG.debug("Retrieve calendar {}", calName);
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<Calendar>() {
          @Override
          public Calendar doInTransaction() throws JobPersistenceException {
            return assembler.getCalendarDao().getCalendar(calName);
          }
        });
  }

  @Override
  public int getNumberOfJobs() throws JobPersistenceException {
    LOG.debug("Get number of jobs");
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<Integer>() {
          @Override
          public Integer doInTransaction() throws JobPersistenceException {
            return assembler.getJobDao().getCount();
          }
        }).intValue();
  }

  @Override
  public int getNumberOfTriggers() throws JobPersistenceException {
    LOG.debug("Get number of triggers");
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<Integer>() {
          @Override
          public Integer doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerDao().getCount();
          }
        }).intValue();

  }

  @Override
  public int getNumberOfCalendars() throws JobPersistenceException {
    LOG.debug("Get number of calendars");
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<Integer>() {
          @Override
          public Integer doInTransaction() throws JobPersistenceException {
            return assembler.getCalendarDao().getCount();
          }
        }).intValue();
  }

  @Override
  public Set<JobKey> getJobKeys(final GroupMatcher<JobKey> matcher) throws JobPersistenceException {
    LOG.debug("Get job keys for {}", matcher);
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<Set<JobKey>>() {
          @Override
          public Set<JobKey> doInTransaction() throws JobPersistenceException {
            return assembler.getJobDao().getJobKeys(matcher);
          }
        });
  }

  @Override
  public Set<TriggerKey> getTriggerKeys(final GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    LOG.debug("Get trigger keys for {}", matcher);
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<Set<TriggerKey>>() {
          @Override
          public Set<TriggerKey> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerDao().getTriggerKeys(matcher);
          }
        });
  }

  @Override
  public List<String> getJobGroupNames() throws JobPersistenceException {
    LOG.debug("Get job group names");
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<List<String>>() {
          @Override
          public List<String> doInTransaction() throws JobPersistenceException {
            return assembler.getJobDao().getGroupNames();
          }
        });
  }

  @Override
  public List<String> getTriggerGroupNames() throws JobPersistenceException {
    LOG.debug("Get trigger group names.");
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<List<String>>() {
          @Override
          public List<String> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerDao().getGroupNames();
          }
        });
  }

  @Override
  public List<String> getCalendarNames() throws JobPersistenceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OperableTrigger> getTriggersForJob(final JobKey jobKey)
      throws JobPersistenceException {
    LOG.debug("Get triggers for job {}", jobKey);
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<List<OperableTrigger>>() {
          @Override
          public List<OperableTrigger> doInTransaction() throws JobPersistenceException {
            return assembler.getPersister().getTriggersForJob(jobKey);
          }
        });
  }

  @Override
  public TriggerState getTriggerState(final TriggerKey triggerKey) throws JobPersistenceException {
    LOG.debug("Get state for trigger {}", triggerKey);
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<TriggerState>() {
          @Override
          public TriggerState doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerStateManager().getState(triggerKey);
          }
        });
  }

  @Override
  public void pauseTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
    LOG.debug("Pause trigger {}", triggerKey);
    assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Void>() {
          @Override
          public Void doInTransaction() throws JobPersistenceException {
            assembler.getTriggerStateManager().pause(triggerKey);

            return null;
          }
        });
  }

  @Override
  public Collection<String> pauseTriggers(final GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    LOG.debug("Pause triggers matching {}", matcher);
    return assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Collection<String>>() {
          @Override
          public Collection<String> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerStateManager().pause(matcher);
          }
        });
  }

  @Override
  public void resumeTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
    LOG.debug("Resume trigger {}", triggerKey);
    assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Void>() {
          @Override
          public Void doInTransaction() throws JobPersistenceException {
            assembler.getTriggerStateManager().resume(triggerKey);

            return null;
          }
        });
  }

  @Override
  public Collection<String> resumeTriggers(final GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    LOG.debug("Resume triggers matching {}", matcher);
    return assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Collection<String>>() {
          @Override
          public Collection<String> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerStateManager().resumeTriggerGroup(matcher);
          }
        });
  }

  @Override
  public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
    LOG.debug("Get paused trigger groups");
    return assembler.getOrientDbConnector()
        .doInTransactionWithoutLock(new TransactionMethod<Set<String>>() {
          @Override
          public Set<String> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerStateManager().getPausedTriggerGroups();
          }
        });
  }

  @Override
  public void pauseAll() throws JobPersistenceException {
    LOG.debug("Pause all");
    assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Void>() {
          @Override
          public Void doInTransaction() throws JobPersistenceException {
            assembler.getTriggerStateManager().pauseAll();

            return null;
          }
        });
  }

  @Override
  public void resumeAll() throws JobPersistenceException {
    LOG.debug("Resume all");
    assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Void>() {
          @Override
          public Void doInTransaction() throws JobPersistenceException {
            assembler.getTriggerStateManager().resumeAll();

            return null;
          }
        });
  }

  @Override
  public void pauseJob(final JobKey jobKey) throws JobPersistenceException {
    LOG.debug("Pause job {}", jobKey);
    assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Void>() {
          @Override
          public Void doInTransaction() throws JobPersistenceException {
            assembler.getTriggerStateManager().pauseJob(jobKey);

            return null;
          }
        });
  }

  @Override
  public Collection<String> pauseJobs(final GroupMatcher<JobKey> groupMatcher)
      throws JobPersistenceException {
    LOG.debug("Pause jobs matching {}", groupMatcher);
    return assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Collection<String>>() {
          @Override
          public Collection<String> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerStateManager().pauseJobs(groupMatcher);
          }
        });
  }

  @Override
  public void resumeJob(final JobKey jobKey) throws JobPersistenceException {
    LOG.debug("Resume job {}", jobKey);
    assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Void>() {
          @Override
          public Void doInTransaction() throws JobPersistenceException {
            assembler.getTriggerStateManager().resumeJob(jobKey);

            return null;
          }
        });
  }

  @Override
  public Collection<String> resumeJobs(final GroupMatcher<JobKey> groupMatcher)
      throws JobPersistenceException {
    LOG.debug("Resume jobs matching job {}", groupMatcher);
    return assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Collection<String>>() {
          @Override
          public Collection<String> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerStateManager().resumeJobs(groupMatcher);
          }
        });
  }

  @Override
  public List<OperableTrigger> acquireNextTriggers(final long noLaterThan, final int maxCount,
      final long timeWindow) throws JobPersistenceException {
    LOG.debug("Acquiring next triggers for {} ({}) maxcount {}, timeWindow {}", noLaterThan,
        new Date(noLaterThan), maxCount, timeWindow);

    return assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<List<OperableTrigger>>() {
          @Override
          public List<OperableTrigger> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerRunner().acquireNext(noLaterThan, maxCount, timeWindow);
          }
        });
  }

  @Override
  public void releaseAcquiredTrigger(final OperableTrigger trigger) throws JobPersistenceException {
    LOG.debug("Releasing acquired trigger {}", trigger);
    assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Void>() {
          @Override
          public Void doInTransaction() throws JobPersistenceException {
            assembler.getTriggerStateManager().releaseAcquiredTrigger(trigger);

            return null;
          }
        });
  }

  @Override
  public List<TriggerFiredResult> triggersFired(final List<OperableTrigger> triggers)
      throws JobPersistenceException {
    LOG.debug("Triggers fired {}", triggers);
    return assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<List<TriggerFiredResult>>() {
          @Override
          public List<TriggerFiredResult> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerRunner().triggersFired(triggers);
          }
        });
  }

  @Override
  public void triggeredJobComplete(final OperableTrigger trigger, final JobDetail job,
      final CompletedExecutionInstruction triggerInstCode) throws JobPersistenceException {
    LOG.debug("Triggered job complete {} for job {} with instruction {}", trigger, job,
        triggerInstCode);
    assembler.getOrientDbConnector().doInTransaction(LockProvider.LOCK_TRIGGER,
        new TransactionMethod<Void>() {
          @Override
          public Void doInTransaction() throws JobPersistenceException {
            assembler.getJobCompleteHandler().jobComplete(trigger, job, triggerInstCode);

            return null;
          }
        });
  }

  @Override
  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  @Override
  public void setInstanceName(String schedName) {
    // Used as part of cluster node identifier:
    schedulerName = schedName;
  }

  public String getInstanceId() {
    return instanceId;
  }

  @Override
  public void setThreadPoolSize(int poolSize) {
    // No-op
  }

  public String getSchedulerName() {
    return schedulerName;
  }

  public void setSchedulerName(String schedulerName) {
    this.schedulerName = schedulerName;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setCollectionPrefix(String prefix) {
    collectionPrefix = prefix + "_";
  }

  public void setOrientDbUri(final String orientdbUri) {
    this.orientDbUri = orientdbUri;
  }

  public String getOrientDbUri() {
    return orientDbUri;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getUsername() {
    return username;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getPassword() {
    return password;
  }

  public void setMisfireThreshold(long misfireThreshold) {
    this.misfireThreshold = misfireThreshold;
  }

  public long getMisfireThreshold() {
    return misfireThreshold;
  }

  public void setTriggerTimeoutMillis(long triggerTimeoutMillis) {
    this.triggerTimeoutMillis = triggerTimeoutMillis;
  }

  public long getTriggerTimeoutMillis() {
    return triggerTimeoutMillis;
  }

  public void setJobTimeoutMillis(long jobTimeoutMillis) {
    this.jobTimeoutMillis = jobTimeoutMillis;
  }

  public long getJobTimeoutMillis() {
    return jobTimeoutMillis;
  }

  public String getAuthDbName() {
    return authDbName;
  }

  public void setAuthDbName(String authDbName) {
    this.authDbName = authDbName;
  }

  public Clock getClock() {
    return clock;
  }

  public void setClock(Clock clock) {
    this.clock = clock;
  }

  public long getDbRetryInterval() {
    return dbRetryInterval;
  }

  public void setDbRetryInterval(long dbRetryInterval) {
    this.dbRetryInterval = dbRetryInterval;
  }

  public ScheduledExecutorService getExecutorService() {
    return executorService;
  }

  public void setExecutorService(ScheduledExecutorService executorService) {
    this.executorService = executorService;
  }

  public void setExternalClassLoader(ClassLoader externalClassLoader) {
    this.externalClassLoader = externalClassLoader;
  }

}
