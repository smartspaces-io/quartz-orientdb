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

import io.smartspaces.scheduling.quartz.orientdb.internal.StandardOrientDbStoreAssembler;
import io.smartspaces.scheduling.quartz.orientdb.internal.cluster.CheckinExecutor;
import io.smartspaces.scheduling.quartz.orientdb.internal.db.StandardOrientDbConnector;
import io.smartspaces.scheduling.quartz.orientdb.internal.db.StandardOrientDbConnector.TransactionMethod;

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

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The Quartz Job Store that uses OrientDB.
 */
public class OrientDbJobStore implements JobStore {

  private StandardOrientDbStoreAssembler assembler = new StandardOrientDbStoreAssembler();

  private String collectionPrefix = "quartz_";
  private String dbName;
  private String authDbName;
  private String schedulerName;
  private String instanceId;
  private String[] addresses;
  private String orientDbUri;
  private String username = "sooperdooper";
  private String password = "sooperdooper";
  long misfireThreshold = 5000;
  long triggerTimeoutMillis = 10 * 60 * 1000L;
  long jobTimeoutMillis = 10 * 60 * 1000L;
  private boolean clustered = false;
  long clusterCheckinIntervalMillis = 7500;

  // Options for the Mongo client.
  Boolean mongoOptionSocketKeepAlive;
  Integer mongoOptionMaxConnectionsPerHost;
  Integer mongoOptionConnectTimeoutMillis;
  Integer mongoOptionSocketTimeoutMillis; // read timeout
  Integer mongoOptionThreadsAllowedToBlockForConnectionMultiplier;
  Boolean mongoOptionEnableSSL;
  Boolean mongoOptionSslInvalidHostNameAllowed;

  int mongoOptionWriteConcernTimeoutMillis = 5000;

  public OrientDbJobStore() {
  }

  public OrientDbJobStore(final String orientdbUri, final String username, final String password) {
    this.orientDbUri = orientdbUri;
    this.username = username;
    this.password = password;
  }

  /**
   * Override to change class loading mechanism, to e.g. dynamic
   * 
   * @param original
   *          default provided by Quartz
   * @return loader to use for loading of Quartz Jobs' classes
   */
  public ClassLoadHelper getClassLoaderHelper(ClassLoadHelper original) {
    return original;
  }

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler)
      throws SchedulerConfigException {
    assembler.build(this, loadHelper, signaler);

    if (isClustered()) {
      try {
        assembler.getTriggerRecoverer().recover();
      } catch (JobPersistenceException e) {
        throw new SchedulerConfigException("Cannot recover triggers", e);
      }
      assembler.getCheckinExecutor().start();
    } else {
      try {
        assembler.getLocksDao().removeAllInstanceLocks();
      } catch (Exception e) {
        throw new SchedulerConfigException("Cannot remove instance locks", e);
      }
    }
  }

  @Override
  public void schedulerStarted() throws SchedulerException {
    // No-op
  }

  @Override
  public void schedulerPaused() {
    // No-op
  }

  @Override
  public void schedulerResumed() {
  }

  @Override
  public void shutdown() {
    if (assembler != null) {
      CheckinExecutor checkinExecutor = assembler.getCheckinExecutor();
      if (checkinExecutor != null) {
        checkinExecutor.shutdown();
      }
      StandardOrientDbConnector orientDbConnector = assembler.getOrientDbConnector();
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

  /**
   * Set whether this instance is part of a cluster.
   */
  public void setIsClustered(boolean isClustered) {
    this.clustered = isClustered;
  }

  @Override
  public boolean isClustered() {
    return clustered;
  }

  /**
   * Set the frequency (in milliseconds) at which this instance "checks-in" with
   * the other instances of the cluster.
   *
   * Affects the rate of detecting failed instances.
   */
  public void setClusterCheckinInterval(long clusterCheckinInterval) {
    this.clusterCheckinIntervalMillis = clusterCheckinInterval;
  }

  /**
   * Job and Trigger storage Methods
   */
  @Override
  public void storeJobAndTrigger(final JobDetail newJob, final OperableTrigger newTrigger)
      throws JobPersistenceException {
    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
      @Override
      public Void doInTransaction() throws JobPersistenceException {
        assembler.getPersister().storeJobAndTrigger(newJob, newTrigger);

        return null;
      }
    });
  }

  @Override
  public void storeJob(final JobDetail newJob, final boolean replaceExisting)
      throws JobPersistenceException {
    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
      @Override
      public Void doInTransaction() throws JobPersistenceException {
        assembler.getJobDao().storeJob(newJob, replaceExisting);

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
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Boolean>() {
      @Override
      public Boolean doInTransaction() throws JobPersistenceException {
        return assembler.getPersister().removeJob(jobKey);
      }
    }).booleanValue();
  }

  @Override
  public boolean removeJobs(final List<JobKey> jobKeys) throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Boolean>() {
      @Override
      public Boolean doInTransaction() throws JobPersistenceException {
        return assembler.getPersister().removeJobs(jobKeys);
      }
    }).booleanValue();
  }

  @Override
  public JobDetail retrieveJob(final JobKey jobKey) throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<JobDetail>() {
      @Override
      public JobDetail doInTransaction() throws JobPersistenceException {
        return assembler.getJobDao().retrieveJob(jobKey);
      }
    });
  }

  @Override
  public void storeTrigger(final OperableTrigger newTrigger, final boolean replaceExisting)
      throws JobPersistenceException {
    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
      @Override
      public Void doInTransaction() throws JobPersistenceException {
        assembler.getPersister().storeTrigger(newTrigger, replaceExisting);

        return null;
      }
    });
  }

  @Override
  public boolean removeTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Boolean>() {
      @Override
      public Boolean doInTransaction() throws JobPersistenceException {
        return assembler.getPersister().removeTrigger(triggerKey);
      }
    }).booleanValue();
  }

  @Override
  public boolean removeTriggers(final List<TriggerKey> triggerKeys) throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Boolean>() {
      @Override
      public Boolean doInTransaction() throws JobPersistenceException {
        return assembler.getPersister().removeTriggers(triggerKeys);
      }
    }).booleanValue();
  }

  @Override
  public boolean replaceTrigger(final TriggerKey triggerKey, final OperableTrigger newTrigger)
      throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Boolean>() {
      @Override
      public Boolean doInTransaction() throws JobPersistenceException {
        return assembler.getPersister().replaceTrigger(triggerKey, newTrigger);
      }
    }).booleanValue();
  }

  @Override
  public OperableTrigger retrieveTrigger(final TriggerKey triggerKey)
      throws JobPersistenceException {
    return assembler.getOrientDbConnector()
        .doInTransaction(new TransactionMethod<OperableTrigger>() {
          @Override
          public OperableTrigger doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerDao().getTrigger(triggerKey);
          }
        });
  }

  @Override
  public boolean checkExists(final JobKey jobKey) throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Boolean>() {
      @Override
      public Boolean doInTransaction() throws JobPersistenceException {
        return assembler.getJobDao().exists(jobKey);
      }
    }).booleanValue();
  }

  @Override
  public boolean checkExists(final TriggerKey triggerKey) throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Boolean>() {
      @Override
      public Boolean doInTransaction() throws JobPersistenceException {
        return assembler.getTriggerDao().exists(triggerKey);
      }
    }).booleanValue();
  }

  @Override
  public void clearAllSchedulingData() throws JobPersistenceException {
    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
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
    // TODO implement updating triggers
    if (updateTriggers) {
      throw new UnsupportedOperationException("Updating triggers is not supported.");
    }

    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
      @Override
      public Void doInTransaction() throws JobPersistenceException {
        assembler.getCalendarDao().store(name, calendar);

        return null;
      }
    });
  }

  @Override
  public boolean removeCalendar(final String calName) throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Boolean>() {
      @Override
      public Boolean doInTransaction() throws JobPersistenceException {
        return assembler.getCalendarDao().remove(calName);
      }
    }).booleanValue();

  }

  @Override
  public Calendar retrieveCalendar(final String calName) throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Calendar>() {
      @Override
      public Calendar doInTransaction() throws JobPersistenceException {
        return assembler.getCalendarDao().retrieveCalendar(calName);
      }
    });
  }

  @Override
  public int getNumberOfJobs() throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Integer>() {
      @Override
      public Integer doInTransaction() throws JobPersistenceException {
        return assembler.getJobDao().getCount();
      }
    }).intValue();
  }

  @Override
  public int getNumberOfTriggers() throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Integer>() {
      @Override
      public Integer doInTransaction() throws JobPersistenceException {
        return assembler.getTriggerDao().getCount();
      }
    }).intValue();

  }

  @Override
  public int getNumberOfCalendars() throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Integer>() {
      @Override
      public Integer doInTransaction() throws JobPersistenceException {
        return assembler.getCalendarDao().getCount();
      }
    }).intValue();
  }

  @Override
  public Set<JobKey> getJobKeys(final GroupMatcher<JobKey> matcher) throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Set<JobKey>>() {
      @Override
      public Set<JobKey> doInTransaction() throws JobPersistenceException {
        return assembler.getJobDao().getJobKeys(matcher);
      }
    });
  }

  @Override
  public Set<TriggerKey> getTriggerKeys(final GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    return assembler.getOrientDbConnector()
        .doInTransaction(new TransactionMethod<Set<TriggerKey>>() {
          @Override
          public Set<TriggerKey> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerDao().getTriggerKeys(matcher);
          }
        });
  }

  @Override
  public List<String> getJobGroupNames() throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<List<String>>() {
      @Override
      public List<String> doInTransaction() throws JobPersistenceException {
        return assembler.getJobDao().getGroupNames();
      }
    });
  }

  @Override
  public List<String> getTriggerGroupNames() throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<List<String>>() {
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
  public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
    return assembler.getPersister().getTriggersForJob(jobKey);
  }

  @Override
  public TriggerState getTriggerState(final TriggerKey triggerKey) throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<TriggerState>() {
      @Override
      public TriggerState doInTransaction() throws JobPersistenceException {
        return assembler.getTriggerStateManager().getState(triggerKey);
      }
    });
  }

  @Override
  public void pauseTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
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
    return assembler.getOrientDbConnector()
        .doInTransaction(new TransactionMethod<Collection<String>>() {
          @Override
          public Collection<String> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerStateManager().pause(matcher);
          }
        });
  }

  @Override
  public void resumeTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
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
    return assembler.getOrientDbConnector()
        .doInTransaction(new TransactionMethod<Collection<String>>() {
          @Override
          public Collection<String> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerStateManager().resume(matcher);
          }
        });
  }

  @Override
  public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Set<String>>() {
      @Override
      public Set<String> doInTransaction() throws JobPersistenceException {
        return assembler.getTriggerStateManager().getPausedTriggerGroups();
      }
    });
  }

  /**
   * Get all paused groups.
   * 
   * <p>
   * only for tests.
   * 
   * @return all paused groups
   * 
   * @throws JobPersistenceException
   */
  public List<String> getPausedJobGroups() throws JobPersistenceException {
    return assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<List<String>>() {
      @Override
      public List<String> doInTransaction() throws JobPersistenceException {
        return assembler.getPausedJobGroupsDao().getPausedGroups();
      }
    });
  }

  @Override
  public void pauseAll() throws JobPersistenceException {
    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
      @Override
      public Void doInTransaction() throws JobPersistenceException {
        assembler.getTriggerStateManager().pauseAll();

        return null;
      }
    });
  }

  @Override
  public void resumeAll() throws JobPersistenceException {
    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
      @Override
      public Void doInTransaction() throws JobPersistenceException {
        assembler.getTriggerStateManager().resumeAll();

        return null;
      }
    });
  }

  @Override
  public void pauseJob(final JobKey jobKey) throws JobPersistenceException {
    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
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
    return assembler.getOrientDbConnector()
        .doInTransaction(new TransactionMethod<Collection<String>>() {
          @Override
          public Collection<String> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerStateManager().pauseJobs(groupMatcher);
          }
        });
  }

  @Override
  public void resumeJob(final JobKey jobKey) throws JobPersistenceException {
    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
      @Override
      public Void doInTransaction() throws JobPersistenceException {
        assembler.getTriggerStateManager().resume(jobKey);

        return null;
      }
    });
  }

  @Override
  public Collection<String> resumeJobs(final GroupMatcher<JobKey> groupMatcher)
      throws JobPersistenceException {
    return assembler.getOrientDbConnector()
        .doInTransaction(new TransactionMethod<Collection<String>>() {
          @Override
          public Collection<String> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerStateManager().resumeJobs(groupMatcher);
          }
        });
  }

  @Override
  public List<OperableTrigger> acquireNextTriggers(final long noLaterThan, final int maxCount,
      final long timeWindow) throws JobPersistenceException {
    return assembler.getOrientDbConnector()
        .doInTransaction(new TransactionMethod<List<OperableTrigger>>() {
          @Override
          public List<OperableTrigger> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerRunner().acquireNext(noLaterThan, maxCount, timeWindow);
          }
        });
  }

  @Override
  public void releaseAcquiredTrigger(final OperableTrigger trigger) throws JobPersistenceException {
    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
      @Override
      public Void doInTransaction() throws JobPersistenceException {
        assembler.getLockManager().unlockAcquiredTrigger(trigger);

        return null;
      }
    });
  }

  @Override
  public List<TriggerFiredResult> triggersFired(final List<OperableTrigger> triggers)
      throws JobPersistenceException {
    return assembler.getOrientDbConnector()
        .doInTransaction(new TransactionMethod<List<TriggerFiredResult>>() {
          @Override
          public List<TriggerFiredResult> doInTransaction() throws JobPersistenceException {
            return assembler.getTriggerRunner().triggersFired(triggers);
          }
        });
  }

  @Override
  public void triggeredJobComplete(final OperableTrigger trigger, final JobDetail job,
      final CompletedExecutionInstruction triggerInstCode) throws JobPersistenceException {
    assembler.getOrientDbConnector().doInTransaction(new TransactionMethod<Void>() {
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

  public void setAddresses(String addresses) {
    this.addresses = addresses.split(",");
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

  public long getClusterCheckinIntervalMillis() {
    return clusterCheckinIntervalMillis;
  }

  public void setClusterCheckinIntervalMillis(long clusterCheckinIntervalMillis) {
    this.clusterCheckinIntervalMillis = clusterCheckinIntervalMillis;
  }

  public void setMongoOptionMaxConnectionsPerHost(int maxConnectionsPerHost) {
    this.mongoOptionMaxConnectionsPerHost = maxConnectionsPerHost;
  }

  public void setMongoOptionConnectTimeoutMillis(int maxConnectWaitTime) {
    this.mongoOptionConnectTimeoutMillis = maxConnectWaitTime;
  }

  public void setMongoOptionSocketTimeoutMillis(int socketTimeoutMillis) {
    this.mongoOptionSocketTimeoutMillis = socketTimeoutMillis;
  }

  public void setMongoOptionThreadsAllowedToBlockForConnectionMultiplier(
      int threadsAllowedToBlockForConnectionMultiplier) {
    this.mongoOptionThreadsAllowedToBlockForConnectionMultiplier =
        threadsAllowedToBlockForConnectionMultiplier;
  }

  public void setMongoOptionSocketKeepAlive(boolean socketKeepAlive) {
    this.mongoOptionSocketKeepAlive = socketKeepAlive;
  }

  public void setMongoOptionEnableSSL(boolean enableSSL) {
    this.mongoOptionEnableSSL = enableSSL;
  }

  public void setMongoOptionSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {
    this.mongoOptionSslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
  }

  public void setMongoOptionWriteConcernTimeoutMillis(int writeConcernTimeoutMillis) {
    this.mongoOptionWriteConcernTimeoutMillis = writeConcernTimeoutMillis;
  }

  public String getAuthDbName() {
    return authDbName;
  }

  public void setAuthDbName(String authDbName) {
    this.authDbName = authDbName;
  }
}
