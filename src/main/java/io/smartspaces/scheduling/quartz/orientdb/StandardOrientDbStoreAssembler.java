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

import org.bson.Document;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.SchedulerSignaler;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import io.smartspaces.scheduling.quartz.orientdb.cluster.CheckinExecutor;
import io.smartspaces.scheduling.quartz.orientdb.cluster.CheckinTask;
import io.smartspaces.scheduling.quartz.orientdb.cluster.RecoveryTriggerFactory;
import io.smartspaces.scheduling.quartz.orientdb.cluster.TriggerRecoverer;
import io.smartspaces.scheduling.quartz.orientdb.dao.StandardCalendarDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.StandardJobDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.StandardLocksDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.StandardPausedJobGroupsDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.StandardPausedTriggerGroupsDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.StandardSchedulerDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.StandardTriggerDao;
import io.smartspaces.scheduling.quartz.orientdb.db.StandardOrientDbConnector;
import io.smartspaces.scheduling.quartz.orientdb.trigger.MisfireHandler;
import io.smartspaces.scheduling.quartz.orientdb.trigger.TriggerConverter;
import io.smartspaces.scheduling.quartz.orientdb.util.Clock;
import io.smartspaces.scheduling.quartz.orientdb.util.ExpiryCalculator;
import io.smartspaces.scheduling.quartz.orientdb.util.QueryHelper;

public class StandardOrientDbStoreAssembler {

  private StandardOrientDbConnector orientDbConnector;
  private JobCompleteHandler jobCompleteHandler;
  private LockManager lockManager;
  private TriggerStateManager triggerStateManager;
  private TriggerRunner triggerRunner;
  private TriggerAndJobPersister persister;

  private StandardCalendarDao calendarDao;
  private StandardJobDao jobDao;
  private StandardLocksDao locksDao;
  private StandardSchedulerDao schedulerDao;
  private StandardPausedJobGroupsDao pausedJobGroupsDao;
  private StandardPausedTriggerGroupsDao pausedTriggerGroupsDao;
  private StandardTriggerDao triggerDao;

  private TriggerRecoverer triggerRecoverer;
  private CheckinExecutor checkinExecutor;

  private QueryHelper queryHelper = new QueryHelper();
  private TriggerConverter triggerConverter;

  public void build(OrientDbJobStore jobStore, ClassLoadHelper loadHelper,
      SchedulerSignaler signaler) throws SchedulerConfigException {
    orientDbConnector = createOrientDbConnector(jobStore);

    jobDao = createJobDao(jobStore, loadHelper);

    triggerConverter = new TriggerConverter(jobDao);

    triggerDao = createTriggerDao(jobStore);
    calendarDao = createCalendarDao(jobStore);
    locksDao = createLocksDao(jobStore);
    pausedJobGroupsDao = createPausedJobGroupsDao(jobStore);
    pausedTriggerGroupsDao = createPausedTriggerGroupsDao(jobStore);
    schedulerDao = createSchedulerDao(jobStore);

    persister = createTriggerAndJobPersister();

    jobCompleteHandler = createJobCompleteHandler(signaler);

    lockManager = createLockManager(jobStore);

    triggerStateManager = createTriggerStateManager();

    MisfireHandler misfireHandler = createMisfireHandler(jobStore, signaler);

    RecoveryTriggerFactory recoveryTriggerFactory = new RecoveryTriggerFactory(jobStore.instanceId);

    triggerRecoverer = new TriggerRecoverer(locksDao, persister, lockManager, triggerDao, jobDao,
        recoveryTriggerFactory, misfireHandler);

    triggerRunner = createTriggerRunner(misfireHandler);

    checkinExecutor = createCheckinExecutor(jobStore);
  }

  public StandardOrientDbConnector getOrientDbConnector() {
    return orientDbConnector;
  }

  public JobCompleteHandler getJobCompleteHandler() {
    return jobCompleteHandler;
  }

  public LockManager getLockManager() {
    return lockManager;
  }

  public TriggerStateManager getTriggerStateManager() {
    return triggerStateManager;
  }

  public TriggerRunner getTriggerRunner() {
    return triggerRunner;
  }

  public TriggerAndJobPersister getPersister() {
    return persister;
  }

  public TriggerRecoverer getTriggerRecoverer() {
    return triggerRecoverer;
  }

  public CheckinExecutor getCheckinExecutor() {
    return checkinExecutor;
  }

  public QueryHelper getQueryHelper() {
    return queryHelper;
  }

  public TriggerConverter getTriggerConverter() {
    return triggerConverter;
  }

  public StandardCalendarDao getCalendarDao() {
    return calendarDao;
  }

  public StandardJobDao getJobDao() {
    return jobDao;
  }

  public StandardLocksDao getLocksDao() {
    return locksDao;
  }

  public StandardSchedulerDao getSchedulerDao() {
    return schedulerDao;
  }

  public StandardPausedJobGroupsDao getPausedJobGroupsDao() {
    return pausedJobGroupsDao;
  }

  public StandardPausedTriggerGroupsDao getPausedTriggerGroupsDao() {
    return pausedTriggerGroupsDao;
  }

  public StandardTriggerDao getTriggerDao() {
    return triggerDao;
  }

  private CheckinExecutor createCheckinExecutor(OrientDbJobStore jobStore) {
    return new CheckinExecutor(new CheckinTask(schedulerDao), jobStore.clusterCheckinIntervalMillis,
        jobStore.instanceId);
  }

  private StandardCalendarDao createCalendarDao(OrientDbJobStore jobStore) {
    return new StandardCalendarDao(this);
  }

  private StandardJobDao createJobDao(OrientDbJobStore jobStore, ClassLoadHelper loadHelper) {
    JobConverter jobConverter = new JobConverter(jobStore.getClassLoaderHelper(loadHelper));
    return new StandardJobDao(this, queryHelper, jobConverter);
  }

  private JobCompleteHandler createJobCompleteHandler(SchedulerSignaler signaler) {
    return new JobCompleteHandler(persister, signaler, jobDao, locksDao, triggerDao);
  }

  private StandardLocksDao createLocksDao(OrientDbJobStore jobStore) {
    return new StandardLocksDao(this, Clock.SYSTEM_CLOCK, jobStore.instanceId);
  }

  private LockManager createLockManager(OrientDbJobStore jobStore) {
    ExpiryCalculator expiryCalculator = new ExpiryCalculator(schedulerDao, Clock.SYSTEM_CLOCK,
        jobStore.jobTimeoutMillis, jobStore.triggerTimeoutMillis);
    return new LockManager(locksDao, expiryCalculator);
  }

  private MisfireHandler createMisfireHandler(OrientDbJobStore jobStore,
      SchedulerSignaler signaler) {
    return new MisfireHandler(calendarDao, signaler, jobStore.misfireThreshold);
  }

  private StandardOrientDbConnector createOrientDbConnector(OrientDbJobStore jobStore)
      throws SchedulerConfigException {
    return StandardOrientDbConnector.builder().withUri(jobStore.orientdbUri)
        .withCredentials(jobStore.username, jobStore.password)
        .withDatabaseName(jobStore.dbName).withAuthDatabaseName(jobStore.authDbName)
        .withMaxConnectionsPerHost(jobStore.mongoOptionMaxConnectionsPerHost)
        .withConnectTimeoutMillis(jobStore.mongoOptionConnectTimeoutMillis)
        .withSocketTimeoutMillis(jobStore.mongoOptionSocketTimeoutMillis)
        .withSocketKeepAlive(jobStore.mongoOptionSocketKeepAlive)
        .withThreadsAllowedToBlockForConnectionMultiplier(
            jobStore.mongoOptionThreadsAllowedToBlockForConnectionMultiplier)
        .withSSL(jobStore.mongoOptionEnableSSL, jobStore.mongoOptionSslInvalidHostNameAllowed)
        .withWriteTimeout(jobStore.mongoOptionWriteConcernTimeoutMillis).build();
  }

  private StandardPausedJobGroupsDao createPausedJobGroupsDao(OrientDbJobStore jobStore) {
    return new StandardPausedJobGroupsDao(this, queryHelper);
  }

  private StandardPausedTriggerGroupsDao createPausedTriggerGroupsDao(OrientDbJobStore jobStore) {
    return new StandardPausedTriggerGroupsDao(this, queryHelper);
  }

  private StandardSchedulerDao createSchedulerDao(OrientDbJobStore jobStore) {
    return new StandardSchedulerDao(this, jobStore.schedulerName,
        jobStore.instanceId, jobStore.clusterCheckinIntervalMillis, Clock.SYSTEM_CLOCK);
  }

  private TriggerAndJobPersister createTriggerAndJobPersister() {
    return new TriggerAndJobPersister(triggerDao, jobDao, triggerConverter);
  }

  private StandardTriggerDao createTriggerDao(OrientDbJobStore jobStore) {
    return new StandardTriggerDao(this, queryHelper, triggerConverter);
  }

  private TriggerRunner createTriggerRunner(MisfireHandler misfireHandler) {
    return new TriggerRunner(persister, triggerDao, jobDao, locksDao, calendarDao, misfireHandler,
        triggerConverter, lockManager, triggerRecoverer);
  }

  private TriggerStateManager createTriggerStateManager() {
    return new TriggerStateManager(triggerDao, jobDao, pausedJobGroupsDao, pausedTriggerGroupsDao);
  }
}
