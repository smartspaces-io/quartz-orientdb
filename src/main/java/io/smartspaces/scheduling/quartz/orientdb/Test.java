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

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import org.quartz.Calendar;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Scheduler;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
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

/**
 * A test driver for the quartz scheduler.
 * 
 * @author Keith M. Hughes
 */
public class Test {
  private static final String ORIENTDB_URI = "PLOCAL:/var/tmp/quartz";
  private static final String ORIENTDB_USER = "sooperdooper";
  private static final String ORIENTDB_PASSWORD = "sooperdooper";
  private static final String SCHEDULER_NAME = "SmartSpacesScheduler";
  private static final String SCHEDULER_INSTANCE_ID = "MainSchedulerNonClustered";

  public static void main(String[] args) throws Exception {
    OrientDbJobStore jobStore =
        new OrientDbJobStore(ORIENTDB_URI, ORIENTDB_USER, ORIENTDB_PASSWORD);
    SimpleThreadPool threadPool = new SimpleThreadPool(10, Thread.NORM_PRIORITY);

    DirectSchedulerFactory instance = DirectSchedulerFactory.getInstance();
    instance.createScheduler(SCHEDULER_NAME, SCHEDULER_INSTANCE_ID, threadPool, jobStore);

    Scheduler scheduler = instance.getScheduler(SCHEDULER_NAME);

    scheduler.start();

    scheduleSimpleJob(scheduler, "simple1");
    // scheduleCronJob(scheduler, "0 0/1 * * * ?", "cron");
  }

  private static void scheduleSimpleJob(Scheduler scheduler, String data)
      throws SchedulerException {
    JobDetail job = newJob(HelloJob.class).withIdentity("myJob" + data, "group1")
        .usingJobData("foo", data).build();

    // Trigger the job to run now, and then every 20 seconds
    Trigger trigger = newTrigger().withIdentity("myTrigger" + data, "group1").startNow()
        .withSchedule(simpleSchedule().withIntervalInSeconds(40).repeatForever()).build();

    // Tell quartz to schedule the job using our trigger
    scheduler.scheduleJob(job, trigger);
  }

  private static void scheduleCronJob(Scheduler scheduler, String cron, String data)
      throws SchedulerException {
    JobDetail job = newJob(HelloJob.class).withIdentity("myJob" + data, "group1")
        .usingJobData("foo", data).build();

    // Trigger the job to run now, and then every 20 seconds
    Trigger trigger = newTrigger().withIdentity("myTrigger" + data, "group1")
        .withSchedule(cronSchedule(cron)).build();

    // Tell quartz to schedule the job using our trigger
    scheduler.scheduleJob(job, trigger);
  }

  public static class HelloJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(HelloJob.class);

    public void execute(JobExecutionContext context) throws JobExecutionException {
      // Say Hello to the World and display the date/time
      Object data = context.getMergedJobDataMap().get("foo");
      log.info("Hello World! - " + data + " - " + new Date());
    }
  }

  public static class DelegateJobStore implements JobStore {
    private static final Logger LOG = LoggerFactory.getLogger(DelegateJobStore.class);
    private JobStore delegate;

    public DelegateJobStore(JobStore delegate) {
      this.delegate = delegate;
    }

    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler)
        throws SchedulerConfigException {
      delegate.initialize(loadHelper, signaler);
    }

    public void schedulerStarted() throws SchedulerException {
      delegate.schedulerStarted();
    }

    public void schedulerPaused() {
      delegate.schedulerPaused();
    }

    public void schedulerResumed() {
      delegate.schedulerResumed();
    }

    public void shutdown() {
      delegate.shutdown();
    }

    public boolean supportsPersistence() {
      return delegate.supportsPersistence();
    }

    public long getEstimatedTimeToReleaseAndAcquireTrigger() {
      return delegate.getEstimatedTimeToReleaseAndAcquireTrigger();
    }

    public boolean isClustered() {
      return delegate.isClustered();
    }

    public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
        throws ObjectAlreadyExistsException, JobPersistenceException {
      LOG.debug("Adding job {}  and trigger {}", newJob, newTrigger);
      delegate.storeJobAndTrigger(newJob, newTrigger);
    }

    public void storeJob(JobDetail newJob, boolean replaceExisting)
        throws ObjectAlreadyExistsException, JobPersistenceException {
      LOG.debug("Adding job {} with replace={}", newJob, replaceExisting);
      delegate.storeJob(newJob, replaceExisting);
    }

    public void storeJobsAndTriggers(Map<JobDetail, List<Trigger>> triggersAndJobs, boolean replace)
        throws ObjectAlreadyExistsException, JobPersistenceException {
      delegate.storeJobsAndTriggers(triggersAndJobs, replace);
    }

    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
      LOG.debug("Removing job {}", jobKey);
      return delegate.removeJob(jobKey);
    }

    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
      LOG.debug("Removing jobs {}", jobKeys);
      return delegate.removeJobs(jobKeys);
    }

    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
      LOG.debug("Retrieve job {}", jobKey);
      return delegate.retrieveJob(jobKey);
    }

    public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
        throws ObjectAlreadyExistsException, JobPersistenceException {
      LOG.debug("Store trigger {} with replace", newTrigger, replaceExisting);
      delegate.storeTrigger(newTrigger, replaceExisting);
    }

    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
      LOG.debug("Removing trigger {}", triggerKey);
      return delegate.removeTrigger(triggerKey);
    }

    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
      LOG.debug("Removing triggers {}", triggerKeys);
      return delegate.removeTriggers(triggerKeys);
    }

    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger)
        throws JobPersistenceException {
      LOG.debug("Replacing trigger {} with {}", triggerKey, newTrigger);
      return delegate.replaceTrigger(triggerKey, newTrigger);
    }

    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
      LOG.debug("Retrieving trigger {}", triggerKey);
      return delegate.retrieveTrigger(triggerKey);
    }

    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
      LOG.debug("Checking existence of job {}", jobKey);
      return delegate.checkExists(jobKey);
    }

    public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
      LOG.debug("Checking existence of trigger {}", triggerKey);
      return delegate.checkExists(triggerKey);
    }

    public void clearAllSchedulingData() throws JobPersistenceException {
      LOG.debug("Clearing all scheduling data");
      delegate.clearAllSchedulingData();
    }

    public void storeCalendar(String name, Calendar calendar, boolean replaceExisting,
        boolean updateTriggers) throws ObjectAlreadyExistsException, JobPersistenceException {
      LOG.debug("Storing calendar {}: {} with replace {}", name, calendar, replaceExisting);
      delegate.storeCalendar(name, calendar, replaceExisting, updateTriggers);
    }

    public boolean removeCalendar(String calName) throws JobPersistenceException {
      LOG.debug("Remove calendar {}", calName);
      return delegate.removeCalendar(calName);
    }

    public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
      LOG.debug("Retrieve calendar {}", calName);
      return delegate.retrieveCalendar(calName);
    }

    public int getNumberOfJobs() throws JobPersistenceException {
      LOG.debug("Get number of jobs");
      return delegate.getNumberOfJobs();
    }

    public int getNumberOfTriggers() throws JobPersistenceException {
      LOG.debug("Get number of triggers");
      return delegate.getNumberOfTriggers();
    }

    public int getNumberOfCalendars() throws JobPersistenceException {
      LOG.debug("Get number of calendars");
      return delegate.getNumberOfCalendars();
    }

    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
      LOG.debug("Get job keys for {}", matcher);
      return delegate.getJobKeys(matcher);
    }

    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
        throws JobPersistenceException {
      LOG.debug("Get trigger keys for {}", matcher);
      return delegate.getTriggerKeys(matcher);
    }

    public List<String> getJobGroupNames() throws JobPersistenceException {
      LOG.debug("Get job group names.");
      return delegate.getJobGroupNames();
    }

    public List<String> getTriggerGroupNames() throws JobPersistenceException {
      LOG.debug("Get trigger group names.");
      return delegate.getTriggerGroupNames();
    }

    public List<String> getCalendarNames() throws JobPersistenceException {
      LOG.debug("Get calendar names.");
      return delegate.getCalendarNames();
    }

    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
      LOG.debug("Get triggers for job {}", jobKey);
      return delegate.getTriggersForJob(jobKey);
    }

    public TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
      LOG.debug("Get state for trigger {}", triggerKey);
      return delegate.getTriggerState(triggerKey);
    }

    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
      LOG.debug("Pause trigger {}", triggerKey);
      delegate.pauseTrigger(triggerKey);
    }

    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
        throws JobPersistenceException {
      LOG.debug("Pause triggers matching {}", matcher);
      return delegate.pauseTriggers(matcher);
    }

    public void pauseJob(JobKey jobKey) throws JobPersistenceException {
      LOG.debug("Pause job {}", jobKey);
      delegate.pauseJob(jobKey);
    }

    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher)
        throws JobPersistenceException {
      LOG.debug("Pause triggers matching {}", groupMatcher);
      return delegate.pauseJobs(groupMatcher);
    }

    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
      LOG.debug("Resume trigger {}", triggerKey);
      delegate.resumeTrigger(triggerKey);
    }

    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher)
        throws JobPersistenceException {
      LOG.debug("Resume triggers matching {}", matcher);
      return delegate.resumeTriggers(matcher);
    }

    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
      LOG.debug("Get paused trigger groups");
      return delegate.getPausedTriggerGroups();
    }

    public void resumeJob(JobKey jobKey) throws JobPersistenceException {
      LOG.debug("Resume job {}", jobKey);
      delegate.resumeJob(jobKey);
    }

    public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher)
        throws JobPersistenceException {
      LOG.debug("Resume jobs matching {}", matcher);
      return delegate.resumeJobs(matcher);
    }

    public void pauseAll() throws JobPersistenceException {
      LOG.debug("Pause all");
      delegate.pauseAll();
    }

    public void resumeAll() throws JobPersistenceException {
      LOG.debug("Resume all");
      delegate.resumeAll();
    }

    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount,
        long timeWindow) throws JobPersistenceException {
      LOG.info("Acquiring next triggers");

      return delegate.acquireNextTriggers(noLaterThan, maxCount, timeWindow);
    }

    public void releaseAcquiredTrigger(OperableTrigger trigger) throws JobPersistenceException {
      LOG.info("Releasing acquired trigger {}", trigger);
      delegate.releaseAcquiredTrigger(trigger);
    }

    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
        throws JobPersistenceException {
      LOG.info("Triggers fired {}", triggers);
      return delegate.triggersFired(triggers);
    }

    public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail,
        CompletedExecutionInstruction triggerInstCode) throws JobPersistenceException {
      LOG.info("Triggered job complete {} for job {} with instruction {}", trigger, jobDetail,
          triggerInstCode);
      delegate.triggeredJobComplete(trigger, jobDetail, triggerInstCode);
    }

    public void setInstanceId(String schedInstId) {
      delegate.setInstanceId(schedInstId);
    }

    public void setInstanceName(String schedName) {
      delegate.setInstanceName(schedName);
    }

    public void setThreadPoolSize(int poolSize) {
      delegate.setThreadPoolSize(poolSize);
    }
  }
}
