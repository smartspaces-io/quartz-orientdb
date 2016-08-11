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

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.SimpleThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

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
    instance.createScheduler(SCHEDULER_NAME, SCHEDULER_INSTANCE_ID, threadPool,
        jobStore);

    Scheduler scheduler = instance.getScheduler(SCHEDULER_NAME);

    scheduler.start();

    // scheduleSimpleJob(scheduler, "simple1");
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
}
